# Installation: pip install SPARQLWrapper
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

MANUAL_GENRE_MAPPING = { 
    "http://www.wikidata.org/entity/Q622291": "http://www.wikidata.org/entity/Q2484376", # mapping of political thriller to thriller film 
    "http://www.wikidata.org/entity/Q860626": "http://www.wikidata.org/entity/Q157443,http://www.wikidata.org/entity/Q1054574", # mapping of romantic comedy to comedy and romance films 
    "http://www.wikidata.org/entity/Q859369": "http://www.wikidata.org/entity/Q157443,http://www.wikidata.org/entity/Q130232" # mapping of comedy-drama to comedy and drama films
}

LIMIT = 10000

prefix = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wds: <http://www.wikidata.org/entity/statement/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX pr: <http://www.wikidata.org/prop/reference/>
    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>"""

movie_query = prefix + """
    
SELECT ?movie ?name ?duration ?director ?directorLabel (YEAR(?directorBirthDate) as ?directorBirthYear) (MIN(YEAR(?publicationDate)) as ?year) ((?year - ?directorBirthYear) as ?directorAge) 
(GROUP_CONCAT(DISTINCT (?genre); SEPARATOR=", ") AS ?genres) (GROUP_CONCAT(DISTINCT (?genreLabel); SEPARATOR=", ") AS ?genreLabels)  WHERE
{
    ?movie wdt:P166 ?award.
    ?award wdt:P31 wd:Q19020.
    ?movie wdt:P31 wd:Q11424.
    ?movie wdt:P1476 ?name.
    OPTIONAL {
        ?movie wdt:P2047 ?duration.
        ?movie wdt:P577 ?publicationDate.
        ?movie wdt:P57 ?director.
        ?director wdt:P569 ?directorBirthDate.
        ?movie wdt:P136 ?genre.
        SERVICE wikibase:label {
            bd:serviceParam wikibase:language "en".
            ?director rdfs:label ?directorLabel.
            ?genre rdfs:label ?genreLabel
        }
    }
}
GROUP BY ?movie ?name ?duration ?director ?directorLabel ?directorBirthDate
ORDER BY ?movie ?director LIMIT """ + str(LIMIT) + """ OFFSET %i"""

actor_query = prefix + """
    
SELECT ?movie ?actor ?actorName (YEAR(?actorBirthDate) as ?actorBirthYear) (MIN(YEAR(?publicationDate)) as ?year) ((?year - ?actorBirthYear) as ?actorAge)  WHERE
{
	?movie wdt:P166 ?award.
    ?award wdt:P31 wd:Q19020.
    ?movie wdt:P31 wd:Q11424.
    ?movie wdt:P161 ?actor.
    ?actor wdt:P1477 ?actorName.
    OPTIONAL {
      ?movie wdt:P577 ?publicationDate.
      ?actor wdt:P569 ?actorBirthDate 
    }
}
GROUP BY ?movie ?actor ?actorName ?actorBirthDate
ORDER BY ?movie ?actor LIMIT """ + str(LIMIT) + """ OFFSET %i"""

def build_movie_query(offset):
    # Not using string.format to avoid escaping the curly braces in query_format
    # (same query except the limitation part can be used in Virtuoso)
    return movie_query % (offset)

def build_actor_query(offset):
    # Not using string.format to avoid escaping the curly braces in query_format
    # (same query except the limitation part can be used in Virtuoso)
    return actor_query % (offset)

def fetch_data(query):
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert();


def preprocess_genres(genres): 

    for key, value in MANUAL_GENRE_MAPPING.items():
        if key in genres: 
            genres = genres.replace(key, value)

    return genres


def ohe_genres(genres_of_interest, genres): 
    one_hot_encoded_genres = {}
    genres = preprocess_genres(genres)

    for genre_key, genre_name in genres_of_interest.items(): 
        one_hot_encoded_genres[genre_name['label']] = genre_key in genres
    return one_hot_encoded_genres 

def parse_movie_results(results, nodes, edges, genres):
    for result in results["results"]["bindings"]:
        movie = result["movie"]["value"]
        label = result["name"]["value"]
        one_hot_encoded_genres = ohe_genres(genres, result["genres"]["value"])
 
        if "director" in result:
            director = result["director"]["value"]
            director_label = result["directorLabel"]["value"]
            director_birth_year = result["directorBirthYear"]["value"]
            directing_age = result["directorAge"]["value"]
          
        year = result["year"]["value"] if "year" in result else ""
        duration = result["duration"]["value"] if "duration" in result else ""
        
        if int(directing_age) < 0: 
            print("{0} is probably too young ({1}) to direct the movie {2}".format(director, directing_age, movie))
        else: 
            movie_obj = {'label': label, 'type': 'MOVIE','year': year, 'duration': duration}
            movie_obj.update(one_hot_encoded_genres)
            nodes[movie] = movie_obj
        
            if director not in nodes:
                nodes[director] = {'label': director_label, 'type': 'DIRECTOR', 'year': director_birth_year }
        
            edges = edges.append({'Source': director, 'Target': movie, 'category': 'is_directing', 'age': directing_age }, ignore_index=True)       

    return nodes, edges

def parse_actor_results(results, nodes, edges):
    for result in results["results"]["bindings"]:
        movie = result["movie"]["value"]
        actor = result["actor"]["value"]
        label = result["actorName"]["value"]
        
        acting_age = result["actorAge"]["value"] if "year" in result else 0 
        actor_birth_year = result["actorBirthYear"]["value"] if "actorBirthYear" in result else 0
          
        if movie not in nodes: 
            print("{0} was not found before".format(movie))        

        if int(acting_age) < 0: 
            print("{0} is probably too young ({1}) to act in the movie {2}".format(actor, acting_age, movie))
        else: 
            if actor not in nodes:
                nodes[actor] = {'label': label, 'type': 'ACTOR', 'year': actor_birth_year}
        
            edges = edges.append({'Source': actor, 'Target': movie, 'category': 'is_acting', 'age': acting_age }, ignore_index=True)       

    return nodes, edges

def remove_duplicates(nodes):
    return nodes.drop_duplicates(subset = ['Source', 'Target'])

def write_csvs(nodes_df):
    nodes_df.to_csv('output/nodes.csv', index=True, index_label='ID', encoding='utf-8')

def write_csvs_edges(edges_df): 
    edges_df.to_csv('output/edges.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    nodes = {}
    edges = pd.DataFrame(columns=['Source', 'Target'])
    genres_of_interest = pd.read_csv("output/top_genres.csv", encoding='utf-8', index_col='ID', usecols=['ID','label']).to_dict(orient='index')

    currentOffset = 0
    has_remaining_results = True
    while (has_remaining_results):
        query = build_movie_query(currentOffset)
        results = fetch_data(query)
        nodes, edges = parse_movie_results(results, nodes, edges, genres_of_interest)
        num_results = len(results["results"]["bindings"])
        if num_results == LIMIT:
            currentOffset += LIMIT
        else:
            has_remaining_results = False
    
    currentOffset = 0
    has_remaining_results = True
    while (has_remaining_results):
        query = build_actor_query(currentOffset)
        results = fetch_data(query)
        nodes, edges = parse_actor_results(results, nodes, edges)
        num_results = len(results["results"]["bindings"])
        if num_results == LIMIT:
            currentOffset += LIMIT
        else:
            has_remaining_results = False


    pd_nodes = pd.DataFrame.from_dict(nodes, columns=['label','type', 'year', 'duration'].extend(genres_of_interest.keys()), orient='index')

    edges = remove_duplicates(edges)
    write_csvs(pd_nodes)
    write_csvs_edges(edges)
