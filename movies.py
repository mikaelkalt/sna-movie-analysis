# Installation: pip install SPARQLWrapper
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

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
    
SELECT ?movie ?name ?director ?directorLabel ?genre ?genreLabel WHERE
{
	?movie wdt:P166 ?award.
    ?award wdt:P31 wd:Q19020.
    ?movie wdt:P31 wd:Q11424.
    ?movie wdt:P1476 ?name.
    OPTIONAL {
      ?movie wdt:P57 ?director. 
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }.
      ?movie wdt:P136 ?genre. 
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
}
ORDER BY ?movie LIMIT 10000 OFFSET %i"""

actor_query = prefix + """
    
SELECT ?movie ?actor ?actorName WHERE
{
	?movie wdt:P166 ?award.
    ?award wdt:P31 wd:Q19020.
    ?movie wdt:P31 wd:Q11424.
    ?movie wdt:P161 ?actor.
    ?actor wdt:P1477 ?actorName.
}
ORDER BY ?movie LIMIT 10000 OFFSET %i"""

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

def parse_movie_results(results, nodes, edges):
    for result in results["results"]["bindings"]:
        movie = result["movie"]["value"]
        label = result["name"]["value"]
        
        if "director" in result:
            director = result["director"]["value"]
            director_label = result["directorLabel"]["value"]
          
        if "genre" in result: 
            genre = result["genre"]["value"]
            genre_label = result["genreLabel"]["value"]


        if movie in nodes: 
            nodes[movie]['genre'] += "," + genre
            nodes[movie]['genre_label'] += "," + genre_label
        else:
             nodes[movie] = {'label': label, 'type': 'MOVIE','genre': genre, 'genre_label': genre_label}

        if director not in nodes:
             nodes[director] = {'label': director_label, 'type': 'DIRECTOR'}
        
        edges = edges.append({'Source': director, 'Target': movie, 'Type': 'is_directing'}, ignore_index=True)       

    return nodes, edges

def parse_actor_results(results, nodes, edges):
    for result in results["results"]["bindings"]:
        movie = result["movie"]["value"]
        actor = result["actor"]["value"]
        label = result["actorName"]["value"]
          
        if movie not in nodes: 
            print(movie + " was not found before")        

        if actor not in nodes:
             nodes[actor] = {'label': label, 'type': 'ACTOR'}
        
        edges = edges.append({'Source': actor, 'Target': movie, 'Type': 'is_acting'}, ignore_index=True)       

    return nodes, edges

def remove_duplicates(nodes):
    return nodes.drop_duplicates()

def write_csvs(nodes_df):
    nodes_df.to_csv('output/nodes.csv', index=True, index_label='ID', encoding='utf-8')

def write_csvs_edges(edges_df): 
    edges_df.to_csv('output/edges.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    nodes = {}
    edges = pd.DataFrame(columns=['Source', 'Target'])

    currentOffset = 0
    has_remaining_results = True
    while (has_remaining_results):
        query = build_movie_query(currentOffset)
        results = fetch_data(query)
        nodes, edges = parse_movie_results(results, nodes, edges)
        num_results = len(results["results"]["bindings"])
        if num_results == 10000:
            currentOffset += 10000
        else:
            has_remaining_results = False
    
    currentOffset = 0
    has_remaining_results = True
    while (has_remaining_results):
        query = build_actor_query(currentOffset)
        results = fetch_data(query)
        nodes, edges = parse_actor_results(results, nodes, edges)
        num_results = len(results["results"]["bindings"])
        if num_results == 10000:
            currentOffset += 10000
        else:
            has_remaining_results = False


    pd_nodes = pd.DataFrame.from_dict(nodes, columns=['label','type', 'genre', 'genre_label'], orient='index')

    edges = remove_duplicates(edges)
    write_csvs(pd_nodes)
    write_csvs_edges(edges)
