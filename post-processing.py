import pandas as pd
 
if __name__ == "__main__":
     
    edges = pd.read_csv("output/edges.csv", encoding='utf-8')
    cleaned_edges = edges.drop_duplicates(subset=['Source', 'Target'])
    cleaned_edges.to_csv('output/cleaned_edges.csv', index=False, encoding='utf-8')
