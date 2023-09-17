import pandas as pd
import matplotlib.pyplot as plt
import re

# Carica i dati dai file CSV
data_mongo = pd.read_csv('csv/result_mongoDEFINITIVO.csv')
data_neo4j = pd.read_csv('csv/result_neoDEFINITIVO.csv')

# Lista delle dimensioni del dataset
dataset_sizes = ['100%', '75%', '50%', '25%']

# Lista delle query
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4']

# Definisce i colori per MongoDB e Neo4j
color_mongo = 'red'
color_neo4j = 'blue'

# Funzione per estrarre i valori minimi e massimi dall'intervallo di confidenza
def extract_confidence_values(confidence_interval_str):
    matches = re.findall(r'\d+\.\d+', confidence_interval_str)
    return float(matches[0]), float(matches[1])

# Per ogni query, crea gli istogrammi
for query in queries:
    # Filtra i dati per la query corrente
    data_mongo_query = data_mongo[data_mongo['Query'] == query]
    data_neo4j_query = data_neo4j[data_neo4j['Query'] == query]
    
    # Crea il primo istogramma con i tempi della prima esecuzione
    plt.figure(figsize=(10, 6))
    for size in dataset_sizes:
        values_mongo = data_mongo_query[data_mongo_query['Dimensione'] == size]['Tempo prima esecuzione']
        values_neo4j = data_neo4j_query[data_neo4j_query['Dimensione'] == size]['Tempo prima esecuzione']
        
        plt.bar([f"{size} (MongoDB)", f"{size} (Neo4j)"], [values_mongo.values[0], values_neo4j.values[0]], color=[color_mongo, color_neo4j])
        
    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione (ms)')
    plt.title(f'Istogramma - Tempo della Prima Esecuzione per {query}')
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Crea il secondo istogramma con le medie dei tempi
    plt.figure(figsize=(10, 6))
    for size in dataset_sizes:
        values_mongo = data_mongo_query[data_mongo_query['Dimensione'] == size]['Tempo medio']
        values_neo4j = data_neo4j_query[data_neo4j_query['Dimensione'] == size]['Tempo medio']
        
        # Estrae intervalli di confidenza
        confidence_intervals_mongo = data_mongo_query[data_mongo_query['Dimensione'] == size]['Confidence Interval (95%)']
        confidence_intervals_neo4j = data_neo4j_query[data_neo4j_query['Dimensione'] == size]['Confidence Interval (95%)']
        conf_intervals_mongo = [extract_confidence_values(conf_str) for conf_str in confidence_intervals_mongo]
        conf_intervals_neo4j = [extract_confidence_values(conf_str) for conf_str in confidence_intervals_neo4j]
        
        # Estrae valori minimi e massimi dagli intervalli di confidenza
        conf_mongo_min = [conf[0] for conf in conf_intervals_mongo]
        conf_mongo_max = [conf[1] for conf in conf_intervals_mongo]
        conf_neo4j_min = [conf[0] for conf in conf_intervals_neo4j]
        conf_neo4j_max = [conf[1] for conf in conf_intervals_neo4j]
        
        # Calcola la differenza tra il valore medio e gli estremi dell'intervallo di confidenza
        mongo_yerr = [[values_mongo.values[0] - conf_mongo_min[0]], [conf_mongo_max[0] - values_mongo.values[0]]]
        neo4j_yerr = [[values_neo4j.values[0] - conf_neo4j_min[0]], [conf_neo4j_max[0] - values_neo4j.values[0]]]
        
        # Rappresenta l'intervallo di confidenza per MongoDB e Neo4j
        plt.bar(f"{size} (MongoDB)", values_mongo.values[0], yerr=mongo_yerr, capsize=5, color=color_mongo, label='MongoDB')
        plt.bar(f"{size} (Neo4j)", values_neo4j.values[0], yerr=neo4j_yerr, capsize=5, color=color_neo4j, label='Neo4j')
        
    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione medio (ms)')
    plt.title(f'Istogramma - Tempo di Esecuzione Medio per {query}')
    plt.legend()
    plt.tight_layout()
    plt.show()