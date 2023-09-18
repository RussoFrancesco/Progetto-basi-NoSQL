import pymongo
import time
import csv
import numpy as np

#funzione per il calcolo dell'intervallo di confidenza
def confidence(data):
    n = len(data)
    mean = np.mean(data)
    std_dev = np.std(data, ddof=1)

    margin = (std_dev / np.sqrt(n)) * 1.96

    lower = mean - margin
    upper = mean + margin

    return (lower, upper)

#funzione per effettuare le query al database
def query(coll, results, j):
    
    #se è la prima query effettuiamo un find sennò un aggregate
    if (j == 1):
        start_time = time.time()
        coll.find(queries[j-1])
        end_time = (time.time() - start_time) * 1000
    else:
        start_time = time.time()
        coll.aggregate(queries[j-1])
        end_time = (time.time() - start_time) * 1000
    results.append(end_time)

    data = []

    for i in range(40):
        if (j == 1):
            start_time40 = time.time()
            coll.find(queries[j-1])
            end_time40 = (time.time() - start_time40) * 1000
        else:
            start_time40 = time.time()
            coll.aggregate(queries[j-1])
            end_time40 = (time.time() - start_time40) * 1000
        data.append(end_time40)
    results.append(sum(data))

    avg = results[3]/40
    results.append(avg)

    confidence_lvl = confidence(data)
    results.append(confidence_lvl)

    writer_result.writerow(results)   
    

percentage = [25, 50, 75, 100]

#apertura del file dei risultati
result_csv = open("csv/result_mongo.csv", "w", newline='')
writer_result = csv.writer(result_csv)

#creazione e scrittura degli headers
headers = ['Query', 'Dimensione', 'Tempo prima esecuzione', 'Tempo delle 40 esecuzioni','Tempo medio', 'Intervallo di confidenza']
writer_result.writerow(headers)

#variabili usate nelle query
start_search = 1672617600	
end_search = 1672703999

#lista delle query
queries = [

        {"first_name":"Laura"}

        ,

        [{"$match": {"startdate": {
                        "$gte": start_search,
                        "$lt": end_search}}},
        { "$lookup": {
                        "from": "people",
                        "localField": "calling",
                        "foreignField": "number",
                        "as": "calling"}},
        {"$match":{"calling.first_name":"Laura"}}
        ],

        [{"$match": {"startdate": {"$gte": start_search,
                                      "$lt": end_search}}},
            {"$lookup": {"from": "people",
                         "localField": "calling",
                         "foreignField": "number",
                         "as": "calling"}},
            {"$lookup": {"from": "cells",
                         "localField": "cell_site",
                         "foreignField": "id",
                         "as": "cell"}},
            {"$match": {"calling.first_name": "Laura" }},
            {"$match":{"cell.state":"Roma"}}
        ],

        [{"$match": {"startdate": {"$gte": start_search,
                                      "$lt": end_search}}},
            {"$lookup": {"from": "people",
                         "localField": "calling",
                         "foreignField": "number",
                         "as": "calling"}},
            {"$lookup": {"from": "cells",
                         "localField": "cell_site",
                         "foreignField": "id",
                         "as": "cell"}},
            {"$lookup": {"from": "people",
                         "localField": "called",
                         "foreignField": "number",
                         "as": "called"}},
            {"$match": {
                "$or":[ 
                {"calling.first_name": "Laura","called.first_name": "Antonio"} ,
                {"called.first_name": "Antonio","calling.first_name": "Laura"}
                ]
                }
            }

        ]
    ]


for j in range(1, len(queries)+1):
    for p in percentage:
        results = ["Query"+str(j), str(p)+"%"]

        #connessione all'istanza di mongo e al database corrispondente
        client = pymongo.MongoClient("127.0.0.1:27017")
        database = client["progetto"+str(p)]

        #se è la prima query ci colleghiamo alla collezione people sennò alla collezione calls
        if j == 1:
            coll = database["people"]
        else:
            coll = database["calls"]    #mi connetto ad il singolo db ed alla collezione calls di ognuno di questo
        
        #chiamata della funzione per effettuare le query
        query(coll, results, j)

        #chiusura del client
        client.close()

