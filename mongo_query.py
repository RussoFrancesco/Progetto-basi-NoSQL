import pymongo
import time
import csv
import numpy as np

def confidence(data):
    n = len(data)
    mean = np.mean(data)
    std_dev = np.std(data, ddof=1)

    margin = (std_dev / np.sqrt(n)) * 1.96

    lower = mean - margin
    upper = mean + margin

    return (lower, upper)

def query(coll, results, j):
    
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
    print(results)

    confidence_lvl = confidence(data)
    #print(confidence_lvl)
    results.append(confidence_lvl)

    writer_result.writerow(results)   
    

percentage = [25, 50, 75, 100]

result_csv = open("csv/result_mongo.csv", "w", newline='')
writer_result = csv.writer(result_csv)
headers = ['Query', 'Dimensione', 'Tempo prima esecuzione', 'Tempo delle 40 esecuzioni','Tempo medio', 'Intervallo di confidenza']
writer_result.writerow(headers)

start_search = 1672617600	
end_search = 1672703999
start_dur_search = 900
end_dur_search = 1200


start_search1 = 1672531200
end_search1 = 1672790399


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
        client = pymongo.MongoClient("127.0.0.1:27017")
        database = client["progetto"+str(p)]
        if j == 1:
            coll = database["people"]
        else:
            coll = database["calls"]    #mi connetto ad il singolo db ed alla collezione calls di ognuno di questo
        
        query(coll, results, j)
        client.close()

