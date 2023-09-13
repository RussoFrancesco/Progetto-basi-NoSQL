import pymongo
import time
import csv
import numpy as np

def clear_cache(database):
    database.command("planCacheClear", "calls")
    database.command("planCacheClear", "cells")
    database.command("planCacheClear", "people")

def confidence(data):
    n = len(data)
    mean = np.mean(data)
    std_dev = np.std(data, ddof=1)

    margin = (std_dev / np.sqrt(n)) * 1.96

    lower = mean - margin
    upper = mean + margin

    return (lower, upper)

percentage = [25, 50, 75, 100]

result_csv = open("csv/result_mongo.csv", "w", newline='')
writer_result = csv.writer(result_csv)
headers = ['Query', 'Dimensione', 'Tempo prima esecuzione', 'Tempo delle 30 esecuzioni','Tempo medio', 'Intervallo di confidenza sup', 'Intervallo di confidenza inf']
writer_result.writerow(headers)

client = pymongo.MongoClient("localhost", 27017)

start_search = 1672617600
end_search = 1672703999
dur_search = 900

query = [
        [
            {"$match": {"startdate": {"$gte": start_search,
                                      "$lt": end_search}}}
        ],

        [{"$match": {"startdate": {
                        "$gte": start_search,
                        "$lt": end_search}}},
        { "$lookup": {
                        "from": "people",
                        "localField": "calling",
                        "foreignField": "number",
                        "as": "calling"}},
        {"$match":{"calling.first_name":"Giovanni"}}
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
            {"$match": {"calling.first_name": "Giovanni" }},
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
                {"calling.first_name": "Giovanni","called.first_name": "Cassandra"} ,
                {"called.first_name": "Giovanni","calling.first_name": "Cassandra"}
                ]
                }
            }

        ]
    ]


for j in range(1, len(query)+1):
    for p in percentage:
        database = client["progetto"+str(p)]
        call = database["calls"]    #mi connetto ad il singolo db ed alla collezione calls di ognuno di questo 
        clear_cache(database)

        results = ["Query"+str(j), str(p)+"%"]

        start_time = time.time()
        call.aggregate(query[j-1])
        end_time = (time.time() - start_time) * 1000
        results.append(end_time)
        print(results)

        data = []

        for i in range(30):
            start30 = time.time()
            call.aggregate(query[j-1])        
            end30 = (time.time() - start30) * 1000
            data.append(end30)
        results.append(sum(data))

        avg = results[3]/30
        results.append(avg)

        confidence_lvl = confidence(data)
        #print(confidence_lvl)
        results.append(confidence_lvl[1])
        results.append(confidence_lvl[0])

        writer_result.writerow(results)   
        clear_cache(database)    


