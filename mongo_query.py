import pymongo
import time
import csv
import numpy as np
from scipy import stats

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

dur_search_start = 900
dur_search_end = 1200

query = [
            [{"$match": {"StartDate": {"$gte": start_search,"$lt": end_search}}}],
            [
                {"$match":{"StartDate": {"$gte": start_search,"$lt": end_search}}},
                {"$lookup":{
                            "from": "people",
                            "localField": "calling",
                            "foreignField": "number",
                            "as": "calling_details"}}
            ],
            [
                {"$match": {"StartDate": {"$gte": start_search,"$lt": end_search}}},
                {"$lookup":{
                            "from": "people",
                            "localField": "calling",
                            "foreignField": "number",
                            "as": "calling_details"}},
                {"$lookup":{
                            "from": "cell",
                            "localField": "cell_site",
                            "foreignField": "id",
                            "as": "cell_details"}}
            ],
            [
            {"$match": {"StartDate": {"$gte": start_search,
                                      "$lt": end_search},
                        "Duration": {"$gte": dur_search_start,
                                     "$lt": dur_search_end}}},
            {"$lookup": {"from": "people",
                         "localField": "calling",
                         "foreignField": "Number",
                         "as": "calling_details"}},
            {"$lookup": {"from": "cells",
                         "localField": "cell_site",
                         "foreignField": "id",
                         "as": "cell_details"}}
        ]
        ]


for j in range(1, len(query)+1):
    for p in percentage:
        database = client["progetto"+str(p)]
        call = database["call"]    #mi connetto ad il singolo db ed alla collezione calls di ognuno di questo 
        clear_cache(database)

        results= ["Query"+str(j), str(p)+"%"]

        start_time = time.time()
        call.aggregate(query[j-1])
        end_time = (time.time() - start_time) * 1000
        results.append(end_time)

        data = []

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
        print(confidence_lvl)
        results.append(confidence_lvl[1])
        results.append(confidence_lvl[0])

        writer_result.writerow(results)   
        clear_cache(database)    
