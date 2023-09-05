import pymongo
import time
import csv

def clear_cache(database):
    database.command("planCacheClear", "calls")
    database.command("planCacheClear", "cells")
    database.command("planCacheClear", "people")

percentage = [25, 50, 75, 100]

result_csv = open("csv/result.csv", "w", newline='\n')
writer_result = csv.writer(result_csv)

client = pymongo.MongoClient("localhost", 27017)

start_search = 1672617600
end_search = 1672703999

query = [
            {"$match": {"StartDate": {"$gte": start_search,"$lt": end_search}}},
            [
                {"$match":{"StartDate": {"$gte": start_search,"$lt": end_search}}},
                {"$lookup":{
                "from": "people",
                "localField": "called",
                "foreignField": "number",
                "as": "called_details"}}
            ]
        ]


for j in range(1, len(query)+1):
    for p in percentage:
        database = client["progetto"+str(p)]
        call = database["call"]    #mi connetto ad il singolo db ed alla collezione calls di ognuno di questo 
        header = ["mongo"+str(p)+"_query"+str(j)]
        writer_result.writerow(header)    
        

        results= []
        
        for i in range(31):
            print(i)
            start_time = time.time_ns() #tempo iniziale in nanosecondi per evitare perdite di approssimazione      
            
            if (j==1):
                call.find(query[j-1]) #esecuzione della query 0 con find i  quanto sempre nella stessa collection 
            else:
                call.aggregate(query[j-1]) #esecuzione della query con aggregate in quanto comprende un operazione di lookup

            end_time = (time.time_ns() - start_time) / 10000000 #calcolo del tempo di esecuzione in millisecondi (divisione per 1 milione)
            results.append(end_time)
        print(results)
        writer_result.writerow(results)   
        clear_cache(database)    

