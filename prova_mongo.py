import pymongo
import csv
from pymongo import command_cursor
import time

start_search = 1672617600
end_search = 1672703999
dur_search = 900

query = [
        [
            {"$match": {"startdate": {"$gte": start_search,
                                      "$lt": end_search},
                        "duration": {"$gte": dur_search}}},
            {"$lookup": {"from": "people",
                         "localField": "calling",
                         "foreignField": "number",
                         "as": "calling"}},
            {"$lookup": {"from": "cells",
                         "localField": "cell_site",
                         "foreignField": "id",
                         "as": "cell"}}
        ]
        ]

client = pymongo.MongoClient("localhost", 27017)


database = client["progetto100"]
calls = database["calls"]
i = 0

res = calls.aggregate(pipeline=query[0], batchSize=1000000, allowDiskUse=True)
#res = database.command("aggregate", value="calls", pipeline=query[0], explain=False)
print(type(res))

while res.next():
    i += 1
    print(str(i)+ str(res))


exit()