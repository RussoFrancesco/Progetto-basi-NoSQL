import pymongo
import datetime


percentage = [25, 50, 75, 100]

result_csv = open("csv/result.csv", "w")

client = pymongo.MongoClient("localhost", 27017)

start_search = 1672617600
end_search = 1672703999

query = [{"$match": {"StartDate": {"$gte": start_search,
                                      "$lt": end_search}}}]

for p in percentage:
    call = client["progetto"+str(p)]["call"]
    start_time = datetime.datetime.now()
    call.find(query[0])
    end_time = datetime.datetime.now() - start_time
    print(end_time)