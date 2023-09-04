import pymongo


percentage = [25, 50, 75, 100]

result_csv = open("result.csv", "w")

client = pymongo.MongoClient("localhost", 27017)

query = []

for p in percentage:
    call = client["progetto"+str(p)]["call"]
    if call:
        print("si")
