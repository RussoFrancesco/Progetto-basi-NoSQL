from py2neo import Graph
import csv
import time
from mongo_query import confidence


percentage = [25, 50, 75, 100]

query = [
       "MATCH (c:call)  \
        WHERE c.startdate >= 1672617600 \
            AND c.startdate < 1672703999   \
        RETURN c",

       "MATCH (p1:person)-[r1:IS_CALLING]->(c:call)  \
        WHERE c.startdate >= 1672617600 \
            AND c.startdate < 1672703999   \
        RETURN p1, r1, c",

       "MATCH (p1:person)-[r1:IS_CALLING]->(c:call)-[r2:IS_DONE]->(ce:cell) \
        WHERE c.startdate >= 1672617600 \
            AND c.startdate < 1672703999   \
        RETURN p1, r1, c, r2, ce",

       "MATCH (p1:person)-[r1:IS_CALLING]->(c:call)-[r2:IS_DONE]->(ce:cell) \
        WHERE c.startdate >= 1672617600 \
            AND c.startdate < 1672703999 \
            AND c.duration >= 900   \
            AND c.duration < 1200 \
        RETURN p1, r1, c, r2, ce"
    ]

result_csv = open("csv/result_neo.csv", "w", newline='')
writer_result = csv.writer(result_csv)
headers = ['Query', 'Dimensione', 'Tempo prima esecuzione', 'Tempo delle 30 esecuzioni','Tempo medio', 'Intervallo di confidenza sup', 'Intervallo di confidenza inf']
writer_result.writerow(headers)

for j in range(1, len(query)+1):
    for p in percentage:
        results = ["Query"+str(j), "Dimensione"+str(p)]
        graph = Graph("neo4j://localhost:7687", name="progetto"+str(p))
        
        start = time.time()
        graph.run(query[j-1])
        end = (time.time() - start) * 1000
        results.append(end)

        data = []
        for i in range(30):
            start30 = time.time()
            graph.run(query[j-1])
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

