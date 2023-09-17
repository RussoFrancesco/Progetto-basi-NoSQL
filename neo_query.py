from py2neo import Graph
import csv
import time
import numpy as np


def confidence(data):
    n = len(data)
    mean = np.mean(data)
    std_dev = np.std(data, ddof=1)

    margin = (std_dev / np.sqrt(n)) * 1.96

    lower = mean - margin
    upper = mean + margin

    return (lower, upper)


percentage = [25, 50, 75, 100]

query = [
       "MATCH (p:person)  \
        WHERE p.first_name='Antonio' AND p.last_name='Moresi' \
        RETURN p",

       "MATCH (p:person)-[r1:IS_CALLING]->(c:call) \
        WHERE c.startdate>=1672617600 AND c.startdate<1672703999 \
        AND c.duration>=900 \
        AND p.first_name='Giovanni'\
        RETURN p,c,r1",

       "MATCH (p:person)-[r1:IS_CALLING]->(c1:call)-[r2:IS_DONE]->(c2:cell) \
        WHERE c1.startdate>=1672617600 AND c1.startdate<1672703999 \
        AND p.first_name='Giovanni' \
        AND c2.state='Roma' \
        RETURN p,c1,r1,r2,c2",

       "MATCH (p1:person)-[r1:IS_CALLING]->(c1:call)-[r2:IS_DONE]->(c2:cell), (p2:person)<-[r3:IS_CALLED]->(c1) \
        WHERE c1.startdate>=1672617600 AND c1.startdate<1672703999 \
        AND \
        ((p1.first_name='Giovanni' AND  p2.first_name='Cassandra') OR \
        (p1.first_name='Cassandra' AND  p2.first_name='Giovanni')) \
        RETURN p1,c1,r1,r2,c2,p2,r3"
    ]

result_csv = open("csv/result_neo.csv", "w", newline='')
writer_result = csv.writer(result_csv)
headers = ['Query', 'Dimensione', 'Tempo prima esecuzione', 'Tempo delle 30 esecuzioni','Tempo medio', 'Intervallo di confidenza sup', 'Intervallo di confidenza inf']
writer_result.writerow(headers)

for j in range(1, len(query)+1):
    for p in percentage:
        results = ["Query"+str(j), str(p)+"%"]
        graph = Graph("neo4j://127.0.0.1:7688", name="progetto"+str(p))
        
        start = time.time()
        graph.run(query[j-1])
        end = (time.time() - start) * 1000
        results.append(end)
        print("prima exe"+" Query"+str(j))

        data = []
        for i in range(40):
            start30 = time.time()
            query_res = graph.run(query[j-1])
            end30 = (time.time() - start30) * 1000
            data.append(end30)
        results.append(sum(data))
        print("30 exe")

        avg = results[3]/40
        results.append(avg)
        print("avg")

        confidence_lvl = confidence(data)
        print(confidence_lvl)
        results.append(confidence_lvl[1])
        results.append(confidence_lvl[0])

        writer_result.writerow(results)
