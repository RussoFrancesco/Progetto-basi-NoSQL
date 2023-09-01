from py2neo import Graph, Relationship, Node
from csv import DictReader

graph = Graph("neo4j://localhost:7687")

people_csv = DictReader(open("csv/people.csv"))
cells_csv = DictReader(open("csv/cells.csv"))
calls_csv = DictReader(open("csv/calls.csv"))

graph.begin()

for row in people_csv:
    node = Node("person", full_name=row["full_name"],first_name=row["first_name"],last_name=row["last_name"],numbers=row["number"])
    graph.create(node)

for row in cells_csv:
    node = Node("cell", id=row["id"], city=row["city"], state=row["state"],address=row["address"])
    graph.create(node)

for row in calls_csv:
    node = Node("call", cell_site=row["cell_site"], calling=row["calling"], called=row["called"], startdate=row["startdate"], enddate=row["enddate"], duration=row["duration"])
    graph.create(node)

graph.commit()

