from py2neo import Graph, Relationship, Node
from csv import DictReader
from threading import Thread

def add_relationship(nodo1, type, nodo2):
    relationship = Relationship(nodo1, type, nodo2)
    graph.create(relationship)

def create_nodes_from_csv(people_csv,cells_csv,calls_csv):
    #Creazione dell'oggetto nodo e dell'inserimento nel database
    for row in people_csv:
        node = Node("person", full_name=row["full_name"],first_name=row["first_name"],last_name=row["last_name"],number=int(row["number"]))
        graph.create(node)

    for row in cells_csv:
        node = Node("cell", id=int(row["id"]), city=row["city"], state=row["state"],address=row["address"])
        graph.create(node)

    for row in calls_csv:
        node = Node("call", cell_site=int(row["cell_site"]), calling=int(row["calling"]), called=int(row["called"]), startdate=int(row["startdate"]), enddate=int(row["enddate"]), duration=int(row["duration"]))
        graph.create(node)

        results=[]

        queries = [
                "MATCH (p1:person) WHERE \
                p1.number="+row["calling"]+" \
                RETURN p1",

                "MATCH (p2:person) WHERE \
                p2.number="+row["called"]+" \
                RETURN p2",

                "MATCH (c2:cell) WHERE \
                c2.id="+row["cell_site"]+" \
                RETURN c2"
        ]

        for i in range(len(queries)):
            results.append(graph.run(queries[i]))

        chiamante = results[0].evaluate()
        chiamato = results[1].evaluate()
        cella = results[2].evaluate()

        add_relationship(node, "IS_CALLED", chiamato)
        add_relationship(chiamante, "IS_CALLING", node)
        add_relationship(node, "IS_DONE", cella)

#Connessione al database di neo4j
graph = Graph("neo4j://localhost:7687", name="progetto100")


#Apertura dei file csv
people_csv = DictReader(open("csv/people.csv"))
cells_csv = DictReader(open("csv/cells.csv"))
calls_csv = DictReader(open("csv/calls.csv"))

create_nodes_from_csv(people_csv, cells_csv, calls_csv)

#creazione relazioni sulla base del file calls.csv
#il file calls.csv contiene sia relazioni con people.csv e con cells.csv 
'''
for row in calls_csv:
    results = []

    #query da porre al db no4j per risalire ai nodi per poi costruire le relazioni
    queries =[ "MATCH (c1:call) WHERE \
    c1.cell_site="+row["cell_site"] +" AND \
    c1.calling="+row["calling"]+" AND \
    c1.called="+row["called"] +" AND \
    c1.enddate="+row["enddate"] +" AND \
    c1.startdate="+row["startdate"] +" AND \
    c1.duration=" +row["duration"] +"\
    RETURN c1 ",
    
    "MATCH (p1:person) WHERE \
    p1.number="+row["calling"]+" \
    RETURN p1",

    "MATCH (p2:person) WHERE \
    p2.number="+row["called"]+" \
    RETURN p2",

    "MATCH (c2:cell) WHERE \
    c2.id="+row["cell_site"]+" \
    RETURN c2"
    ]

    #esecuzione delle query
    for i in range(len(queries)):
        results.append(graph.run(queries[i]))
    
    #recupero i risultati delle query e le assegno nelle ripsettive variabili
    #evaluate è usato nelle query con 1 solo risultato per recuperare il nodo associato
    chiamata = results[0].evaluate()
    chiamante = results[1].evaluate()
    chiamato = results[2].evaluate()
    cella = results[3].evaluate()

    add_relationship(chiamata, "IS_CALLED", chiamato)
    add_relationship(chiamante, "IS_CALLING", chiamata)
    add_relationship(chiamata, "IS_DONE", cella)
    '''
