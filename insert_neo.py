from py2neo import Graph, Relationship, Node
from csv import DictReader

def add_relationship(nodo1, type, nodo2):
    #creazione della relazione
    relationship = Relationship(nodo1, type, nodo2)
    graph.create(relationship)

def create_nodes_from_csv(people_csv,cells_csv,calls_csv):
    '''Creazione dell'oggetto nodo e dell'inserimento nel database
        Andiamo ad inserire tutte le persone e le celle anadando a recuperare i dati generati sul file peoples.csv e cells.csv'''
    for row in people_csv:
        node = Node("person", full_name=row["full_name"],first_name=row["first_name"],last_name=row["last_name"],number=int(row["number"]))
        graph.create(node)

    for row in cells_csv:
        node = Node("cell", id=int(row["id"]), city=row["city"], state=row["state"],address=row["address"])
        graph.create(node)

    ''' I nodi calls oltre alla creazione saranno utilizzati in quanto, nella forma tabellare del files csv si  hanno 3 campi (foreign keys) 
        che legano le chiamate al chiamato,al chiamante (entrambi identificati dal numero in people.csv) e la cella (id in cells.csv) '''
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

        #recupero dell'oggetto nodo risultante dalle query
        chiamante = results[0].evaluate()
        chiamato = results[1].evaluate()
        cella = results[2].evaluate()

        #creazione delle relazioni con il nodo chiamata
        add_relationship(node, "IS_CALLED", chiamato)
        add_relationship(chiamante, "IS_CALLING", node)
        add_relationship(node, "IS_DONE", cella)


percentage = [25, 50, 75, 100]

for p in percentage:
    #Connessione al database di neo4j
    graph = Graph("neo4j://localhost:7687", name="progetto"+str(p))


    #Apertura dei file csv
    people_csv = DictReader(open("csv/people"+str(p)+".csv"))
    cells_csv = DictReader(open("csv/cells"+str(p)+".csv"))
    calls_csv = DictReader(open("csv/calls"+str(p)+".csv"))

    #creazione dei nodi
    create_nodes_from_csv(people_csv, cells_csv, calls_csv)
