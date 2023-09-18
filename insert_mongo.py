import pymongo 
import csv 

#conversione da stringa a intero
def dictate(csv):
    for k, e in enumerate(csv):
        for f, i in e.items():
            if i.isdigit():
                e[f] = int(i)
    return csv

    

#ci colleghiamo al container che esegue localmente mongodb 
client = pymongo.MongoClient("localhost", 27017)

percentage = [25, 50, 75, 100]

for p in percentage:

    #connessione al database
    db = client["progetto"+str(p)] 

    """
    Creazione 3 collezioni per distinte 
    -1 Calls per identificare le chiamate definite da: id, numero chiamante, numero chiamato, durata
    -2 cells per identificare le celle telefoniche coinvolte nelle chiamate con: id, citta', stato, indirizzo
    -3 people per identificare le persone coinvolte caratterizzate da: numero di telefono, nome, cognome, nome completo
    """

    calls_coll = db["calls"]
    cells_coll= db["cells"]    
    people_coll = db ["people"]

    #apertura dei file csv
    calls_file = open("csv/calls"+str(p)+".csv")
    cells_file = open("csv/cells"+str(p)+".csv")
    people_file = open("csv/people"+str(p)+".csv")

    #creazione oggetto DictReader dai file csv
    calls_reader = csv.DictReader(calls_file)
    cells_reader = csv.DictReader(cells_file)
    people_reader = csv.DictReader(people_file)

    #conversione dei valori numerici da stringa a intero
    calls_list = dictate(list(calls_reader))
    cells_list = dictate(list(cells_reader))
    people_list = dictate(list(people_reader))

    #inserimento dei dati nelle collezioni e creazione dell'indice
    insert_cells = cells_coll.insert_many(cells_list)
    people_coll.create_index("number", unique=True, background=True)
    insert_people = people_coll.insert_many(people_list)
    insert_calls = calls_coll.insert_many(calls_list)
