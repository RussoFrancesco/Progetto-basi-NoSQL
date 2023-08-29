import pymongo 
import csv 

def dictate(reader):
    dictionary = []
    for row in reader:
        dictionary.append(row)
    return dictionary
    

#ci colleghiamo al container che esegue localmente mongodb 
client = pymongo.MongoClient("localhost", 27017)

db = client["progetto"] #creazione db test tramite ogg client 

"""
Creazione 3 collezioni per distinte 
-1 Calls per identificare le chiamate definite da: id, numero chiamante, numero chiamato, durata
-2 cells per identificare le celle telefoniche coinvolte nelle chiamate con: id, citta', stato, indirizzo
-3 people per identificare le persone coinvolte caratterizzate da: numero di telefono, nome, cognome, nome completo

"""


#
calls_coll = db["calls"]
cells_coll= db["cells"]    
people_coll=db ["people"]

calls_file = open("calls.csv")
cells_file = open("cells.csv")
people_file = open("people.csv")


calls_reader = csv.DictReader(calls_file)
cells_reader = csv.DictReader(cells_file)
people_reader = csv.DictReader(people_file)

calls_list = list(dictate(calls_reader))
cells_list = list(dictate(cells_reader))
people_list = list(dictate(people_reader))

insert_calls = calls_coll.insert_many(calls_list)
insert_cells = cells_coll.insert_many(cells_list)
insert_people = people_coll.insert_many(people_list)


    
