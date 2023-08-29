from faker import Faker
import csv
import os
import random
from datetime import date
from datetime import datetime
from threading import Thread
from time import time


fake=Faker('it_It')

'''Definizione delle variabili utili'''
percentage = 100
persons = 20000
calls = 1000000
cells = 16000


persons=int(persons * (percentage/100))
calls=int(calls * (percentage/100))
cells=int(cells * (percentage/100))

people = []

#Funzione per la generazione delle persone 
def people_generator(num_person):
    header=["full_name","first_name","last_name","number"]
    global people

    for i in range(num_person):
        person = []
        first_name=fake.first_name()
        last_name=fake.last_name()

        while True:
            phone = fake.unique.phone_number()
            if phone[0]=='3' and len(phone) == 10:
                break

        fullname=first_name+last_name
        person.append(fullname)
        person.append(first_name)
        person.append(last_name)
        person.append(phone)
        people.append(person)

        write_on_file("people", people, header)






#codice per generae le celle telefoniche 
def cells_generator(num_cells):
    header=["id","city","state","address"]
    cells = []
    startid=fake.unique.pyint()

    for i in range(num_cells):
        startid+=1
        cell = [
                    startid,
                    fake.city(),
                    fake.state(),
                    fake.street_name() +", "+ fake.building_number()
        ]
        cells.append(cell)
    
    write_on_file("cells", cells, header)

        


def calls_generator(num_calls):
    start_date = datetime(2023,1,1)
    end_date = datetime(2023,1,3)
    
    header=["id","calling","called","startdate","enddate","duration"]
    calls = []


    with open("people.csv", 'r') as file:
        csv_reader = csv.reader(file)
        num_rows = sum(1 for row in csv_reader)
    
    startid=fake.unique.pyint()

    for i in range(num_calls):
        call = []
        row1 = random.randint(1, len(people)-1)
        row2 = random.randint(1, len(people)-1)
        start_timestamp = fake.unix_time(end_date, start_date)
        end_timestamp = start_timestamp + random.randint(60, 3600)
        
        call=[
                startid,
                people[row1][3],
                people[row2][3],
                start_timestamp,
                end_timestamp,
                end_timestamp - start_timestamp
            ]
        if len(call):
            calls.append(call)
        
        startid+=1

    write_on_file("calls", calls, header)
    
    


def write_on_file(filename, list, headers):
    f =open (filename+".csv",'w')
    writer=csv.writer(f)
    writer.writerow(headers)
    writer.writerows(list)
    f.flush()



cells_generator(cells)
people_generator(persons)
calls_generator(calls)

