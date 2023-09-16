from faker import Faker
import csv
import random
from datetime import datetime


fake=Faker('it_It')

#Funzione per la generazione delle persone 
def people_generator(num_person):
    
    people=[]

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

    return people

      






#codice per generae le celle telefoniche 
def cells_generator(num_cells):
    cells_list= []
    
    startid=fake.unique.pyint()

    for i in range(num_cells):
        startid+=1
        cell = [
                    startid,
                    fake.city(),
                    fake.state(),
                    fake.street_name() +", "+ fake.building_number()
        ]
        cells_list.append(cell)
    return cells_list


        


def calls_generator(num_calls,people,cells_list):
    start_date = datetime(2023,1,1)
    end_date = datetime(2023,1,3)
    calls = []

    for i in range(num_calls):
        call = []
        row1 = random.randint(0, len(people)-1)
        row2 = random.randint(0, len(people)-1)
        site = cells_list[random.randint(0, len(cells_list)-1)][0]
        start_timestamp = fake.unix_time(end_date, start_date)
        end_timestamp = start_timestamp + random.randint(60, 3600)
        
        call = [
                site,
                people[row1][3],
                people[row2][3],
                start_timestamp,
                end_timestamp,
                end_timestamp - start_timestamp
            ]
        
        if len(call):
            calls.append(call)
    return calls
        
    
    
    


def write_on_file(filename, list, headers, percentage):
    f =open ("csv/"+filename+str(percentage)+".csv",'w')
    writer=csv.writer(f)
    writer.writerow(headers)
    writer.writerows(list)
    f.flush()

'''Definizione delle variabili utili'''
percentage = [25, 50, 75, 100]
people= 4000
calls = 200000
cells = 2000

header_cells=["id","city","state","address"]
header_people=["full_name","first_name","last_name","number"]
header_calls=["cell_site", "calling","called","startdate","enddate","duration"]

cells_list=cells_generator(cells)
people_list=people_generator(people)


for p in percentage:
    num_people=people*p//100
    num_cells=cells*p//100
    num_calls=calls*p//100
    calls_list=calls_generator(calls,people_list[:num_people+1],cells_list[:num_cells+1])
    write_on_file("people",people_list[:num_people+1],header_people,p)
    write_on_file("cells",cells_list[:num_cells+1],header_cells,p)
    write_on_file("calls",calls_list[:num_calls+1],header_calls,p)