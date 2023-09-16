from faker import Faker
import csv
import random
from datetime import datetime


fake=Faker('it_It')

#Funzione per la generazione delle persone 
def people_generator(num_person, percentage):
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

        write_on_file("people", people, header, percentage)






#codice per generae le celle telefoniche 
def cells_generator(num_cells, percentage):
    global cells_list
    header=["id","city","state","address"]
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
    
    write_on_file("cells", cells_list, header, percentage)

        


def calls_generator(num_calls, percentage):
    global cells_list
    global people
    start_date = datetime(2023,1,1)
    end_date = datetime(2023,1,3)
    calls = []
    header=["cell_site", "calling","called","startdate","enddate","duration"]

    
    for i in range(num_calls):
        call = []
        row1 = random.randint(1, len(people)-1)
        row2 = random.randint(1, len(people)-1)
        site = cells_list[random.randint(1, len(cells_list)-1)][0]
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
        
    write_on_file("calls", calls, header, percentage)
    
    


def write_on_file(filename, list, headers, percentage):
    f =open ("csv/"+filename+str(percentage)+".csv",'w')
    writer=csv.writer(f)
    writer.writerow(headers)
    writer.writerows(list)
    f.flush()

'''Definizione delle variabili utili'''
percentage = [25, 50, 75, 100]
persons = 4000
calls = 200000
cells = 2000

for p in percentage:

    num_persons=persons * p // 100
    num_calls=calls * p // 100
    num_cells=cells * p // 100

    people = []
    cells_list = []

    cells_generator(num_cells, p)
    people_generator(num_persons, p)
    calls_generator(num_calls, p)

