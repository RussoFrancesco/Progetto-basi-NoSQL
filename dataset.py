from faker import Faker
import csv
import os
from datetime import date
from datetime import datetime
from threading import Thread


fake=Faker('it_It')

'''Definizione delle variabili utili'''
percentage = 10
persons = 100
calls = 1000
cells = 500
start_date = [2023, 1, 1]
end_date = [2023, 12, 31]

persons=int(persons * (percentage/100))
calls=int(calls * (percentage/100))
cells=int(cells * (percentage/100))


    #file = open("people.csv", 'w')


#Funzione per la generazione delle persone 
def people_generator(num_person):
    header=["full_name","first_name","last_name","number"]
    people = []

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







def cells_generator():
    pass


def calls_generator():
    pass


def write_on_file():
    pass


people_generator(persons)