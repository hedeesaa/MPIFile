import requests
import os

URL = "http://127.0.0.1:5000"

program_on = True
while program_on:
    print("1) upload")
    print("2) list")
    print("5) exit")
    choice = int(input("Choice: "))

    if choice == 1:
        #'files/comp6231.2022s.a03.assignment.iii.pdf'
        file_address = input("File Address: ")
        files={'file': open(file_address, 'rb')}
        file_size= os.stat(file_address).st_size
        values = {'size': str(file_size)}
        r = requests.post(f'{URL}/upload', files=files, data=values)
        print(r.text)

    if choice == 2:
        r = requests.get(f'{URL}/list')
        print(r.text)
        
    if choice == 5:
        print("Goodbye!")
        program_on = False
