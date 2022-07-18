import requests
import os

URL = "http://127.0.0.1:5000"

program_on = True
while program_on:
    print("1) upload")
    print("2) list")
    print("3) Download")
    print("4) remove")
    print("5) exit")
    choice = int(input("Choice: "))

    if choice == 1:
        file_address = input("File Address: ")
        files={'file': open(file_address, 'rb')}
        file_size= int(os.stat(file_address).st_size)
        values = {'size': file_size}
        r = requests.post(f'{URL}/upload', files=files, data=values)
        print(r.text)

    if choice == 2:
        r = requests.get(f'{URL}/list')
        print(r.text)

    if choice == 3:
        filename = input("Filename: ")

        response = requests.get(f'{URL}/download/{filename}')
        file=response.headers.get("Content-Disposition").split("filename=")[1]
        open(file, "+wb").write(response.content)
         
    if choice == 4:
        filename = input("Filename: ")
        r = requests.get(f'{URL}/remove?filename={filename}')
        print(r.text)

    if choice == 5:
        print("Goodbye!")
        program_on = False
