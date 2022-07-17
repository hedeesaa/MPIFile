import requests



# with open('file/comp6231.2022s.a03.assignment.iii.pdf', 'rb') as f:
#     requests.post('http://127.0.0.1:5000/upload', data=f)


files={'file': open('files/comp6231.2022s.a03.assignment.iii.pdf', 'rb')}
values = {'DB': 'photcat', 'OUT': 'csv', 'SHORT': 'short'}
r = requests.post('http://127.0.0.1:5000/upload', files=files, data=values)
