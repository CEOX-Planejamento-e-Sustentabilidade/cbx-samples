from urllib.parse import urlencode
import requests
import json

headers = { 'Authorization': '' }
cars = ["MT-5100102-30B854D6342C4B2BBD5C1EC02A056092"]

def get_car_report():
    params = {'client_id': 1, 'nr_cars': cars}
    url_api = f'http://localhost:5000/car_report'
    response = requests.get(url_api, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data))
    else:
        print('Failed to fetch data:', response.status_code)

def get_car_input():
    params = {'client_id': 1, 'nr_cars': cars}
    url_api = f'http://localhost:5000/car_input'

    response = requests.get(url_api, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data))
    else:
        print('Failed to fetch data:', response.status_code)

#get_car_report()
get_car_input()