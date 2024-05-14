from urllib.parse import urlencode
import requests
import json

headers = { 'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6InJjYnVsbEBnbWFpbC5jb20iLCJ1c2VyIjpbMSwicmNidWxsQGdtYWlsLmNvbSIsIjlmM2M0Yzg1OTczZGY3YjA4NzliZGQzMWQ2Zjc0ZGRhZjk5NzUzNzgwODg3MGMyZjE1N2E2OTQ0MjRhZWQ0MDgiLHsiY2xpZW50cyI6WzMsMiwxLDQsNSw2LDddLCJuYW1lIjoiUmljYXJkbyJ9LGZhbHNlLCJhZG1pbiJdfQ.go9bzu7Q2n7SuEW8oJxtV5B1QkohVajgV16S77EWwjg' }
#cars = [ "MT-5104526-D227623ED4654F298AAE9632847245E4", "MT-5108907-A0A881EBF9444B079C558F7F8D9F7904", "MT-5107941-0F0557A79DE145B09F2D4789EC398BCA"]
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