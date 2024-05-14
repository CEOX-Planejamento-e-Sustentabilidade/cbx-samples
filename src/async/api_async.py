import base64
import requests
import json

url = 'https://api.infosimples.com/api-async/v2/car/demonstrativo'

args = {
  "car": "MT-5104526-D227623ED4654F298AAE9632847245E4",
  "token": "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3",
  "callback_url": "http://192.168.1.112:8000/CAR",
  "context": base64.b64encode("MT-5104526-D227623ED4654F298AAE9632847245E4".encode("ascii")),
  "timeout": 300
}

response = requests.post(url, args)
response_json = response.json()
response.close()

print(json.dumps(response_json))