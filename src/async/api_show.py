import requests
import json

#url = "https://api.infosimples.com/api-async/v2/car/demonstrativo"       
url = "https://api.infosimples.com/api-async/v2/show"
args = {
  "request_id": "d97c0a51-014f-4f8c-9fda-6ed8abfe8f8c",
  "token": "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3"  
}

response = requests.get(url, args)
response_json = response.json()
response.close()

print(json.dumps(response_json))