import requests
import json

url = "https://api.infosimples.com/api-async/v2/show"
args = {
  "request_id": "cde0ea30-7752-46b1-9afd-b51370bd0538",
  "token": "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3"  
}

response = requests.get(url, args)
response_json = response.json()
response.close()

print(json.dumps(response_json))