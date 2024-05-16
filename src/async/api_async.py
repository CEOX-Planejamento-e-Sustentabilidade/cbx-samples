import base64
import uuid
import requests
import json

url = 'https://api.infosimples.com/api-async/v2/car/demonstrativo'

transaction_id = str(uuid.uuid4())
context = f"createdby={139};updatedby={139};transactionid={transaction_id}"
encode_context = base64.b64encode(context.encode("ascii"))

args = {
  "car": "MT-5102637-4AF7AA48774144498AD3039DD377D764",
  "token": "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3",
  "callback_url": "http://192.168.1.112:8000/CAR",
  "context": str(encode_context),
  "timeout": 300
}

response = requests.post(url, args)
response_json = response.json()
response.close()

print(json.dumps(response_json))