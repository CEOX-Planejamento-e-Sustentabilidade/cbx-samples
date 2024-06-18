import requests
import json

url = 'https://api.infosimples.com/api/v2/consultas/car/demonstrativo'
args = {
  "car":     "RJ-3301009-C7EB51D58BCD4D60A5D45815C9111588",
  "token":   "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3",
  "timeout": 300
}

try:
  response = requests.post(url, args)
  response_json = response.json()
  response.close()
except Exception as ex:
  print(ex)


# if response_json['code'] == 200:
#   print("Retorno com sucesso: ", response_json['data'])
# elif response_json['code'] in range(600, 799):
#   mensagem = "Resultado sem sucesso. Leia para saber mais: \n"
#   mensagem += "Código: {} ({})\n".format(response_json['code'], response_json['code_message'])
#   mensagem += "; ".join(response_json['errors'])
#   print(mensagem)

#print("Cabeçalho da consulta: ", response_json['header'])
#print("URLs com arquivos de visualização (HTML/PDF): ", response_json['site_receipts'])
print(json.dumps(response_json))