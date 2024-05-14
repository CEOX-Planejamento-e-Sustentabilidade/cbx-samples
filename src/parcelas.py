#/consultas/status/incra/sigef/detalhes-parcela
#/consultas/status/incra/sigef/parcelas

#Matrícula Figueirópolis
#INCRA 6270200045610

#Matrícula Alvorada
#INCRA 9240160015113

import requests
import json

url = 'https://api.infosimples.com/api/v2/consultas/status/incra/sigef/parcelas'
args = {
  "codigo_imovel": "9240160015113",
  "token": "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3"
}

response = requests.post(url, args)
response_json = response.json()
response.close()

if response_json['code'] == 200:
  print("Retorno com sucesso: ", response_json['data'])
elif response_json['code'] in range(600, 799):
  mensagem = "Resultado sem sucesso. Leia para saber mais: \n"
  mensagem += "Código: {} ({})\n".format(response_json['code'], response_json['code_message'])
  mensagem += "; ".join(response_json['errors'])
  print(mensagem)

print("Cabeçalho da consulta: ", response_json['header'])
print("URLs com arquivos de visualização (HTML/PDF): ", response_json['site_receipts'])
print(json.dumps(response_json))