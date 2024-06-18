#/consultas/status/incra/sigef/detalhes-parcela
#/consultas/status/incra/sigef/parcelas

#Matrícula Figueirópolis
#INCRA 6270200045610

#Matrícula Alvorada
#INCRA 9240160015113

#22068769816
#Senha@123

import requests
import json
import jwt

SECRET_KEY = 'xsgdfytgdfjhdfjfikgfjfnjfjkdfprt90435i950r*/dffdgk59ejej4$%gf89443%%34534345$&^&%^*&%^gfdlkgmsdfjhjsd`121`2232131'
token = jwt.encode({'username': 'edmar'}, SECRET_KEY, algorithm='HS256')
print(token)
payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
print(payload)
# Extract the user's identity from the payload
username = payload['username']
print(username)

response_json = {
  'code': 600,
  'code_message': 'Erro no retorno da consulta.',
  'errors': ['Os dados não foram recebidos corretamente. Entre em contato com www.suporte.com.br', 'Erro no server']
  }
msg = "Código: {} - {}\n".format(response_json['code'], response_json['code_message'])
msg += "\n".join([f"Erro {index + 1}: {error}" for index, error in enumerate(response_json['errors'])])
print(msg)

url = 'https://api.infosimples.com/api/v2/consultas/incra/sigef/parcelas'
args = {
  #"cpf":           "VALOR_DO_PARAMETRO_CPF",
  #"cnpj":          "VALOR_DO_PARAMETRO_CNPJ",
  #"pagina":        "VALOR_DO_PARAMETRO_PAGINA",  
  "login_cpf":     "22068769816",
  "login_senha":   "Senha@123",  
  "codigo_imovel": "6270200045610",
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