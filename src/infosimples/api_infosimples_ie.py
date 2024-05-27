import requests
import json

# url = 'https://api.infosimples.com/api/v2/consultas/sintegra/ms'
# args = {
#   "cnpj":    "", #12995806000499
#   "ie":      "", #136560490
#   "cpf":     "36300870863", #66041147915
#   "token":   "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3",
#   "timeout": 300
# }

url = 'https://api.infosimples.com/api/v2/consultas/sintegra/unificada'
args = {
  "uf":          "MT", #MT
  "cnpj":        "", #12995806000499
  "ie":          "",
  "ie_produtor": "",
  "cpf":         "21214913920",#66041147915 70657505153
   "token":   "NuUBrhgD5v4nIWfb4bgrmXNwg7AALulkSq-BsYk3",
   "timeout": 300
}

response = requests.post(url, args)
response_json = response.json()
response.close()

if response_json['code'] == 200:
  #print("Retorno com sucesso: ", response_json['data'])
  print(json.dumps(response_json))
elif response_json['code'] in range(600, 799):
  mensagem = "Resultado sem sucesso. Leia para saber mais: \n"
  mensagem += "Código: {} ({})\n".format(response_json['code'], response_json['code_message'])
  mensagem += "; ".join(response_json['errors'])
  print(mensagem)

#print("Cabeçalho da consulta: ", response_json['header'])
#print("URLs com arquivos de visualização (HTML/PDF): ", response_json['site_receipts'])