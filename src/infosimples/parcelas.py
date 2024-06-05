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

  # "cpf":           "VALOR_DO_PARAMETRO_CPF",
  # "cnpj":          "VALOR_DO_PARAMETRO_CNPJ",
  # "codigo_imovel": "VALOR_DO_PARAMETRO_CODIGO_IMOVEL",
  # "pagina":        "VALOR_DO_PARAMETRO_PAGINA",
  # "login_cpf":     "VALOR_DO_PARAMETRO_LOGIN_CPF",
  # "login_senha":   "VALOR_DO_PARAMETRO_LOGIN_SENHA",
  # #"pkcs12_cert":   aes256.encrypt(base64.b64encode(open("certificado.pfx", "rb").read()).decode(), "INFORME_A_CHAVE_DE_CRIPTOGRAFIA"),
  # #"pkcs12_pass":   aes256.encrypt("SENHA_DO_CERTIFICADO", "INFORME_A_CHAVE_DE_CRIPTOGRAFIA"),
  # "token":         "INFORME_AQUI_O_TOKEN_DA_CHAVE_DE_ACESSO",
  # "timeout":       300


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