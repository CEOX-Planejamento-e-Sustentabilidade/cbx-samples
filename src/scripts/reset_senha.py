
import json

import jwt
from config import JWT_SECRET
from hashlib import sha256

def encrypt_password(password):    




    return sha256(password.encode('utf-8')).hexdigest()

def reset_password(new_pass):
    try:
        #new_pass = 'cri$2025'
        encrypt_pass = encrypt_password(new_pass)
        print(f'pass: {new_pass} - encrypt_pass: {encrypt_pass}')
    except Exception as e:
        print(str(e))

def decode():
    user = {}
    return json.dumps({"auth": True,
                    "token": jwt.encode({"email": 'ti@cbxsustentabilidade.com.br', "user": user}, JWT_SECRET, algorithm='HS256'),
                    "role": 'admin',
                    "clients": "[3,2,1,4,5,6,7,8,9,10,11,12,13,14,15]"})

#manuela@ceoxplan.com.br
#nadia@ceoxplan.com.br
#gabriella@ceoxplan.com.br
#reset_password('manu@2025')
#reset_password('nadi@2025')
#reset_password('gabr@2025')
# matheus@ceoxplan.onmicrosoft.com
#reset_password('rxftkdspok') #ce91562991917dfb722b5f2c8b43a45eb18b8dbf5a00b979c544fda8c35e7d87
reset_password('edmar_z') #ce91562991917dfb722b5f2c8b43a45eb18b8dbf5a00b979c544fda8c35e7d87