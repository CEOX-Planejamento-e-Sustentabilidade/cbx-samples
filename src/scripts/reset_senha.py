from hashlib import sha256
import json

import jwt

JWT_SECRET='elvWTMVhPyuQH76otMUx8k5krvRZ88'
JWT_AUTH_HEADER_PREFIX='Bearer'

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
    return json.dumps({"auth": True,
                    "token": jwt.encode({"email": 'ti@cbxsustentabilidade.com.br', "user": user}, JWT_SECRET, algorithm='HS256'),
                    "role": 'admin',
                    "clients": "[3,2,1,4,5,6,7,8,9,10,11,12,13,14,15]"})

reset_password('p3dr02025')
reset_password('cri$2025')
reset_password('luc@2025')
reset_password('nicol3@2025')