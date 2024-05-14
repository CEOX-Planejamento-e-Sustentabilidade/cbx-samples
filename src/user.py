import base64
from hashlib import sha256


def encrypt_password(password):
    return sha256(password.encode('utf-8')).hexdigest()

def decrypt_password(password):
    return sha256(password.decode('utf-8')).hexdigest()

print(encrypt_password('hello'))
print(decrypt_password('c61a70bc12861406e1d7bcefaa4b9f744bfe656723c778996f1254151c1d2861'))