import secrets
import uuid
import jwt
from datetime import datetime, timedelta

def encodex():
    # Generate the JWT with the specified payload and secret key
    token = jwt.encode(payload, secret_key, algorithm='HS256')
    # Print or use the generated token
    print("Generated Token:", token)
    return token

def decodex():
    try:
        tkn = encodex() 
        # Decode and verify the JWT
        decoded_token = jwt.decode(tkn, secret_key, algorithms=['HS256'])

        # Access claims from the decoded token
        user_id = decoded_token['user_id']
        username = decoded_token['username']

        # Check if the token is still valid (not expired)
        current_time = datetime.utcnow()
        expiration_time = datetime.utcfromtimestamp(decoded_token['exp'])
        if expiration_time > current_time:
            print("Token is valid.")
            print("User ID:", user_id)
            print("Username:", username)
        else:
            print("Token has expired.")
    except jwt.ExpiredSignatureError:
        print("Token has expired.")
    except jwt.InvalidTokenError:
        print("Invalid token.")

def gen_secret_key():
    # Generate a random 256-bit (32-byte) secret key
    secret_key = secrets.token_hex(32)
    guid = str(uuid.uuid4())
    print(f"Generated Secret Key: {secret_key}\nUUID: {guid}")
    return secret_key

# Define your secret key (keep it secret!)
secret_key = gen_secret_key()
# Define the payload (claims) for the JWT
payload = {
    'user_id': 123,
    'username': 'john_doe',
    'exp': datetime.utcnow() + timedelta(hours=1)  # Set expiration time (e.g., 1 hour from now)
}

decodex()