import json
import os
import boto3
from functools import lru_cache

@lru_cache()
def get_secrets():
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId='cbx-plataforma-cripto')
    secret_string = response["SecretString"]
    return json.loads(secret_string)

def get(key_suffix, default=None):    
    key = f"/cbx/{ENVIRONMENT}/{key_suffix}"
    return secrets.get(key, default)

secrets = get_secrets()
     
# environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev") #dev ou prod
DEBUG = ENVIRONMENT == "dev"
URL_PLATFORM = 'https://fs.cbxsustentabilidade.com.br'

# sintegra
TOKEN_SINTEGRAWS = get('sintegra/token')

# s3
S3_BUCKET_NAME = get('s3/bucket_name')
S3_WAIT_TIME_SECONDS = get('s3/wait_time_seconds')

# sqs
SQS_PROCESSAMENTO_RENOVABIO = get('sqs/processamento_renovabio')
SQS_PROCESSAMENTO_RENOVABIO_DLQ = get('sqs/processamento_renovabio_dlq')
SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER = get('sqs/processamento_renovabio_dispatcher')
SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER_DLQ = get('sqs/processamento_renovabio_dispatcher_dlq')
SQS_WAIT_TIME_SECONDS = get('sqs/wait_time_seconds')

# sendgrid
SENDGRID_API_KEY = get('sendgrid/api_key')

# sharepoint
SHAREPOINT_KEY_PATH = 'src/certificate_keys/sharepoint/key.pem'

# api
JWT_SECRET = get('api/jwt_secret')
JWT_AUTH_HEADER_PREFIX = get('api/jwt_auth_header_prefix')
ROOT_UPLOAD_FOLDER = 'uploads'
API_URL = 'http://localhost:5000'
EMAIL_FROM = get('user/email')

# user
USER_EMAIL = get('user/email')
USER_PASS = get('user/pass')

# database
PG_USER = get('database/pg_user')
PG_PASSWORD = get('database/pg_password')
PG_DATABASE = get('database/pg_database')
PG_HOST = get('database/pg_host')
PG_PORT = get('database/pg_port')
SQLALCHEMY_DATABASE_URI = get('database/sqlalchemy_database_uri')

# trello
TRELLO_API_KEY = get('trello/api_key')
TRELLO_API_SECRET = get('trello/api_secret')
TRELLO_TOKEN = get('trello/token')

# infosimples
INFOSIMPLES_TOKEN = get('infosimples/token')
INFOSIMPLES_TIMEOUT = get('infosimples/timeout')
INFOSIMPLES_HTTPX_TIMEOUT = get('infosimples/httpx/timeout')
INFOSIMPLES_CALLBACK_URL = get('infosimples/call_back_url')
INFOSIMPLES_CALLBACK_SECRET = get('infosimples/call_back_secret')
INFOSIMPLES_URL_SHOW = get('infosimples/url_show')
