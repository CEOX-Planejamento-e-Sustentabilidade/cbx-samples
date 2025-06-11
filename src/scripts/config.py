import json
import os
import boto3
import yaml
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet

# carrega configs
def load_encrypted_config(enrcypted_file_name, key):
    base_path = os.path.dirname(os.path.abspath(__file__))
    encrypted_file_path = os.path.join(base_path, 'cripto_worker', enrcypted_file_name)
    try:
        cipher = Fernet(key)
        with open(encrypted_file_path, "rb") as f:
            decrypted_data = cipher.decrypt(f.read())
        return yaml.safe_load(decrypted_data)
    except Exception as ex:
        raise ex

# recupera a secret
def get_secrets():
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId='cbx-plataforma-cripto')
        secrets = json.loads(get_secret_value_response['SecretString'])
        return secrets
    except ClientError as e:
        raise e

ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')  # default to 'development'
secret = get_secrets()

if ENVIRONMENT == 'production':
    config = load_encrypted_config("config.prod.enc", str(secret['cbx-plataforma-key-prod']).encode())
else:
    config = load_encrypted_config("config.dev.enc", str(secret['cbx-plataforma-key-dev']).encode())
        
# environment
DEBUG = config["environment"]["debug"] == "true"
EMAIL_FROM = config["environment"]["email_from"]
URL_PLATFORM = config["environment"]["url_platform"]

# sintegra
TOKEN_SINTEGRAWS = config["sintegra"]["token"]

# aws
AWS_ACCESS_KEY = config["aws"]["access_key"]
AWS_SECRET_KEY = config["aws"]["secret_key"]
AWS_REGION_NAME = config["aws"]["region_name"]

# s3
S3_BUCKET_NAME = config["s3"]["bucket_name"]
S3_WAIT_TIME_SECONDS = config["s3"]["wait_time_seconds"]

# ssm
SSM_SERVICE_NAME = config["ssm"]["service_name"]
SSM_SECRET_ID = config["ssm"]["secret_id"]

# sqs
SQS_PROCESSAMENTO_RENOVABIO = config["sqs"]["sqs_processamento_renovabio"]
SQS_PROCESSAMENTO_RENOVABIO_DLQ = config["sqs"]["sqs_processamento_renovabio_dlq"]
SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER = config["sqs"]["sqs_processamento_renovabio_dispatcher"]
SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER_DLQ = config["sqs"]["sqs_processamento_renovabio_dispatcher_dlq"]
SQS_WAIT_TIME_SECONDS = config["sqs"]["wait_time_seconds"]

# sendgrid
SENDGRID_API_KEY = config["sendgrid"]["sendgrid_api_key"]

# sharepoint
SHAREPOINT_KEY_PATH = config["sharepoint"]["sharepoint_key_path"]

# api
JWT_SECRET = config["api"]["jwt_secret"]
JWT_AUTH_HEADER_PREFIX = config["api"]["jwt_auth_header_prefix"]
ROOT_UPLOAD_FOLDER = config["api"]["root_upload_folder"]
API_URL = config["api"]["url"]

# user
USER_EMAIL = config["user"]["email"]
USER_PASS = config["user"]["pass"]

# database
PG_USER = config["database"]["pg_user"]
PG_PASSWORD = config["database"]["pg_password"]
PG_DATABASE = config["database"]["pg_database"]
PG_HOST = config["database"]["pg_host"]
PG_PORT = config["database"]["pg_port"]  
SQLALCHEMY_DATABASE_URI = config["database"]["sqlalchemy_database_uri"]

# trello
TRELLO_API_KEY = config["trello"]["trello_api_key"]
TRELLO_API_SECRET = config["trello"]["trello_api_secret"]
TRELLO_TOKEN = config["trello"]["trello_token"]