import boto3

import os

from util_file import UtilFile
from util_xml import UtilXml

# AWS S3
ACCESS_KEY='AKIARUFQOA2GQD7UITUT'
SECRET_KEY='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'
REGION_NAME='sa-east-1'
BUCKET_NAME='cbx-plataforma'
LOCAL_FOLDER='src/scripts/aws'
LOCAL_FOLDER_EXTRACT='src/scripts/aws/xml'
S3_FOLDER = 'temp/'

def awsSign():
    return boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def download_folder_from_s3():
    # Cria uma sessão do cliente S3
    s3 = awsSign()

    # Cria o diretório local se não existir
    if not os.path.exists(LOCAL_FOLDER):
        os.makedirs(LOCAL_FOLDER)

    # Lista os arquivos no S3 na pasta especificada
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_FOLDER)

    # Verifica se há arquivos
    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            # Verifica se o objeto é um arquivo (não uma pasta)
            if file_key.endswith('/'):
                continue  # Ignora pastas

            local_file_path = os.path.join(LOCAL_FOLDER, os.path.basename(file_key))
            try:
                # Faz o download do arquivo
                s3.download_file(BUCKET_NAME, file_key, local_file_path)
                print(f'Arquivo {file_key} baixado')
            except Exception as e:
                print(f'Erro ao baixar {file_key}: {e}')
    else:
        print(f'Nenhum arquivo encontrado em {S3_FOLDER}')
    print(f'Download Complete!')

def extract_files():
    ff = UtilFile()
    ff.DEFAULT_DIR = 'src/scripts/aws/xml'
    ff.transfer_files_with_extraction(LOCAL_FOLDER)
    print(f'Extraction Complete!')
    
def save_xml_database():   
    xx = UtilXml()    
    xx.save_xmls(LOCAL_FOLDER_EXTRACT, 1)
    print(f'Saving Complete!')

if __name__ == "__main__":
    #download_folder_from_s3()
    #extract_files()
    save_xml_database()
