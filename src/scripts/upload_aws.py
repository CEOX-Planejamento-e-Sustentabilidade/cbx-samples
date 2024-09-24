import boto3

import os

from util_file import UtilFile
from util_xml import UtilXml

# AWS S3
ACCESS_KEY='AKIARUFQOA2GQD7UITUT'
SECRET_KEY='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'
REGION_NAME='sa-east-1'
BUCKET_NAME='cbx-plataforma'
LOCAL_FOLDER='src/scripts/aws/zip'
LOCAL_FOLDER_EXTRACT='src/scripts/aws/xml'
S3_FOLDER = 'temp/'
# LOCAL_FOLDER='src/scripts/aws/nfs/xml'
# LOCAL_FOLDER_EXTRACT='src/scripts/aws/nfs/xml'
# S3_FOLDER = 'nfs/xml/'

def awsSign():
    return boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def create_local_folders():
    if not os.path.exists(LOCAL_FOLDER):
        os.makedirs(LOCAL_FOLDER)
    if not os.path.exists(LOCAL_FOLDER_EXTRACT):
        os.makedirs(LOCAL_FOLDER_EXTRACT)

def download_folder_from_s3():
    # Cria uma sessão do cliente S3
    s3 = awsSign()
        
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_FOLDER):
        if 'Contents' in page:
            for obj in page['Contents']:
                file_key = obj['Key']
                if file_key.endswith('/'):
                    continue  # Ignora pastas
                
                local_file_path = os.path.join(LOCAL_FOLDER, os.path.basename(file_key))

                # Faça o download do arquivo
                try:
                    print(f'Baixando {file_key} para {local_file_path}...')
                    s3.download_file(BUCKET_NAME, file_key, local_file_path)
                except Exception as e:
                    print(f'Erro ao baixar {file_key}: {e}')
            else:
                print(f'Nenhum arquivo encontrado em {S3_FOLDER}')
    print('--------------------------------------')
    print(f'Download Complete!')
    print('--------------------------------------')
   
def extract_files():
    ff = UtilFile()
   
    ff.DEFAULT_DIR = LOCAL_FOLDER_EXTRACT
    ff.transfer_files_with_extraction(LOCAL_FOLDER)
    print('--------------------------------------')
    print(f'Extraction Complete!')
    print('--------------------------------------')
    
def save_xml_database():   
    xx = UtilXml()    
    erros, total_sucesso, total_saved, count_errors = xx.save_xmls(LOCAL_FOLDER_EXTRACT, 1)
    print('--------------------------------------')
    print(f'Saving Complete! Total: {total_saved}')
    print('--------------------------------------')

if __name__ == "__main__":
    #create_local_folders()
    #download_folder_from_s3()
    #extract_files()
    save_xml_database()
