import os
import boto3

# AWS S3
ACCESS_KEY='AKIARUFQOA2GQD7UITUT'
SECRET_KEY='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'
REGION_NAME='sa-east-1'
BUCKET_NAME='cbx-plataforma'
S3_FOLDER = 'temp/'
# S3_FOLDER = 'nfs/xml/'

class UtilAws:
    def awsSign(self):
        return boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    def download_folder_from_s3(self, local_folder):
        # Cria uma sessão do cliente S3
        s3 = self.awsSign()
            
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_FOLDER):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_key = obj['Key']
                    if file_key.endswith('/'):
                        continue  # Ignora pastas
                    
                    local_file_path = os.path.join(local_folder, os.path.basename(file_key))

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
