import os
import uuid

from util_nf import UtilNf
from util_aws import UtilAws
from util_file import UtilFile
from util_xml import UtilXml

PRODx = True
LOCAL_FOLDER='src/scripts/zip_fs'
LOCAL_FOLDER_EXTRACT='src/scripts/zip_fs/xml'

def create_local_folders():
    if not os.path.exists(LOCAL_FOLDER):
        os.makedirs(LOCAL_FOLDER)
    if not os.path.exists(LOCAL_FOLDER_EXTRACT):
        os.makedirs(LOCAL_FOLDER_EXTRACT)
        
def download_folder_from_s3():
    utilAws = UtilAws()
    utilAws.download_folder_from_s3(LOCAL_FOLDER)
   
def extract_files():
    ff = UtilFile()   
    ff.DEFAULT_DIR = LOCAL_FOLDER_EXTRACT
    #ff.transfer_files_with_extraction(LOCAL_FOLDER)
    extract_paths = ff.extract_zip(LOCAL_FOLDER)
    print('--------------------------------------')
    print(f'Extraction Complete!')
    print('--------------------------------------')
    return extract_paths
    
def save_xml_database():   
    xx = UtilXml()
    xx.PRODx = PRODx
    
    # 1 - FS
    # 15 - Paineiras
    
    # QUAL O CLIENTE?
    
    erros, total_sucesso, total_saved, count_errors = xx.save_xmls(LOCAL_FOLDER_EXTRACT, -1)
    print('--------------------------------------')
    print(f'Saving Complete! Total: {total_saved}')
    print('--------------------------------------')

def processar_nfs_insumos_from_paths(xml_paths: list):
    nf = UtilNf()
    nf.PRODx = PRODx

    for path_value in xml_paths:
        file_id = str(uuid.uuid1())
        
        parts = path_value.split('/')
        folder_name = parts[-1]
                
        status, content, total_sucesso, total_erros = nf.processar_nfs_insumos(path_value, f'{folder_name}-{file_id}')
        
        print('--------------------------------------')
        print(f'Excel gerado - {path_value}')
        print('--------------------------------------')
    
    
def processar_nfs_insumos():
    nf = UtilNf()
    nf.PRODx = PRODx
    
    # 2023 - \src\scripts\zip_lucas\xml\183d2f77-039a-4377-9141-0283e17a95d0\downloads
    # 2022 - \src\scripts\zip_lucas\xml\183d2f77-039a-4377-9141-0283e17a95d0\XMLs de entrada 2022
    # 2021 - \src\scripts\zip_lucas\xml\183d2f77-039a-4377-9141-0283e17a95d0\ENTRADA CANA 2021
    # 2020 - \src\scripts\zip_lucas\xml\183d2f77-039a-4377-9141-0283e17a95d0\XMLs de Entrada 2020
    
    paths = [
        {"id": 2020, "path": 'src/scripts/zip_lucas/xml/183d2f77-039a-4377-9141-0283e17a95d0/XMLs de Entrada 2020'},
        {"id": 2021, "path": 'src/scripts/zip_lucas/xml/183d2f77-039a-4377-9141-0283e17a95d0/ENTRADA CANA 2021'},
        {"id": 2022, "path": 'src/scripts/zip_lucas/xml/183d2f77-039a-4377-9141-0283e17a95d0/XMLs de entrada 2022'},
        {"id": 2023, "path": 'src/scripts/zip_lucas/xml/183d2f77-039a-4377-9141-0283e17a95d0/downloads'}
    ]    
    
    for item in paths:
        path_id = item['id']
        path_value = item['path']    
        file_id = str(uuid.uuid1())
        
        #ff = os.path.dirname(path_value)
        #dd = os.path.basename(path_value)
        
        file_name = f'{str(path_id)}-{file_id}'
                
        status, content, total_sucesso, total_erros = nf.processar_nfs_insumos(path_value, file_name)
        
        print('--------------------------------------')
        print(f'Excel gerado - {str(path_id)} {path_value}')
        print('--------------------------------------')
    

if __name__ == "__main__":
    #create_local_folders()
    #download_folder_from_s3
    paths = extract_files()
    #save_xml_database()    
    processar_nfs_insumos_from_paths(paths)
    #processar_nfs_insumos()
    
