import os
import uuid

from util_nf import UtilNf
from util_aws import UtilAws
from util_file import UtilFile
from util_xml import UtilXml

PRODx = False
LOCAL_FOLDER='src/scripts/zip_matheus'
LOCAL_FOLDER_EXTRACT='src/scripts/zip_matheus/xml'

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
    
    erros, total_files, total_sucesso, total_saved, count_errors = xx.save_xmls(LOCAL_FOLDER_EXTRACT, 1)
    msg = f"Total de XMLs: {total_files} - Total Processados: {total_saved} - Total Erros: {count_errors}"
    print('--------------------------------------')
    print(f'Saving Complete!')
    print(f'{msg}')
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

if __name__ == "__main__":
    PRODx = False
    create_local_folders()
    #download_folder_from_s3
    paths = extract_files()
    save_xml_database()    
    processar_nfs_insumos_from_paths(paths)    
    
