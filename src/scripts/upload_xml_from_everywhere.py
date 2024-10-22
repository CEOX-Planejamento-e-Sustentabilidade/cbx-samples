import os

from util_aws import UtilAws
from util_file import UtilFile
from util_xml import UtilXml

#LOCAL_FOLDER='src/scripts/aws/zip'
#LOCAL_FOLDER_EXTRACT='src/scripts/aws/xml'
# LOCAL_FOLDER='src/scripts/aws/nfs/xml'
# LOCAL_FOLDER_EXTRACT='src/scripts/aws/nfs/xml'
LOCAL_FOLDER='src/scripts/zip_lucas'
LOCAL_FOLDER_EXTRACT='src/scripts/zip_lucas/xml'

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
    ff.extract_zip(LOCAL_FOLDER)
    print('--------------------------------------')
    print(f'Extraction Complete!')
    print('--------------------------------------')
    
def save_xml_database():   
    xx = UtilXml()    
    
    # 15 - Paineiras
    erros, total_sucesso, total_saved, count_errors = xx.save_xmls(LOCAL_FOLDER_EXTRACT, 15)
    print('--------------------------------------')
    print(f'Saving Complete! Total: {total_saved}')
    print('--------------------------------------')

if __name__ == "__main__":
    #create_local_folders()
    #download_folder_from_s3
    #extract_files()
    save_xml_database()
