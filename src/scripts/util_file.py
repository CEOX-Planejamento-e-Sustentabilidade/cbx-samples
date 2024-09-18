import json
import os
import shutil
import uuid
import zipfile

from io import TextIOWrapper
from pathlib import Path
from os import makedirs
from os.path import join
from werkzeug.utils import secure_filename

class UtilFile:
    ALLOWED_EXTENSIONS = {'txt', 'csv'}
    ALLOWED_ZIP = {'zip'}
    DEFAULT_DIR = 'json'
    
    def __init__(self):
        self = self

    def save_zip(self, file, filename):
        file.save(filename)

    def get_path_dir_extract(self, file_id):
        return join(self.DEFAULT_DIR, file_id)

    def create_dir_extract(self, file_id):
        filepath_to_extract = self.get_path_dir_extract(file_id)
        #shutil.rmtree(filepath_to_extract)
        makedirs(filepath_to_extract)
        return filepath_to_extract

    def setup_zip_env(self, file):
        file_id = str(uuid.uuid1())
        name_file = f'{file.stem}-{file_id}'
        dir_path = self.create_dir_extract(name_file)
        return dir_path        

    def transfer_files_with_extraction(self, local_folder):
        files = self.read_all_files(local_folder)
        for file in files:
            try:
                file_name = file.name
                if file_name.endswith('.xml'):
                    source_file_path = os.path.join(local_folder, file_name)                
                    self.move_file(source_file_path, self.DEFAULT_DIR)
                elif file_name.endswith('.zip'):                
                    path_to_extract = self.setup_zip_env(file)
                    path_zip = os.path.join(local_folder, file_name)
                    files = self.open_zip(path_zip, path_to_extract, '')
            except Exception as ex:
                print(f'{str(ex)}')

    def read_all_files(self, path):
        folder = Path(path).rglob('*')
        # Filter to only include files
        files = [x for x in folder if x.is_file()]
        return files
    
    def move_file(self, source_file_path, destination_folder):
        # Convert paths to Path objects
        source = Path(source_file_path)
        destination = Path(destination_folder)
        # Ensure the destination folder exists
        destination.mkdir(parents=True, exist_ok=True)
        # Move the file
        shutil.move(str(source), str(destination / source.name))
        print(f'Moved {source} to {destination / source.name}')    
                
    def open_zip(self, zip_path, path_to_extract, password):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            if password != '':
                zip_ref.setpassword (password.encode())
            zip_ref.extractall(path_to_extract)
        
    def open_file(self, file, is_json = True):
        with open(file, 'r', encoding='utf-8-sig') as json_file:
            content_json = json_file.read()
            if not content_json:
                return False, None
            if is_json:
                content_json = json.loads(content_json)
            return True, content_json
    
    def allowed_file(self, filename, allowed_extensions):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

    def read_file_split(self, file):
        content = self.read_file(file)
        separators = [',', ';', '|', '\t', '\n']        
        for separator in separators:
            if separator in content:
                splited = content.strip().split(separator)
                return splited
        return None
        
    def read_file(self, file):
        text_stream = TextIOWrapper(file, encoding='utf-8-sig')
        content = text_stream.read()   
        return content