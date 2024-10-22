import os
from util_database import connect_to_db
from util_file import UtilFile
from util_xml import UtilXml

FOLDER_SOURCE = 'src/scripts/aws/xml'
FOLDER_TARGET = 'src/scripts/xml_synchronize'
PROD = True

# Hit all key_nfs that exist in cbx.nf_view and does not exist in cbx.nf
# If nf exist in nf and not in nfview, just run the script ~/CBX/cbx-nf-load

# Function to matches the nfs
def search_orphans_nfs():
    conn = connect_to_db(prod=PROD)
    cur = conn.cursor()
    
    query = """
        SELECT distinct nfv.key_nf
        FROM cbx.nf_view nfv
        LEFT JOIN cbx.nf nf ON nfv.key_nf = nf.key_nf
        WHERE nf.key_nf IS null
        """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    keys = [row[0] for row in rows]
    return keys        

# Function to search for text in files and return matching file names
def find_files_with_keys(folder_path, keys_arr):
    matching_files = []
    
    utilFile = UtilFile()
    files = utilFile.read_all_files(folder_path)
    for file in files:
        try:
            #file_path = os.path.join(folder_path, file.name)
        
            if os.path.isfile(file):
                # Open the file and search for the specific text
                with open(file, 'r', encoding='utf-8') as filex:
                    file_content = filex.read()                    
                    # Check if any of the strings in keys_arr are in the file
                    if any(key in file_content for key in keys_arr):
                        # full path
                        matching_files.append(file)
        except Exception as e:
            print(f"Could not read {file}: {e}")
                
    return matching_files

def save_xml_database():   
    xx = UtilXml()    
    erros, total_sucesso, total_saved, count_errors = xx.save_xmls('', 1)
    print('--------------------------------------')
    print(f'Saving Complete! Total: {total_saved}')
    print('--------------------------------------')


keys = search_orphans_nfs()
keys_set = set(keys)
print("Total orphan nfview: "+ str(len(keys_set)))

files_found = find_files_with_keys(FOLDER_SOURCE, keys_set)

print("Total file orphan nfview found: "+ str(len(files_found)))

if files_found:
    print("Files containing any of the specified texts:")
    for file in files_found:
        print(file)
else:
    print("No files found containing the specified texts.")
