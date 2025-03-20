import math
from pathlib import Path
import pandas as pd 
import json

from sqlalchemy import create_engine
from psycopg2.extras import NamedTupleCursor
from datetime import datetime
import os
from util_database import *
from util_format import *
   
def get_keys():    
    #file_path = 'src/scripts/sefaz/Rel Saída Jul.xls'
    #file_path = 'src/scripts/sefaz/Rel Saída Jul.xlsx'
    
    all_data = []
    
    # Find all Excel files
    excel_files = list(Path("src/scripts/sefaz").rglob("*.xls")) + list(Path("src/scripts/sefaz").rglob("*.xlsx"))
    
    for file_path in excel_files:
        file_extension = file_path.suffix.lower()    
                
        # Determine the correct engine based on file extension
        #file_extension = os.path.splitext(file_path)[1].lower()
        engine = "xlrd" if file_extension == ".xls" else "openpyxl"

        # Read the Excel file without assuming headers
        df = pd.read_excel(file_path, dtype=str, engine=engine, header=None)    
        # Strip spaces and lowercase everything in the first column
        df.iloc[:, 0] = df.iloc[:, 0].str.strip().str.lower()

        # Find the first row where 'DATA EMISSÃO' or 'DATA EMISSAO' appears
        header_row_index = df[df.iloc[:, 0].str.contains(r"data emissão|data emissao", na=False, case=False, regex=True)].index

        if header_row_index.empty:
            raise ValueError("⚠️ 'DATA EMISSÃO' not found in the first column!")

        header_row_index = header_row_index[0]  # Get the first match    

        # Set this row as the header and keep only relevant data
        df = df.iloc[header_row_index:].reset_index(drop=True)

        # Set the first row as column names
        df.columns = df.iloc[0].str.strip().str.lower()  # Normalize column names
        df = df[1:].reset_index(drop=True)  # Remove the first row from data

        # Find the correct column name for 'CHAVE DE ACESSO' or 'CHAVE ACESSO'
        chave_column = df.columns[df.columns.str.contains(r"chave de acesso|chave acesso", na=False, case=False, regex=True)]

        if chave_column.empty:
            raise ValueError("⚠️ 'CHAVE DE ACESSO' column not found in the table!")

        # Keep only the 'CHAVE DE ACESSO' column and rename it
        df = df[[chave_column[0]]].rename(columns={chave_column[0]: 'key_nf'})    
        all_data.append(df)
        
    if not all_data:
        raise ValueError("⚠️ No valid tables found in any Excel files!")

    return pd.concat(all_data, ignore_index=True)
    
def main():
    try:
        df = get_keys()
    except Exception as ex:            
        print(f'erro: {ex}')
        return
        
    for index, row in df.iterrows():
        print(row['key_nf'])

    print('---------------------')
    print('Read ok!')
    print('---------------------')

if __name__ == "__main__":
    main()
    