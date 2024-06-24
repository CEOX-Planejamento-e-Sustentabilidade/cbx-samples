import math
import os
import re
import pandas as pd 
import json
import psycopg2
import dbf
from dbfread import DBF

from sqlalchemy import create_engine
from datetime import datetime

def connect_to_db():
    conn = psycopg2.connect(
        host="localhost",
        database="cbx_dev",
        user="postgres",
        password="local123"
    )
    # conn = psycopg2.connect(
    #     host="plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com",
    #     database="cbx_prd",
    #     user="postgres",
    #     password="84iuPbpQnCF5vze"
    # )    
    return conn

def format_person_doc(doc: str):
    # remove dots and hyphens
    new_doc = ''.join(filter(str.isdigit, str(doc)))
    return new_doc
        
def verify_cpf_cnpj(doc: str):
    doc = format_person_doc(doc)
    if len(doc) <= 11:
        return "CPF"
    elif len(doc) > 11:
        return "CNPJ"
    
def get_dbfs():
    # src/scripts/dbf
    dbf_dir = 'src/scripts/dbf'
    dbf_arr = []
            
    try:
        df = pd.DataFrame()
        for root, dirs, files in os.walk(dbf_dir):
            for file in files:
                if file.endswith('.dbf'):
                    dbf_file_path = os.path.join(root, file)
                    try:
                        with DBF(dbf_file_path, encoding='utf-8-sig') as dbf_table:
                            key = ['COD_PRODUT', 'CPF_CNPJ', 'ANO_SAFRA', 'COD_IMOVEL']
                            if len(dbf_table.field_names) == 15 and all(value in dbf_table.field_names for value in key):
                                #records = [record for record in dbf_table]
                                #dbf_arr.append(records)
                                df = pd.concat([df, pd.DataFrame(dbf_table.records)]) 
                        
                    except Exception as ex:
                        print(f'Error reading DBF file: {dbf_file_path}')
                        print(f'Exception: {ex}')
        #df = pd.DataFrame(dbf_arr)
               
        columns_to_be = {
            'COD_PRODUT': 'cod_grupo',
            'PLANT_USIN': 'planta',
            'NOM_PRODUT': 'nome_produtor',
            'COD_IMOVEL': 'nr_car',
            'CIDADE': 'nome_municipio',
            'DATA_REGIS': 'data_registro',
            'ELEGIBILID': 'elegibilidade',
            
            # 'COD_PRODUT': 'cod_grupo',
            # 'PLANT_USIN': 'planta',
            # 'NOM_PRODUT': 'nome_produtor',
            # 'CPF_CNPJ': 'cpf_cnpj',
            # 'ANO_SAFRA': 'ano_safra',
            # 'TIPO': 'tipo',
            # 'COD_IMOVEL': 'nr_car',
            # 'COD_MAPA': 'cod_mapa',
            # 'CIDADE': 'nome_municipio',
            # 'DATA_REGIS': 'data_registro',
            # 'SITUACAO': 'situacao',
            # 'AREA_TOTAL': 'area_total',
            # 'AREA_MAPA': 'area_mapa',
            # 'SUPRESSAO': 'supressao',
            # 'ELEGIBILID': 'elegibilidade'            
        }                
        df.rename(columns=columns_to_be, inplace=True)
        df.columns = map(str.lower, df.columns)
        df['cpf_cnpj'] = df.apply(lambda row: format_person_doc(row.cpf_cnpj), axis=1)
        df['cod_grupo'] = df.apply(lambda row: int(row.cod_grupo), axis=1)
        df['data_registro'] = df.apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x['data_registro']) and isinstance(x['data_registro'], str) else x['data_registro'], axis=1)
        df['created_at'] = datetime.now().isoformat()
        df['updated_at'] = datetime.now().isoformat()
        df['created_by'] = 139
        df['updated_by'] = 139        
        return df

    except Exception as ex:
        print(f'Error traversing directory: {ex}')
              
def main():
    try:
        df  = get_dbfs()
    except Exception as ex:            
        print(f'erro: {ex}')
        return
    
    conn = connect_to_db()    
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)    
        
    chunksize = 1
    
    for i in range(0, len(df), chunksize):
        try:            
            chunk = df[i:i+chunksize]
            chunk.to_sql('shapefile', engine, schema='cbx', if_exists='append', index=False)
        except Exception as ex:            
            print(f'erro: {ex.args}')

    engine.dispose()
    
    print('---------------------')
    print('Upload SHAPEFILE ok!')
    print('---------------------')

if __name__ == "__main__":
    main()
    