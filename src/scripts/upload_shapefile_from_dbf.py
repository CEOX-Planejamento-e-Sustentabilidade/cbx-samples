import os
import pandas as pd 
import json

from dbfread import DBF
from sqlalchemy import create_engine
from datetime import datetime
from util_database import *
from util_format import *
    
def get_dbfs():
    dbf_dir = 'src/scripts/dbf'
            
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
                                df = pd.concat([df, pd.DataFrame(dbf_table.records)]) 
                        
                    except Exception as ex:
                        print(f'Error reading DBF file: {dbf_file_path}')
                        print(f'Exception: {ex}')
               
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
        df['cod_grupo'] = df['cod_grupo'].apply(lambda x: int(x) if x.strip() else None) #df.apply(lambda row: row.cod_grupo, axis=1)
        df['data_registro'] = df.apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x['data_registro']) and isinstance(x['data_registro'], str) else x['data_registro'], axis=1)
        df['created_at'] = datetime.now().isoformat()
        df['updated_at'] = datetime.now().isoformat()
        df['created_by'] = 139
        df['updated_by'] = 139      
        df['clients'] = df.apply(lambda row: json.dumps([{"id_client": 1}]), axis=1)  
        return df

    except Exception as ex:
        print(f'Error traversing directory: {ex}')
              
def main():
    try:
        df  = get_dbfs()
    except Exception as ex:            
        print(f'erro: {ex}')
        return
    
    conn = connect_to_db(prod=False)
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)    
        
    chunksize = 1
    
    for i in range(0, len(df), chunksize):
        try:            
            chunk = df[i:i+chunksize]
            insert_into_db(engine=engine, df=chunk, table='shapefile')
            #chunk.to_sql('shapefile', engine, schema='cbx', if_exists='append', index=False)
        except Exception as ex:            
            print(f'erro: {ex.args}')

    engine.dispose()
    
    print('---------------------')
    print('Upload SHAPEFILE ok!')
    print('---------------------')

if __name__ == "__main__":
    main()
    