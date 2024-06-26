import os
import pandas as pd 
import json

from sqlalchemy import create_engine
from datetime import datetime
from util_database import *
from util_format import *
    
def get_de_paras():
    dir = 'src/scripts/de_para'
            
    try:
        dfs = []
        df = pd.DataFrame()
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith('.xlsx'):
                    file_path = os.path.join(root, file)
                    try:
                        #encoding='utf-8-sig'
                        with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                            dtype_dict = {
                                'cod_imovel': str,
                                'parcela_co': str,
                                'codigo_imo': str,
                                'nome_area': str 
                            }
                            
                            df = pd.read_excel(xls, dtype=dtype_dict)
                            
                            # area = acima de 10ha, coluna x
                            # % inter = acima de 15%, coluna y
                            filtered_df = df[(df['area_inter'] > 10) & (df['%intersecc'] > 0.15)]
                                                        
                            dfs.append(filtered_df)
                    except Exception as ex:
                        print(f'Error reading DBF file: {file_path}')
                        print(f'Exception: {ex}')

        if dfs:
            dfx = pd.concat(dfs, ignore_index=True)
                        
            columns_to_be = {
                'cod_imovel': 'nr_car',
                'parcela_co': 'codigo_parcela',
                'codigo_imo': 'codigo_imovel_parcela',
                'nome_area': 'nome_parcela'
            }

            dfx.rename(columns=columns_to_be, inplace=True)
            dfx.columns = map(str.lower, dfx.columns)
            
            dfx['created_at'] = datetime.now().isoformat()
            dfx['updated_at'] = datetime.now().isoformat()
            dfx['created_by'] = 139
            dfx['updated_by'] = 139      
            dfx['clients'] = df.apply(lambda row: json.dumps([{"id_client": 1}]), axis=1)  
                        
            columns_to_keep = [
                'nr_car', 'codigo_parcela', 'codigo_imovel_parcela', 'nome_parcela',
                'created_at', 'updated_at', 'created_by', 'updated_by', 'clients']
            df_subset = dfx[columns_to_keep]        
            
            return df_subset

    except Exception as ex:
        print(f'Error traversing directory: {ex}')
              
def main():
    try:
        df  = get_de_paras()
    except Exception as ex:            
        print(f'erro: {ex}')
        return
    
    conn = connect_to_db(prod=True)
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)    
    cur = conn.cursor()
        
    chunksize = 1
    
    for i in range(0, len(df), chunksize):
        try:            
            chunk = df[i:i+chunksize]            
            nr_car = chunk['nr_car'].values[0]
            codigo_parcela = chunk['codigo_parcela'].values[0]            
            cur.execute(f"SELECT COUNT(*) FROM cbx.sigef where nr_car = '{nr_car}' and codigo_parcela = '{codigo_parcela}'")            
            total = cur.fetchone()[0]
            if total == 0:
                insert_into_db(engine=engine, df=chunk, table='sigef', if_exis='append')
        except Exception as ex:
            print(f'erro: {ex.args}')
    engine.dispose()
    
    print('---------------------')
    print('Upload SIGEF ok!')
    print('---------------------')

if __name__ == "__main__":
    main()
    