import os
import pandas as pd 
import json
import chardet

from sqlalchemy import create_engine
from datetime import datetime
from util_database import *
from util_format import *
    
def get_de_paras():
    dir = 'src/scripts/de_para_PB'
            
    try:
        chunksize = 5000 
        dfs = []
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    try:                    
                        # Detect encoding
                        # with open(file_path, 'rb') as f:
                        #     result = chardet.detect(f.read())
                        # print(f"Detected encoding: {result['encoding']}")
                        
                        dtype_dict = {
                            'cod_imovel': str,
                            'parcela_co': str,
                            'codigo_imo': str,
                            'nome_area': str 
                        }
                                                                        
                        csv_data = pd.read_csv(file_path, chunksize=chunksize, dtype=dtype_dict, encoding='MacRoman')
                        for chunk in csv_data:
                            # area = acima de 10ha, coluna x
                            # % inter = acima de 15%, coluna y
                            filtered_df = chunk[(chunk['area_inter'] > 10) & (chunk['%intersecc'] > 0.15)]
                            #filtered_df = chunk[(chunk['cod_imovel'] == 'PE-2607653-10DFA7615B414E3193E1BDC6694288D5')]
                            if not filtered_df.empty:
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
            dfx['clients'] = json.dumps([{"id_client": 1}]) # 1 - FS
            dfx['sources'] = json.dumps([{"id_source": 4}]) # 4 - Sigef
                        
            columns_to_keep = [
                'nr_car', 'codigo_parcela', 'codigo_imovel_parcela', 'nome_parcela',
                'created_at', 'updated_at', 'created_by', 'updated_by', 'clients', 'sources']
            df_subset = dfx[columns_to_keep]        
            
            return df_subset

    except Exception as ex:
        raise f'Error traversing directory: {ex}'
              
def main():
    try:
        df  = get_de_paras()
    except Exception as ex:            
        print(f'erro: {ex}')
        return
    
    conn = connect_to_db(prod=True)
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
    cur = conn.cursor()
        
    # Drop duplicates based on nr_car and codigo_parcela
    filtered_df = df.drop_duplicates(subset=['nr_car', 'codigo_parcela'])
    cur.execute(f"SELECT nr_car, codigo_parcela FROM cbx.sigef")            
    sigefs = cur.fetchall()
    if sigefs:
        sigefs_df = pd.DataFrame(sigefs, columns=['nr_car', 'codigo_parcela'])
        # Apply filter to exclude rows where nr_car and codigo_parcela match
        filtered_df = filtered_df.loc[~filtered_df[['nr_car', 'codigo_parcela']].apply(tuple, axis=1).isin(sigefs_df.apply(tuple, axis=1))]
    
    chunksize = 5000      
    for i in range(0, len(filtered_df), chunksize):
        try:            
            chunk = filtered_df[i:i+chunksize]            
            insert_into_db(engine=engine, df=chunk, table='sigef', if_exis='append')
        except Exception as ex:
            print(f'erro: {ex.args}')
         
    try:
        # ADICIONAR: car que estao na tabela sigef mas nao estao na tabela car_input
        # insert_statement = """
        #     insert into cbx.car_input (nr_car, cpf_cnpj, nome_proprietario, status, 
        #     clients, 
        #     sources, 
        #     created_at, updated_at, created_by, updated_by)
        #     select distinct 
        #         sg.nr_car, sg.cpf_cnpj , '', true, 
        #         COALESCE(ci.clients::jsonb, '[]'::jsonb) || '[{"id_client": 1}]'::jsonb, 
        #         COALESCE(ci.sources::jsonb, '[]'::jsonb) || '[{"id_source": 4}]'::jsonb,
        #         current_timestamp, current_timestamp, 139, 139
        #     from cbx.sigef sg
        #     left join cbx.car_input ci on sg.nr_car = ci.nr_car 
        #     --and sg.cpf_cnpj = ci.cpf_cnpj
        #     where ci.nr_car is null    
        # """
        # cur.execute(insert_statement)
        
        # ATUALIZAR: car que estao na duas tabelas (car_input, sigef)
        update_statement = """
            update cbx.car_input as cii set
                sources = COALESCE(cii.sources::jsonb, '[]'::jsonb) || '[{"id_source": 4}]'::jsonb
            from cbx.sigef as sg
            where cii.nr_car = sg.nr_car
            and cii.id not in
            (
                SELECT filtered.id
                FROM (
                    SELECT ci.id, jsonb_array_elements(ci.sources::jsonb) AS elem
                    FROM cbx.car_input ci
                ) AS filtered	   	
                WHERE filtered.elem->>'id_source' = '4'       
            )        
        """
        cur.execute(update_statement)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e    
    
    cur.close()
    engine.dispose()
    conn.close()        
    
    print('---------------------')
    print(f'Upload SIGEF ok! - {conn.dsn}')
    print('---------------------')

if __name__ == "__main__":
    main()
    