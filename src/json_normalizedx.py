from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import pandas as pd
import json
from pandas import json_normalize
from sqlalchemy import create_engine

def connect_to_db():
    conn = psycopg2.connect(
        host="localhost",
        database="cbx_dev",
        user="postgres",
        password="local123"
    )
    return conn

def get_total_rows(cur):
    cur.execute("SELECT COUNT(*) FROM cbx.nf")
    return cur.fetchone()[0]

# Function to extract and normalize the 'det' array from the JSON data
# def extract_and_normalize(id, json_data):
#     if 'nfeProc' not in json_data or 'NFe' not in json_data['nfeProc'] or 'infNFe' not in json_data['nfeProc']['NFe']:
#         return pd.DataFrame()  # Return an empty DataFrame if 'nfeProc' node doesn't exist
        
#     det_data = json_normalize(json_data['nfeProc']['NFe']['infNFe']['det'])
#     ide_data = json_normalize(json_data['nfeProc']['NFe']['infNFe']['ide'])
#     ide_data = pd.concat([ide_data] * len(det_data)).reset_index(drop=True)
#     result_df = pd.concat([det_data, ide_data], axis=1)
    
#     # Add the ID column to the result DataFrame
#     result_df['id'] = id
#     return result_df

# Function to process data in chunks
def process_chunk(offset, chunk_size):
    conn = connect_to_db()
    cur = conn.cursor()
    
    query = f"""
        SELECT content_json, id,  
        ie_emissor, ie_destinatario, cnpj_cpf_emissor, cnpj_cpf_destinatario, 
        razao_social_emissor, razao_social_destinatario, client_id
        FROM cbx.nf ORDER BY id LIMIT {chunk_size} OFFSET {offset}
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    data = []
    for row in rows:
        json_data = row[0]
        id_value = row[1]        
        ie_emissor = row[2]
        ie_dest = row[3]
        cpf_emissor = row[4]
        cpf_dest = row[5]
        razao_emissor = row[6]
        razao_dest = row[7]
        client_id = row[8]

        if 'nfeProc' in json_data and 'NFe' in json_data['nfeProc'] and 'infNFe' in json_data['nfeProc']['NFe']:
            infNFe = json_data['nfeProc']['NFe']['infNFe']
            
            if 'det' in infNFe:   
                ide = infNFe['ide']
                prod_list = infNFe['det']
                
                if not isinstance(prod_list, list):
                    prod = prod_list['prod']                   
                    
                    flat_det = {
                        'id_nf': id_value, #integer,
                        'nro_nota': ide['nNF'], #integer,
                        'tipo_nota': ide['tpNF'], #integer,
                        'data_emissao': ide['dhEmi'], #timestamp(0) without time zone,	
                        'ie_emissor': ie_emissor, #integer,
                        'cnpj_cpf_emissor': cpf_emissor, #varchar(20),
                        'razao_social_emissor': razao_emissor, #varchar(250),
                        'ie_destinatario': ie_dest, #integer,
                        'cnpj_cpf_destinatario': cpf_dest, #varchar(20),
                        'razao_social_destinatario': razao_dest, #varchar(250),
                        'cfop': prod['CFOP'], #varchar(10),
                        'ncm': prod['NCM'], #varchar(100),
                        'nome_produto': prod['xProd'], #varchar(200),
                        'quantidade': prod['qCom'], #numeric(10, 2),
                        'unidade_medida': prod['uCom'], #varchar(10)
                        'client_id': client_id
                    }                    
                    data.append(flat_det)
                else:
                    for item in prod_list:
                        prod = item.get('prod', {})
                        
                        flat_det = {
                            'id_nf': id_value, #integer,
                            'nro_nota': ide['nNF'], #integer,
                            'tipo_nota': ide['tpNF'], #integer,
                            'data_emissao': ide['dhEmi'], #timestamp(0) without time zone,	
                            'ie_emissor': ie_emissor, #integer,
                            'cnpj_cpf_emissor': cpf_emissor, #varchar(20),
                            'razao_social_emissor': razao_emissor, #varchar(250),
                            'ie_destinatario': ie_dest, #integer,
                            'cnpj_cpf_destinatario': cpf_dest, #varchar(20),
                            'razao_social_destinatario': razao_dest, #varchar(250),
                            'cfop': prod['CFOP'], #varchar(10),
                            'ncm': prod['NCM'], #varchar(100),
                            'nome_produto': prod['xProd'], #varchar(200),
                            'quantidade': prod['qCom'], #numeric(10, 2),
                            'unidade_medida': prod['uCom'], #varchar(10)
                            'client_id': client_id
                        }                    
                        data.append(flat_det)
    
    df = pd.DataFrame(data)
    cur.close()
    conn.close()
    return df

def insert_into_db(engine, df):
    df.to_sql('nf_view', engine, schema='cbx', if_exists='append', index=False)

def main():
    conn = connect_to_db()
    cur = conn.cursor()
    
    conn_str = "postgresql://postgres:local123@localhost:5432/cbx_dev"
    engine = create_engine(conn_str)

    total_rows = get_total_rows(cur)
    chunk_size = 1000
    cur.close()
    conn.close()

    offsets = range(0, total_rows, chunk_size)
    
    #xx = process_chunk(0, chunk_size)
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_chunk, offset, chunk_size) for offset in offsets]

        for future in as_completed(futures):
            df = future.result()
            if df is not None and not df.empty:
                try:
                    insert_into_db(engine, df)
                except Exception as ex:
                    print(f'Error inserting chunk: idnf: {df["id_nf"]} nro: {df["nro_nota"]} erro: {ex.args}')
        
        print('---------------------')
        print('NFs inseridas na VIEW')
        print('---------------------')
                

if __name__ == "__main__":
    main()