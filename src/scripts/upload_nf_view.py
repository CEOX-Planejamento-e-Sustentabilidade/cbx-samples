import pandas as pd
from sqlalchemy import create_engine
from util_database import *
from util_format import *

PROD = False

def get_total_rows_to_chunky(cur):
    cur.execute("""
                SELECT COUNT(*) 
                FROM cbx.nf nf
                LEFT JOIN cbx.nf_view nfv ON nf.key_nf = nfv.key_nf
                WHERE nfv.key_nf IS NULL
                """)
    return cur.fetchone()[0]

# Function to process data in chunks
def process_chunk(offset, chunk_size):
    conn = connect_to_db(prod=PROD)
    cur = conn.cursor()
    
    query = f"""    
        SELECT nf.content_json, nf.key_nf,
        nf.ie_emissor, nf.ie_destinatario, nf.cnpj_cpf_emissor, nf.cnpj_cpf_destinatario,
        nf.razao_social_emissor, nf.razao_social_destinatario, nf.client_id
        FROM cbx.nf nf
        LEFT JOIN cbx.nf_view nfv ON nf.key_nf = nfv.key_nf
        WHERE nfv.key_nf IS NULL
        ORDER BY nf.id LIMIT {chunk_size} OFFSET {offset}        
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    data = []
    for row in rows:
        json_data = row[0]
        key_nf = row[1]        
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
                        'key_nf': key_nf,
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
                            'key_nf': key_nf,
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

def main():
    conn = connect_to_db(prod=PROD)
    cur = conn.cursor()
    
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)

    total_rows = get_total_rows_to_chunky(cur)
    chunk_size = 10000

    offsets = range(0, total_rows, chunk_size)
       
    for offset in offsets:
        df = process_chunk(offset, chunk_size)
        try:
            insert_into_db(engine=engine, df=df, table='nf_view')
        except Exception as ex:
            print(f'Error inserting chunk: key-nf: {df["key_nf"]} nro: {df["nro_nota"]} erro: {ex.args}')
    print('---------------------')
    print('NFs inseridas na VIEW')
    print('---------------------')
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()