import math
import re
import pandas as pd 
import json
import psycopg2

from sqlalchemy import create_engine
from datetime import datetime

# Update IEs 
# 1 - cpf_cnpj remove characters '.0' at the final of the value
# 2 - cpf_cnpj field need to be with zeros on the left, if need it
# 3 - remove all character from the cpf_cnpj, like . or - or /
# 4 - read the ies from the file and update client of it to CBX
# 5 - some cnpj are on the cpf field, need to swap them

def format_person_doc(doc: str):
    # ex.: "13577891815.0" - remove .0
    if doc.endswith(".0"):
        doc = doc[:-2] 
    # remove dots and hyphens
    new_doc = ''.join(filter(str.isdigit, doc)) #re.sub(r'\..*', '', str(doc)) if pd.notna(doc) and str(doc).strip() != '' else doc
    return new_doc

def need_update_doc(doc: str):
    special_characters = ['.', '/', '-']
    return doc.endswith(".0") or any(char in doc for char in special_characters)

def has_11(doc: str):
    doc = format_person_doc(doc)
    return len(doc) == 11

def has_14(doc: str):
    doc = format_person_doc(doc)
    return len(doc) == 14

def format_zero_left(doc: str):
    doc = format_person_doc(doc)
    if len(doc) < 11:        
        return doc.zfill(11)
    elif len(doc) > 11 and len(doc) < 14:
        return doc.zfill(14)
    return doc
        
def verify_cpf_cnpj(doc: str):
    doc = format_zero_left(doc) 
    if len(doc) <= 11:
        return "CPF"
    elif len(doc) > 11: #and len(doc) <= 14:
        return "CNPJ"
    #else:
    #    return "Invalid"
       
def connect_to_db():
    # conn = psycopg2.connect(
    #     host="localhost",
    #     database="cbx_dev",
    #     user="postgres",
    #     password="local123"
    # )
    conn = psycopg2.connect(
        host="plataforma.cfjbmj8sxs2z.sa-east-1.rds.amazonaws.com",
        database="cbx_prd",
        user="postgres",
        password="84iuPbpQnCF5vze"
    )    
    return conn

def get_total_rows(cur):
    cur.execute("SELECT COUNT(*) FROM cbx.ie")
    return cur.fetchone()[0]

def process_chunk(offset, chunk_size):
    conn = connect_to_db()
    cur = conn.cursor()
    
    query = f"""
        SELECT id, ie_value, cpf, cnpj FROM cbx.ie ORDER BY id LIMIT {chunk_size} OFFSET {offset}
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    data = []
    for row in rows:
        id = row[0]
        ie_value = row[1]
        cpf = row[2]
        cnpj = row[3]
        
        data.append({
            'id': id,
            'ie_value': ie_value,
            'cpf': cpf,
            'cnpj': cnpj
        })
            
    df = pd.DataFrame(data)
    cur.close()
    conn.close()
    return df

def update_cpf_cnpj(conn):
    try:
        cur = conn.cursor()
        #engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
                
        total_rows = get_total_rows(cur)
        chunk_size = 500
        offsets = range(0, total_rows, chunk_size)
                
        for offset in offsets:
            df = process_chunk(offset, chunk_size)
            for i in range(0, len(df), 1):
                chunk = df[i:i+1]
                id = chunk['id'].values[0]
                ie_value = chunk['ie_value'].values[0]
                cpf = chunk['cpf'].values[0]
                cnpj = chunk['cnpj'].values[0]
                try:
                    commit = False
                    if cpf:
                        if verify_cpf_cnpj(cpf) == 'CNPJ':
                            if need_update_doc(cpf) or not has_14(cpf):
                                cpf = format_zero_left(cpf)
                                update_statement = f" update cbx.ie set cpf='', cnpj='{cpf}' where id = {id} "
                                cur.execute(update_statement)
                                commit = True
                        else:
                            if need_update_doc(cpf) or not has_11(cpf):
                                cpf = format_zero_left(cpf)
                                update_statement = f" update cbx.ie set cpf='{cpf}' where id = {id} "
                                cur.execute(update_statement)
                                commit = True
                    if cnpj:
                        if verify_cpf_cnpj(cnpj) == 'CPF':
                            if need_update_doc(cnpj) or not has_11(cnpj):
                                cnpj = format_zero_left(cnpj)
                                update_statement = f" update cbx.ie set cnpj='', cpf='{cnpj}' where id = {id} "
                                cur.execute(update_statement)
                                commit = True
                        else:
                            if need_update_doc(cnpj) or not has_14(cnpj):
                                cnpj = format_zero_left(cnpj)
                                update_statement = f" update cbx.ie set cnpj='{cnpj}' where id = {id} "
                                cur.execute(update_statement)
                                commit = True
                        
                    if commit:
                        conn.commit()
                except Exception as ex:
                    conn.rollback()
                    print(f'error: {ie_value} - {ex.args}')
                
        cur.close()
        conn.close()
                
        print('---------------------')
        print('IEs atualizadas com sucesso!')
        print('---------------------')
    
    except Exception as ex:            
        print(f'erro: {ex}')
        return
    
def update_client(conn):
    cur = conn.cursor()
    
    with pd.ExcelFile('src/scripts/ie/ie_de_fs_para_cbx.xlsx', engine='openpyxl') as xls:
        df = pd.read_excel(xls, dtype={'ie_value': str})
        for i in range(0, len(df), 1):
            chunk = df[i:i+1]
            ie_value = chunk['ie_value'].values[0]
            try:
                # from 1-FS to 2-CBX
                new_client = json.dumps([{"id_client": 2}])
                update_statement = f" update cbx.ie set clients='{new_client}' where ie_value = '{ie_value}' "
                cur.execute(update_statement)                    
                conn.commit()
            except Exception as ex:
                conn.rollback()
                print(f'error: {ie_value} - {ex.args}')
            
    cur.close()
    conn.close()                
            
    print('---------------------')
    print('Clientes da IE atualizadas com sucesso para CBX!')
    print('---------------------')
        
    
def main():   
    conn = connect_to_db()
    update_cpf_cnpj(conn)
    conn = connect_to_db()
    update_client(conn)
    
if __name__ == "__main__":
    main()    