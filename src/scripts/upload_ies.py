import math
import re
import pandas as pd 
import json
import psycopg2

from sqlalchemy import create_engine
from datetime import datetime

def format_person_doc(doc: str):
    # remove dots and hyphens
    new_doc = ''.join(filter(str.isdigit, str(doc))) #re.sub(r'\..*', '', str(doc)) if pd.notna(doc) and str(doc).strip() != '' else doc
    return new_doc
        
def verify_cpf_cnpj(doc: str):
    #number = ''.join(filter(str.isdigit, number))  # Remove non-digit characters    
    doc = format_person_doc(doc)  # Remove non-digit characters    
    if len(doc) == 11:
        return "CPF"
    elif len(doc) == 14:
        return "CNPJ"
    else:
        return "Invalid"
    
def get_ies():
    with pd.ExcelFile('src/scripts/ies_cadastrar.xlsx', engine='openpyxl') as xls:
        df = pd.read_excel(xls, dtype={'ie': str})
        
        #EXCEL: id_grupo|produtor|cpf_cnpj|ie|situacao_ie|municipio|nome_fantasia|logradouro|no|complemento|cep|bairro|data_inicio_atividade|data_situacao_cadastral|nome_empresarial|descricao_atividade|
        #BD: id|ie_value|cpf|cnpj|status|ie_status_text|properties|cpf_main|group_cbx|updated_at|clients|cbx_cod|s3_url|created_at|created_by|updated_by|sources|
        
        columns_to_be = {'id_grupo': 'cbx_cod',
                         #'produtor': 'group_cbx',
                         #'cpf_cnpj': 'cpf_main',
                         'ie': 'ie_value',
                         'situacao_ie': 'ie_status_text'}
        
        df.rename(columns=columns_to_be, inplace=True)
               
        df['cpf'] = df.apply(lambda row: format_person_doc(row.cpf_cnpj) if verify_cpf_cnpj(row.cpf_cnpj) == 'CPF' else '', axis=1)
            
        df['cnpj'] = df.apply(lambda row: format_person_doc(row.cpf_cnpj) if verify_cpf_cnpj(row.cpf_cnpj) == 'CNPJ' else '', axis=1)
        
        df['status'] = True
        
        # sources: 1 - Manual | 2 - Infosimples
        df['sources'] = df.apply(lambda row: json.dumps([{"id_source": 1}]), axis=1)
        
        # clients: 1 - FS
        df['clients'] = df.apply(lambda row: json.dumps([{"id_client": 1}]), axis=1)
        
        df['created_at'] = datetime.now().isoformat()
        df['updated_at'] = datetime.now().isoformat()
        df['created_by'] = 139
        df['updated_by'] = 139
        
        # create properties
        df['properties'] = df.apply(lambda row: json.dumps({
            'uf': 'MT',
            'COD': '0',
            'cep': str(row.cep).replace('\n', '').replace('\r', ''), #f'{row.cep}',
            'cpf': f'{row.cpf}',
            'mei': '',
            'cnpj': f'{row.cnpj}',
            'nome': f'{row.produtor}',
            'email': '',
            'bairro': f'{row.bairro}',
            'numero': f'{row.no}',
            'contador': '',
            'fantasia': f'{row.nome_fantasia}',
            'natureza': '',
            'telefone': '',
            'municipio': str(row.municipio).replace('\n', '').replace("'", "''"),#f'{row.municipio}',
            'logradouro': str(row.logradouro).replace('\n', '').replace("'", "''"), # f'{row.logradouro}',
            'complemento': '' if isinstance(row.complemento, float) and math.isnan(row.complemento) else str(row.complemento).replace('\n', ''),
            'arquivo_trello': '',
            'nomeempresarial': str(row.nome_empresarial).replace('\n', ''), #f'{row.nome_empresarial}',
            'simplenaciponal': '',
            'demaisatividades': '',
            'formadetributacao': '',
            'situacaocadastral': '',
            'atividadeprincipal': str(row.descricao_atividade).replace('\n', ''), #f'{ row.descricao_atividade}',
            'data_inicio_atividade': f'{row.data_inicio_atividade}',
            'datasituacaocadastral': f'{row.data_situacao_cadastral}',
            'motivosituacaocadastral': ''            
            }), axis=1)       
        
        columns_to_keep = ['cbx_cod', 'cpf', 'cnpj', #'group_cbx', 'cpf_main',
                           'ie_value', 'ie_status_text', 'status', 'sources', 'clients',
                           'created_at', 'updated_at', 'created_by', 'updated_by', 'properties']
        df_subset = df[columns_to_keep]        
                
        return df_subset
    
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


def get_update_sql(field_name, field_value, ie):    
    # Define the SQL statement as a string
    sql_statement = """
        UPDATE cbx.ie SET
          properties = jsonb_set(properties, '{{"{field_name}"}}', '"{field_value}"')
        WHERE ie_value = '{ie_value}';
    """

    # Format the SQL statement with the values
    formatted_sql = sql_statement.format(field_name=field_name, field_value=field_value, ie_value=ie)
    return formatted_sql

def main():   
    try:
        df  = get_ies()
    except Exception as ex:            
        print(f'erro: {ex}')
        return
    
    conn = connect_to_db()    
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)    
        
    chunksize = 1
    
    for i in range(0, len(df), chunksize):
        try:            
            chunk = df[i:i+chunksize]
            ie = chunk['ie_value'].values[0]
            
            cur.execute(f"SELECT COUNT(*) FROM cbx.ie where ie_value = '{ie}'")
            total = cur.fetchone()[0]
            if total == 0:
                chunk.to_sql('ie', engine, schema='cbx', if_exists='append', index=False)
            else:
                try:
                    properties_json = json.loads(chunk['properties'].values[0])
                    
                    cur.execute(get_update_sql('uf', properties_json['uf'], ie))
                    cur.execute(get_update_sql('cep', properties_json['cep'], ie))
                    cur.execute(get_update_sql('cpf', properties_json['cpf'], ie))
                    cur.execute(get_update_sql('cnpj', properties_json['cnpj'], ie))
                    cur.execute(get_update_sql('nome', properties_json['nome'], ie))
                    cur.execute(get_update_sql('bairro', properties_json['bairro'], ie))
                    cur.execute(get_update_sql('numero', properties_json['numero'], ie))
                    cur.execute(get_update_sql('fantasia', properties_json['fantasia'], ie))
                    cur.execute(get_update_sql('municipio', properties_json['municipio'], ie))
                    cur.execute(get_update_sql('logradouro', properties_json['logradouro'], ie))
                    cur.execute(get_update_sql('complemento', properties_json['complemento'], ie))
                    cur.execute(get_update_sql('nomeempresarial', properties_json['nomeempresarial'], ie))
                    cur.execute(get_update_sql('atividadeprincipal', properties_json['atividadeprincipal'], ie))
                    cur.execute(get_update_sql('data_inicio_atividade', properties_json['data_inicio_atividade'], ie))
                    cur.execute(get_update_sql('datasituacaocadastral', properties_json['datasituacaocadastral'], ie))
                                                        
                    update_statement = f"""
                        update cbx.ie set 
                        updated_by=139,
                        updated_at=now(),
                        clients='{chunk['clients'].values[0]}',
                        sources='{chunk['sources'].values[0]}',
                        cbx_cod={chunk['cbx_cod'].values[0]},
                        ie_status_text='{chunk['ie_status_text'].values[0]}',
                        status=true,
                        cnpj='{properties_json['cnpj']}',
                        cpf='{properties_json['cpf']}'
                        where ie_value = {ie}
                    """
                    cur.execute(update_statement)
                    
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e                        
        except Exception as ex:            
            print(f'ie: {ie} - erro: {ex.args}')

    engine.dispose()
    
    print('---------------------')
    print('Upload IE ok!')
    print('---------------------')

if __name__ == "__main__":
    main()
    