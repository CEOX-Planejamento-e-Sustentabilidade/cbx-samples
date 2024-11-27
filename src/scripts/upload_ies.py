import math
import pandas as pd 
import json

from sqlalchemy import create_engine
from psycopg2.extras import NamedTupleCursor
from datetime import datetime
from util_database import *
from util_format import *
   
def get_ies():
    #path = 'src/scripts/ie/ies_cadastrar.xlsx'
    path = 'src/scripts/bd_ie/bd_ie_(03.2024).xlsx'
    
    with pd.ExcelFile(path, engine='openpyxl') as xls:
        
        dtype_dict = {
            'cpf': str,
            'cnpj': str,
            'cpf_cnpj': str,
            'cpf_principal': str
        }
        
        df = pd.read_excel(xls, dtype=dtype_dict)
        
        #EXCEL: id_grupo|produtor|cpf_cnpj|ie|situacao_ie|municipio|nome_fantasia|logradouro|no|complemento|cep|bairro|data_inicio_atividade|data_situacao_cadastral|nome_empresarial|descricao_atividade|
        #BD: id|ie_value|cpf|cnpj|status|ie_status_text|properties|cpf_main|group_cbx|updated_at|clients|cbx_cod|s3_url|created_at|created_by|updated_by|sources|
        
        columns_to_be = {'id_grupo': 'cbx_cod',
                         'ie': 'ie_value',
                         'cod': 'codigo_produtor_sap',
                         'situacao_ie': 'ie_status_text'}
        
        df.rename(columns=columns_to_be, inplace=True)
               
        df['cpf'] = df.apply(lambda row: format_zero_left(row.cpf), axis=1)            
        df['cnpj'] = df.apply(lambda row: format_zero_left(row.cnpj), axis=1)
        df['cpf_cnpj'] = df.apply(lambda row: format_zero_left(row.cpf_cnpj), axis=1)
        
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
            'municipio': str(row.municipio).replace('\n', '').replace("'", "''"),
            'logradouro': str(row.logradouro).replace('\n', '').replace("'", "''"),
            'complemento': '' if isinstance(row.complemento, float) and math.isnan(row.complemento) else str(row.complemento).replace('\n', ''),
            'arquivo_trello': '',
            'nomeempresarial': str(row.nome_empresarial).replace('\n', ''),
            'simplenaciponal': '',
            'demaisatividades': '',
            'formadetributacao': '',
            'situacaocadastral': '',
            'atividadeprincipal': str(row.descricao_atividade).replace('\n', ''),
            'data_inicio_atividade': f'{row.data_inicio_atividade}',
            'datasituacaocadastral': f'{row.data_situacao_cadastral}',
            'motivosituacaocadastral': ''            
            }), axis=1)       
        
        columns_to_keep = ['cbx_cod', 'cpf', 'cnpj', 'ie_value', 'ie_status_text',
                           'codigo_produtor_sap', 'cpf_cnpj',
                           'status', 'sources', 'clients', 'created_at',
                           'updated_at', 'created_by', 'updated_by', 'properties']
        df_subset = df[columns_to_keep]
                
        return df_subset
    
def get_update_sql(field_name, field_value, ie):    
    # Define the SQL statement as a string
    field_value = field_value.replace("\r", "\\r").replace("\n", "\\n").replace("'", "''").replace('"', '\\"')
    
    sql_statement = """
        UPDATE cbx.ie SET
          properties = jsonb_set(properties, '{{"{field_name}"}}', '"{field_value}"')
        WHERE ie_value = '{ie_value}';
    """

    # Format the SQL statement with the values
    formatted_sql = sql_statement.format(field_name=field_name, field_value=field_value, ie_value=ie)
    return formatted_sql

def get_group_business(cur, cbx_cod, cpf_cnpj):
    cur.execute(f"SELECT id, cbx_cod FROM cbx.group_business WHERE cbx_cod = {cbx_cod} AND cpf_cnpj = '{cpf_cnpj}' LIMIT 1")
    rows = cur.fetchall()
        
    data = []
    for row in rows:
        id = row[0]
        cbx_cod = row[1]
        
        data.append({
            'id': id,
            'cbx_cod': cbx_cod
        })
            
    return data[0]

def main():
    try:
        df = get_ies()
    except Exception as ex:            
        print(f'erro: {ex}')
        return
    
    prod=False
    
    conn = connect_to_db(prod)
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)    
        
    chunksize = 1
    
    for i in range(0, len(df), chunksize):
        try:            
            chunk = df[i:i+chunksize]
            ie = chunk['ie_value'].values[0]
                       
            if not ie or str(ie).lower() == 'nan' or not isinstance(ie, (int, float)):
                print(f'IE inválida {ie} - Type: {type(ie)}')
                continue
            
            cur.execute(f"SELECT COUNT(*) FROM cbx.ie where ie_value = {ie}")
            total = cur.fetchone()[0]
            if total == 0:
                # nao considera campo cpf
                subset_chunk = chunk.drop(columns=['cpf_cnpj'])
                subset_chunk.to_sql('ie', engine, schema='cbx', if_exists='append', index=False)
                print(f'Nova IE {ie}')
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
                    
                    cpf_cnpj = chunk['cpf_cnpj'].values[0]
                    cbx_cod = chunk['cbx_cod'].values[0]
                    codigo_produtor_sap = chunk['codigo_produtor_sap'].values[0]

                    #grp = get_group_business(cur, cbx_cod, cpf_cnpj)
                    #group_id={grp['id_main'] if grp['id_main'] else grp['id']}
                                                        
                    update_statement = f"""
                        update cbx.ie set 
                        updated_by=139,
                        updated_at=now(),
                        clients='{chunk['clients'].values[0]}',
                        sources='{chunk['sources'].values[0]}',
                        cbx_cod={cbx_cod},
                        ie_status_text='{chunk['ie_status_text'].values[0]}',
                        status=true,
                        cnpj='{properties_json['cnpj']}',
                        cpf='{properties_json['cpf']}',
                        codigo_produtor_sap={'null' if not codigo_produtor_sap or str(codigo_produtor_sap).lower() in ['nan', '-', 'na', 'n/a'] else codigo_produtor_sap}
                        where ie_value = {ie}
                    """
                    cur.execute(update_statement)
                    
                    conn.commit()
                    
                    print(f'Atualizada IE {ie}')
                except Exception as e:
                    print(f'ie: {ie} - erro: {ex.args}')
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
    