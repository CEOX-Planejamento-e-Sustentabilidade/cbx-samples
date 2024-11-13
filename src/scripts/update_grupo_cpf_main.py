from datetime import datetime
import json
import pandas as pd
from sqlalchemy import create_engine

from util_database import check_extension_pg_trgm, connect_to_db, execute_sql
from util_format import format_person_doc, format_zero_left

def get_bd_ie():
    with pd.ExcelFile('src/scripts/bd_ie/bd_ie_(03.2024).xlsx', engine='openpyxl') as xls:
        dtype_dict = {
            'cpf_cnpj': str,
            'cpf_principal': str
        }
        
        df = pd.read_excel(xls, dtype=dtype_dict)
        
        #antes: cpf_principal, id_grupo, Grupo, produtor, cpf_cnpj
        #dpois: cpf_principal, cbx_cod, group_name, group_farmer, cpf_cnpj
        
        columns_to_be = {'Grupo': 'group_name',
                         'id_grupo': 'cbx_cod',
                         'produtor': 'group_farmer'}
        
        df.rename(columns=columns_to_be, inplace=True)
        
        df['cpf_principal'] = df.apply(lambda row: format_zero_left(row.cpf_principal), axis=1)
        df['cpf_cnpj'] = df.apply(lambda row: format_zero_left(row.cpf_cnpj), axis=1)
        
        columns_to_keep = ['cpf_principal', 'cbx_cod', 'group_name', 'group_farmer', 'cpf_cnpj']
        df_subset = df[columns_to_keep]        
                
        return df_subset
        
def get_grupos_from_bd(cur, where=''):
    sql = f"SELECT * FROM cbx.group_business {f'where {where}' if where else ''}"
    cur.execute(sql)
    rows = cur.fetchall()    
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    return df

def update_group_farmer(conn, cur, df_ie):
    # atualiza o nome do produtor do grupo
    df_unique_groups = df_ie[['cpf_cnpj', 'cbx_cod', 'group_farmer']].drop_duplicates().sort_values(by='cbx_cod').reset_index(drop=True)
    for row in df_unique_groups.itertuples(index=True):
        try:           
            sql = f"""
                update cbx.group_business as gb set
                    group_farmer = '{row.group_farmer}'
                where gb.cbx_cod = {row.cbx_cod}
                and cpf_cnpj = '{row.cpf_cnpj}'
            """
            result = execute_sql(conn, cur, sql)
            print(f'{row.cbx_cod} - {row.cpf_cnpj} - rows affected: {str(result)}')
        except Exception as ex:
            print(f'error: {str(ex)}')        
    
def update_cpf_main(conn, cur, df_ie):
    # atualiza o grupo principal
    
    # df: cpf_principal, cbx_cod, group_name, group_farmer, cpf_cnpj
       
    # primeira lista: agrupar por código do grupo e cpf_principal
    df_unique_groups = df_ie[['cpf_principal', 'cbx_cod']].drop_duplicates().sort_values(by='cbx_cod').reset_index(drop=True)
        
    # atualizar todos os id_main do cpf que nao seja igual ao cpf_principal pelo codigo grupo
    # os id_main que permancerem nulos, serão os donos do grupo
    
    for row in df_unique_groups.itertuples(index=True):
        try:           
            sql = f"""
                update cbx.group_business as gb set
                    group_id_main = (select id from cbx.group_business sub where sub.cbx_cod = gb.cbx_cod and sub.cpf_cnpj = '{row.cpf_principal}' limit 1)
                where gb.cbx_cod = {row.cbx_cod}
                and cpf_cnpj <> '{row.cpf_principal}'
            """
            result = execute_sql(conn, cur, sql)
            print(f'{row.cbx_cod} - {row.cpf_principal} - rows affected: {str(result)}')
        except Exception as ex:
            print(f'error: {str(ex)}')
    
def insert(df, engine):
    erros = []
    chunk_size = 500
    
    #cpf_principal, cbx_cod, group_name, group_farmer, cpf_cnpj    
    
    # clients: 1 - FS
    df['status'] = True
    df['clients'] = df.apply(lambda row: json.dumps([{"id_client": 1}]), axis=1)
    df['created_at'] = datetime.now().isoformat()
    df['updated_at'] = datetime.now().isoformat()
    df['created_by'] = 139
    df['updated_by'] = 139
    
    # remover coluna cpf_principal
    df.drop(columns=['cpf_principal'], inplace=True)
        
    grouped_df = df.groupby(['cbx_cod', 'cpf_cnpj'], as_index=False).first()
    
    for start in range(0, len(grouped_df), chunk_size):
        df_chunk = grouped_df[start:start + chunk_size]
        try:
            if not df_chunk.empty:
                df_chunk.to_sql('group_business', con=engine, schema='cbx', if_exists='append', index=False, method='multi')
        except Exception as e:
            erros.append(e.args)
            print(f"Error inserting group business by chunk: {e}")

def delete_repeated(conn, cur):
    try:
        # cbx_cod e cpf_cnpj iguais
        sql = """
            with repetidos as(
                select cbx_cod, cpf_cnpj, count(*)
                from cbx.group_business 
                group by cbx_cod, cpf_cnpj
                having count(*) > 1
            ),
            completo as (
                select 
                    ROW_NUMBER() OVER (PARTITION BY gb.cbx_cod ORDER BY gb.cbx_cod) AS row_num,
                    gb.* 
                from cbx.group_business gb 
                inner join repetidos r on gb.cbx_cod = r.cbx_cod and gb.cpf_cnpj = r.cpf_cnpj
                order by gb.cbx_cod
            )
            delete from cbx.group_business where id in (select id from completo where row_num = 1)    
        """
        result = execute_sql(conn, cur, sql)
        print(f'all EQUAL repeated keys, cbx_cod and cpf_cnpj, was deleted - rows affected: {str(result)}')
        
        # cbx_cod iguais e cpf_cnpj similares com margem de 70% de acerto na comparacao
        check_extension_pg_trgm(conn, cur)
        
        sql = """
            delete from cbx.group_business 
            where id in (
                select gbb.id 
                from cbx.group_business gba, cbx.group_business gbb
                where similarity(gba.cpf_cnpj, gbb.cpf_cnpj) >= 0.7 
                and similarity(gba.cpf_cnpj, gbb.cpf_cnpj) < 1
                and (LENGTH(gba.cpf_cnpj) = 11 or LENGTH(gba.cpf_cnpj) = 14)
            )
        """ 
        result = execute_sql(conn, cur, sql)
        print(f'all SIMILAR (70% markup) repeated keys, cbx_cod and cpf_cnpj, was deleted - rows affected: {str(result)}')
    except Exception as ex:
        print(f'error: {str(ex)}')    

def main():       
    # atualizar grupos que nao existem na tabela de grupo economico
    # atualizar cpf principal e produtor da nova tabela de grupo economico
    # excluir grupos que contenham o mesmo codigo e mesmo cpf, ou cpfs 70% identicos
    # ex.:
    #   id   nome                    cod     cpf_cnpj
    #   55  COACEN - ADEMIR PIVATTO	1003	223868071340
    #   209 COACEN - ADEMIR PIVATTO	1003	22386807134
    # res: manter o id 209
    
    try:
        df_bd_ie  = get_bd_ie()
    except Exception as ex:
        print(f'erro: {ex}')
        return    
    
    prod=False
    
    conn = connect_to_db(prod)    
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
    
    try:
        df_grupo_from_bd = get_grupos_from_bd(cur)
    except Exception as ex:            
        print(f'erro: {ex}')
        return 
    
    # ATUALIZAR GRUPOS QUE NAO EXISTEM NA TABELA DE GRUPO ECONOMICO
    # entre bd_ie e base de dados, verificar os que nao existem na tabela group_business por cpf_cnpj
    # pegar de df_bd_ie todos os grupos que nao tem cpf em df_grupo_base
    
    # get just the groups that are not in the database by cod and cpf
    df_bd_ie['key'] = df_bd_ie['cpf_cnpj'].astype(str) + '-' + df_bd_ie['cbx_cod'].astype(str)
    df_grupo_from_bd['key'] = df_grupo_from_bd['cpf_cnpj'].astype(str) + '-' + df_grupo_from_bd['cbx_cod'].astype(str)
    # Filtra as linhas de df_bd_ie que não estão em df_grupo_from_bd para a chave combinada
    df_grupo_new = df_bd_ie[~df_bd_ie['key'].isin(df_grupo_from_bd['key'])]
    # Remove a coluna de chave combinada
    df_grupo_new = df_grupo_new.drop(columns=['key'])    
    
    if not df_grupo_new.empty:
        insert(df_grupo_new, engine)
            
    # excluir grupos que contenham o mesmo codigo e mesmo cpf, ou cpfs 70% identicos
    delete_repeated(conn, cur)
    
    # atualizar cpf principal da nova tabela de grupo economico
    update_cpf_main(conn, cur, df_bd_ie)
    
    # atualizar nome do produtor do grupo economico
    update_group_farmer(conn, cur, df_bd_ie)

    cur.close()
    conn.close()
    engine.dispose()
    
    print('---------------------')
    print('Grupo OK!')
    print('---------------------')

if __name__ == "__main__":
    main()

