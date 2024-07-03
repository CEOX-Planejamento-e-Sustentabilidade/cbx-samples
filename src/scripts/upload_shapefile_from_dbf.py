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
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)    
        
    chunksize = 1
    
    for i in range(0, len(df), chunksize):
        try:            
            chunk = df[i:i+chunksize]
            insert_into_db(engine=engine, df=chunk, table='shapefile')
            #chunk.to_sql('shapefile', engine, schema='cbx', if_exists='append', index=False)
        except Exception as ex:            
            print(f'erro: {ex.args}')
    
    try:
        reset_statement = """
        -- deleta todos os cars com source puramente de 'shapefile'
        delete from cbx.car_input WHERE sources::jsonb = '[{"id_source": 6}]'::jsonb;        
        
        /* reset sources sem shapefile, desconsidera todos as fontes 'shapefile'
        ex.:
        para sources = [{"id_source": 1}, {"id_source": 6}] 
        se torna = [{"id_source": 1}] 
        */

        DO $$ 
        declare
            total_rows integer;    
            iterations integer;
            offsetx integer;
            chunk_size integer := 1000; -- Adjust the chunk size as needed   
        BEGIN
            -- Count total rows that need updating  
            select count(*) 
            into total_rows
            from
            (
                SELECT filtered.id
                FROM (
                    SELECT ci.id, jsonb_array_elements(ci.sources::jsonb) AS elem
                    FROM cbx.car_input ci
                ) AS filtered	    
                WHERE filtered.elem->>'id_source' <> '6'
                group by filtered.id        
            ) x;

            -- Calculate number of iterations
            iterations := CEIL(total_rows / chunk_size);
            
            -- Process updates in batches
            FOR i IN 0 .. iterations - 1 loop
                begin
                    offsetx := i * chunk_size;
            
                    UPDATE cbx.car_input AS ci
                    SET sources = subq.sources
                    FROM (
                        SELECT filtered.id, jsonb_agg(filtered.elem) as sources
                        FROM (
                            SELECT ci.id, jsonb_array_elements(ci.sources::jsonb) AS elem
                            FROM cbx.car_input ci
                        ) AS filtered	    
                        WHERE filtered.elem->>'id_source' <> '6'
                        group by filtered.id
                        OFFSET offsetx LIMIT chunk_size 
                    ) AS subq
                    WHERE ci.id = subq.id
                    and ci.sources::jsonb @> '[{"id_source": 6}]'::jsonb;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Rollback in case of error
                        ROLLBACK;
                        RAISE EXCEPTION 'Error occurred: %', SQLERRM;
                end;
            END loop;
        END $$;        
        """
        #cur.execute(reset_statement)
        
        insert_statement = """                 
        -- adicionar cars que nao tem na ci mas tem na shp (cars com cpf)
        insert into cbx.car_input (nr_car, cpf_cnpj, nome_proprietario, status, 
        clients, 
        sources, 
        created_at, updated_at, created_by, updated_by)
        select distinct 
            shp.nr_car, shp.cpf_cnpj , '' as nome_proprietario, true as status, 
            COALESCE(ci.clients::jsonb, '[]'::jsonb) || '[{"id_client": 1}]'::jsonb as clients, 
            COALESCE(ci.sources::jsonb, '[]'::jsonb) || '[{"id_source": 6}]'::jsonb as sources,
            current_timestamp, current_timestamp, 139, 139
        from cbx.shapefile shp
        left join cbx.car_input ci on shp.nr_car = ci.nr_car and shp.cpf_cnpj = ci.cpf_cnpj
        where ci.nr_car is null 
        and ci.cpf_cnpj is null
        and shp.nr_car <> ''
        and shp.nr_car <> 'NÃO TEM CAR';

        -- atualizar car que estao em shp e ci, mas nao tem cpf no ci
        update cbx.car_input ci SET
            cpf_cnpj = s.cpf_cnpj
            ,sources = coalesce(sources::jsonb, '[]'::jsonb) || '[{"id_source": 6}]'::jsonb
        from cbx.shapefile s
        where s.nr_car = ci.nr_car
        and s.cpf_cnpj is not null
        and ci.cpf_cnpj is null
        and s.nr_car <> 'NÃO TEM CAR'
        and s.nr_car <> ''
        and not ci.sources::jsonb @> '[{"id_source": 6}]'::jsonb;
        """
        cur.execute(insert_statement)
       
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e    
    
    cur.close()
    engine.dispose()
    conn.close()        
    
    print('---------------------')
    print(f'Upload SHAPEFILE ok! - {conn.dsn}')
    print('---------------------')

if __name__ == "__main__":
    main()
    