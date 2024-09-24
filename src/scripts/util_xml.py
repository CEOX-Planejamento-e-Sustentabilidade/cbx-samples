import re
from sqlalchemy import create_engine
import xmltodict
import json
import pandas as pd

from pathlib import Path
from util_database import connect_to_db, format_in

class UtilXml:
    def remove_xml_header(self, xml_content):
        
        # Regular expression pattern to match XML declaration
        # This pattern matches <?xml ... ?> declarations with optional attributes
        pattern = r'^\s*<\?xml\s+[^>]*\?>\s*'

        # Remove XML declaration using regular expression
        cleaned_content = re.sub(pattern, '', xml_content, count=1, flags=re.DOTALL)

        return cleaned_content
    
    def save_xmls(self, pasta, client_id):
        xml_folder = Path(pasta).rglob('*.xml')
        files = [x for x in xml_folder]
        erros = []
        total_sucesso = 0
        total_saved = 0

        # Process files in chunks of 15,000
        chunk_size_files = 15000
        
        # get all keys that are in database from the keys_nf        
        con = connect_to_db(prod=True)
        engine = create_engine('postgresql+psycopg2://', creator=lambda: con)
        cur = con.cursor()
        cur.execute(f"SELECT key_nf FROM cbx.nf")
        rows = cur.fetchall()
        df_rows = pd.DataFrame(rows, columns=['key_nf'])
        keys_nf_bd = set(df_rows['key_nf'])
        df_key_nf_bd = pd.DataFrame(keys_nf_bd, columns=['key_nf'])

        for i in range(0, len(files), chunk_size_files):
            nf = []
            nf_view = []
            chunk_files = files[i:i + chunk_size_files]
            for file in chunk_files:
                try:               
                    with open(file, 'r', encoding='utf-8-sig') as xml_file:            
                        content_xml = xml_file.read()
                        if not content_xml:
                            continue
                        content_xml = self.remove_xml_header(content_xml)
                        content_json = xmltodict.parse(content_xml)    
                    
                    if 'procEventoNFe' in content_json:
                        infEvento = content_json['procEventoNFe']['evento']['infEvento']
                        nf.append(                    
                        {
                            'client_id': client_id,
                            'key_nf': infEvento['chNFe'],
                            'status': not 'cancelad' or not 'canc' in file.name.lower(),
                            'situacao': 'Evento de Cancelamento para o CPF: '+
                                infEvento['CPF']
                                    if 'CPF' in infEvento
                                    else infEvento['CNPJ']
                                        if 'CNPJ' in infEvento
                                        else 'nao informado',
                            'content_json': json.dumps(content_json),
                            'content_xml': content_xml,
                        })
                        
                        desc_evento = infEvento['detEvento']['descEvento'] \
                            +' - '+ infEvento['detEvento']['xJust']
                        nf_view.append({
                            'key_nf': infEvento['chNFe'],
                            'data_emissao': infEvento['dhEvento'], #data do evento
                            'razao_social_emissor': desc_evento,
                            'razao_social_destinatario': desc_evento,
                            'client_id': client_id
                        })
                    else:
                        nf_json = content_json['nfeProc']['NFe'] if 'nfeProc' in content_json else content_json['NFe']
                        infNFe = nf_json['infNFe']
                        ide = infNFe['ide']            
                        
                        ie_emissor = infNFe['emit']['IE'] if 'IE' in infNFe['emit'] else ''
                        ie_dest = infNFe['dest']['IE'] if 'IE' in infNFe['dest'] else ''
                        cpf_emissor = infNFe['emit']['CPF'] if 'CPF' in infNFe['emit'] \
                                else infNFe['emit']['CNPJ'] \
                                    if 'CNPJ' in infNFe['emit'] \
                                    else ''
                        cpf_dest = infNFe['dest']['CNPJ'] \
                            if 'CNPJ' in infNFe['dest'] \
                            else infNFe['dest']['CPF'] \
                                if 'CPF' in infNFe['dest'] \
                                else ''
                        razao_emissor = infNFe['emit']['xNome']
                        razao_dest = infNFe['dest']['xNome']
                        
                        nf.append(                    
                        {
                            'client_id': client_id,
                            'date': ide['dhEmi'],
                            'key_nf': infNFe['@Id'][3:],
                            'status': not 'cancelad' in file.name.lower(),
                            'situacao': content_json['nfeProc']['protNFe']['infProt']['xMotivo']
                                if 'nfeProc' in content_json 
                                else 'Não Informado',
                            'content_json': json.dumps(content_json),
                            'content_xml': content_xml,
                            'ie_emissor': ie_emissor,
                            'ie_destinatario': ie_dest,
                            'cnpj_cpf_emissor': cpf_emissor,                        
                            'cnpj_cpf_destinatario': cpf_dest,
                            'razao_social_emissor': razao_emissor,
                            'razao_social_destinatario': razao_dest,
                            'fantasia_emissor': infNFe['emit']['xFant']
                                if 'xFant' in infNFe['emit']
                                else '',
                            'email_destinatario': infNFe['dest']['email']
                                if 'email' in infNFe['dest']
                                else ''                
                        })
                        
                        if 'det' in infNFe:
                            prod_list = infNFe['det']
                            
                            if not isinstance(prod_list, list):
                                prod = prod_list['prod']                   
                                
                                nf_view.append({
                                    'key_nf': infNFe['@Id'][3:],
                                    'nro_nota': ide['nNF'], 
                                    'tipo_nota': ide['tpNF'], 
                                    'data_emissao': ide['dhEmi'], 
                                    'ie_emissor': ie_emissor, 
                                    'cnpj_cpf_emissor': cpf_emissor,
                                    'razao_social_emissor': razao_emissor, 
                                    'ie_destinatario': ie_dest, 
                                    'cnpj_cpf_destinatario': cpf_dest,
                                    'razao_social_destinatario': razao_dest,
                                    'cfop': prod['CFOP'], 
                                    'ncm': prod['NCM'], 
                                    'nome_produto': prod['xProd'], 
                                    'quantidade': prod['qCom'], 
                                    'unidade_medida': prod['uCom'], 
                                    'client_id': client_id
                                })
                            else:
                                for item in prod_list:
                                    prod = item.get('prod', {})
                                    
                                    nf_view.append({
                                        'key_nf': infNFe['@Id'][3:],
                                        'nro_nota': ide['nNF'], 
                                        'tipo_nota': ide['tpNF'], 
                                        'data_emissao': ide['dhEmi'], 
                                        'ie_emissor': ie_emissor, 
                                        'cnpj_cpf_emissor': cpf_emissor,
                                        'razao_social_emissor': razao_emissor,
                                        'ie_destinatario': ie_dest, 
                                        'cnpj_cpf_destinatario': cpf_dest, 
                                        'razao_social_destinatario': razao_dest,
                                        'cfop': prod['CFOP'], 
                                        'ncm': prod['NCM'], 
                                        'nome_produto': prod['xProd'], 
                                        'quantidade': prod['qCom'], 
                                        'unidade_medida': prod['uCom'], 
                                        'client_id': client_id            
                                    })                                
                except Exception as ex:
                    print(f'Erro arquivo: {file.name}  {str(ex)}')
                
            df1 = pd.DataFrame(nf)       
            df2 = pd.DataFrame(nf_view)            
            df1 = df1.drop_duplicates(subset=['key_nf']).reset_index(drop=True)
            df2 = df2.drop_duplicates(subset=['key_nf']).reset_index(drop=True)
                        
            keys_nf = set(df1['key_nf'])                                   
            df_key_nf = pd.DataFrame(keys_nf, columns=['key_nf'])
            
            # get just the key_nfs that are not in the database
            df_keys_bulk = df_key_nf[~df_key_nf['key_nf'].isin(df_key_nf_bd['key_nf'])]
            if not df_keys_bulk.empty:
                df_key_nf_bd = pd.concat([df_key_nf_bd, df_keys_bulk], ignore_index=True)
                df_key_nf_bd = df_key_nf_bd.drop_duplicates(subset=['key_nf']).reset_index(drop=True)
            
            # filter df1 and df2 to keep rows where 'key_nf' is in df_keys_bulk['key_nf']
            df1_final = df1[df1['key_nf'].isin(df_keys_bulk['key_nf'])]
            df2_final = df2[df2['key_nf'].isin(df_keys_bulk['key_nf'])]           
            
            chunk_size = 15000
            total_saved += len(df1_final)           
            for start in range(0, max(len(df1_final), len(df2_final)), chunk_size):
                df1_chunk = df1_final[start:start + chunk_size]
                df2_chunk = df2_final[start:start + chunk_size]

                try:
                    if not df1_chunk.empty:
                        df1_chunk.to_sql('nf', con=engine, schema='cbx', if_exists='append', index=False, method='multi')
                        total_sucesso += 1
                except Exception as e:
                    erros.append(e.args)
                    print(f"Error inserting nf by chunk: {e}")

                try:
                    if not df2_chunk.empty:
                        df2_chunk.to_sql('nf_view', con=engine, schema='cbx', if_exists='append', index=False, method='multi')
                        total_sucesso += 1
                except Exception as e:
                    erros.append(e.args)            
                    print(f"Error inserting nf-view by chunk: {e}")
                                
        return erros, total_sucesso, total_saved, len(erros)
    