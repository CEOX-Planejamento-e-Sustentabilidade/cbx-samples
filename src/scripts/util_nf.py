import os
import re
import traceback
import numpy as np
import pandas as pd
import pytz
import xmltodict

from collections import Counter
from datetime import datetime
from pandas import ExcelWriter
from pathlib import Path
from util_database import connect_to_db

class UtilNf:
    PRODx = False
    def __init__(self):
        self = self
    
    def get_ncms(self):
        conn = connect_to_db(prod=self.PRODx)
        cur = conn.cursor()
        
        query = """
            select id, name, ncm, status, type_ncm, group_ncm, properties from cbx.ncms        
            """
        
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        results = [
            {
                "id": ncms[0],
                "name": ncms[1],
                "ncm": ncms[2],
                "status": ncms[3],
                "tipo": ncms[4],
                "grupo": ncms[5],
                "properties": ncms[6]
            } for ncms in rows]

        return rows, columns


    def remove_xml_header(self, xml_content):
        
        # Regular expression pattern to match XML declaration
        # This pattern matches <?xml ... ?> declarations with optional attributes
        pattern = r'^\s*<\?xml\s+[^>]*\?>\s*'

        # Remove XML declaration using regular expression
        cleaned_content = re.sub(pattern, '', xml_content, count=1, flags=re.DOTALL)

        return cleaned_content

    def parser_nf_insumos(self, dados, xml_path):
        try:
            with open(xml_path, encoding='utf-8-sig') as arquivo:
                file_content = arquivo.read()

                CFOPs = []
                PLACA = ""
                FANTASIA = ""
                UF = ""
                PRODUTOS = []
                TOTAL = 0
                NATOP = ""
                CFOP = ""
                CHAVE = ""
                info_adicionais = ""
                infAdFisco = ""
                CNPJ = ""
                IE = ""
                CPF = ""
                FORNECEDOR_NOME = ""
                RETIRADA_DE = ""
                CPF = ""
                FORNECEDOR_IE = ""
                FORNECEDOR_NF = ""
                EMITIDA_EM = ""
                PLACA = ""
                nNF = ""
                DATA_EMISSAO = ""
                NATOP = ""
                CFOP = ""
                NOME = ""
                DEST_CPF_CNPJ = ""
                DEST_NOME = ""
                DEST_IE = ""

                file_content = self.remove_xml_header(file_content)
                doc = xmltodict.parse(file_content)
                    # .replace("<?xml version=\"1.0\" encoding=\"utf-8\"?>", "")
                    # .replace("<?xml version=\"1.0\"?>", ""))

                NATOP = doc["nfeProc"]["NFe"]["infNFe"]["ide"]["natOp"]

                tpNF = doc["nfeProc"]["NFe"]["infNFe"]["ide"]["tpNF"]

                CHAVE = doc["nfeProc"]["protNFe"]["infProt"]["chNFe"]

                nNF = doc["nfeProc"]["NFe"]["infNFe"]["ide"]["nNF"]
                DATA_EMISSAO = doc["nfeProc"]["NFe"]["infNFe"]["ide"]["dhEmi"]

                if "CNPJ" in doc["nfeProc"]["NFe"]["infNFe"]["emit"]:
                    CNPJ = doc["nfeProc"]["NFe"]["infNFe"]["emit"]["CNPJ"]
                if "IE" in doc["nfeProc"]["NFe"]["infNFe"]["emit"]:
                    IE = doc["nfeProc"]["NFe"]["infNFe"]["emit"]["IE"]
                if "CPF" in doc["nfeProc"]["NFe"]["infNFe"]["emit"]:
                    CPF = doc["nfeProc"]["NFe"]["infNFe"]["emit"]["CPF"]
                if "xNome" in doc["nfeProc"]["NFe"]["infNFe"]["emit"]:
                    NOME = doc["nfeProc"]["NFe"]["infNFe"]["emit"]["xNome"]
                if "xFant" in doc["nfeProc"]["NFe"]["infNFe"]["emit"]:
                    FANTASIA = doc["nfeProc"]["NFe"]["infNFe"]["emit"]["xFant"]
                if "CNPJ" in doc["nfeProc"]["NFe"]["infNFe"]["dest"]:
                    DEST_CPF_CNPJ = doc["nfeProc"]["NFe"]["infNFe"]["dest"]["CNPJ"]
                    DEST_NOME = doc["nfeProc"]["NFe"]["infNFe"]["dest"]["xNome"]
                if "IE" in doc["nfeProc"]["NFe"]["infNFe"]["dest"]:
                    DEST_IE = doc["nfeProc"]["NFe"]["infNFe"]["dest"]["IE"]
                if "CPF" in doc["nfeProc"]["NFe"]["infNFe"]["dest"]:
                    DEST_CPF_CNPJ = doc["nfeProc"]["NFe"]["infNFe"]["dest"]["CPF"]
                    DEST_NOME = doc["nfeProc"]["NFe"]["infNFe"]["dest"]["xNome"]

                if "infAdic" in doc["nfeProc"]["NFe"]["infNFe"]:
                    if "infAdFisco" in doc["nfeProc"]["NFe"]["infNFe"]["infAdic"]:
                        infAdFisco = doc["nfeProc"]["NFe"]["infNFe"]["infAdic"]["infAdFisco"]
                    if "infCpl" in doc["nfeProc"]["NFe"]["infNFe"]["infAdic"]:
                        info_adicionais = doc["nfeProc"]["NFe"]["infNFe"]["infAdic"]["infCpl"]

                if "veicTransp" in doc["nfeProc"]["NFe"]["infNFe"]["transp"]:
                    PLACA = doc["nfeProc"]["NFe"]["infNFe"]["transp"]["veicTransp"]["placa"]
                    if 'UF' in doc["nfeProc"]["NFe"]["infNFe"]["transp"]["veicTransp"]:
                        UF = doc["nfeProc"]["NFe"]["infNFe"]["transp"]["veicTransp"]["UF"]

                itens = doc["nfeProc"]["NFe"]["infNFe"]["det"]

                verifica_lista = isinstance(itens, list)

                cEAN = ""
                NCM = ""
                xProd = ""
                uCom = ""
                qCom = ""
                vUnCom = ""
                vProd = ""
                uTrib = ""
                qTrib = ""
                vUnTrib = ""

                if verifica_lista:
                    for itens in itens:
                        for key, value in itens.items():
                            if key == "prod":
                                for prod_key, prod_value in value.items():
                                    if prod_key == "uCom":
                                        uCom = prod_value
                                    if prod_key == "qCom":
                                        qCom = prod_value
                                        TOTAL += float(qCom)
                                    if prod_key == "xProd":
                                        xProd = prod_value
                                    if prod_key == "CFOP":
                                        CFOP = prod_value
                                        CFOPs.append(CFOP)
                                    if prod_key == "cProd":
                                        cProd = prod_value
                                    if prod_key == "cEAN":
                                        cEAN = prod_value
                                    if prod_key == "NCM":
                                        NCM = prod_value
                                    if prod_key == "vUnCom":
                                        vUnCom = prod_value
                                    if prod_key == "vProd":
                                        vProd = prod_value
                                    if prod_key == "uTrib":
                                        uTrib = prod_value
                                    if prod_key == "qTrib":
                                        qTrib = prod_value
                                    if prod_key == "vUnTrib":
                                        vUnTrib = prod_value
                                PRODUTOS.append(
                                    {"cProd": cProd, "cEAN": cEAN, "NCM": float(NCM), "CFOP": float(CFOP), "xProd": xProd,
                                    "uCom": uCom, "qCom": float(qCom), "vUnCom": float(vUnCom), "vProd": float(vProd),
                                    "uTrib": uTrib, "qTrib": float(qTrib), "vUnTrib": float(vUnTrib)})
                else:
                    for key, value in itens.items():
                        if key == "prod":
                            for prod_key, prod_value in value.items():
                                if prod_key == "uCom":
                                    uCom = prod_value
                                if prod_key == "qCom":
                                    qCom = prod_value
                                    TOTAL += float(qCom)
                                if prod_key == "xProd":
                                    xProd = prod_value
                                if prod_key == "CFOP":
                                    CFOP = prod_value
                                    CFOPs.append(CFOP)
                                if prod_key == "cProd":
                                    cProd = prod_value
                                if prod_key == "cEAN":
                                    cEAN = prod_value
                                if prod_key == "NCM":
                                    NCM = prod_value
                                if prod_key == "vUnCom":
                                    vUnCom = prod_value
                                if prod_key == "vProd":
                                    vProd = prod_value
                                if prod_key == "uTrib":
                                    uTrib = prod_value
                                if prod_key == "qTrib":
                                    qTrib = prod_value
                                if prod_key == "vUnTrib":
                                    vUnTrib = prod_value
                            PRODUTOS.append(
                                {"cProd": cProd, "cEAN": cEAN, "NCM": float(NCM), "CFOP": float(CFOP), "xProd": xProd,
                                "uCom": uCom, "qCom": float(qCom), "vUnCom": float(vUnCom), "vProd": float(vProd),
                                "uTrib": uTrib, "qTrib": float(qTrib), "vUnTrib": float(vUnTrib)})

            for produto in PRODUTOS:
                dados.append({
                    "arquivo": xml_path,
                    "NF": nNF,
                    "INSUMO": " ",
                    "TIPO NF": tpNF,
                    "CHAVE": CHAVE,
                    "DATA EMISSAO": datetime.fromisoformat(DATA_EMISSAO).astimezone(pytz.utc),
                    "NATUREZA OPERACAO": NATOP,
                    "CFOPs": Counter(CFOPs),
                    "FORNECEDOR": NOME,
                    "NOME FANTASIA": FANTASIA,
                    "CPF": CPF,
                    "CNPJ": CNPJ,
                    "IE": IE,
                    "COMPRADOR_CPF_CNPJ": DEST_CPF_CNPJ,
                    "COMPRADOR": DEST_NOME,
                    "COMPRADOR_IE": DEST_IE,
                    "VEICULO": PLACA,
                    "VEICULO UF": UF,
                    "INFORMACOES ADICIONAIS": info_adicionais,
                    "INFORMACOES FISCO": infAdFisco,
                    "cProd": produto["cProd"],
                    "cEAN": produto["cEAN"],
                    "NCM": produto["NCM"],
                    "CFOP": produto["CFOP"],
                    "xProd": produto["xProd"],
                    "uCom": produto["uCom"],
                    "qCom": produto["qCom"],
                    "vUnCom": produto["vUnCom"],
                    "vProd": produto["vProd"],
                    "uTrib": produto["uTrib"],
                    "qTrib": produto["qTrib"],
                    "vUnTrib": produto["vUnTrib"]
                })
            return dados

        except Exception as ex:
            traceback.print_exc()
            raise ex


    def processar_nfs_insumos(self, pasta, file):
        try:
            erros = []
            total_sucesso = 0
            total_erros = 0
            dados = []
            xml_folder = Path(pasta).rglob('*.xml')
            files = [x for x in xml_folder]

            for f in files:
                if "__MACOSX" not in str(f):
                    try:
                        dados_nfs = self.parser_nf_insumos(dados, f)
                        if dados_nfs is not None:
                            dados = dados_nfs
                        total_sucesso += 1
                    except Exception as ex:
                        erros.append({"arquivo": str(f), "exception": str(ex)})
                        total_erros += 1
                        pass
                    
            df = pd.DataFrame(dados)

            # consultar NCMs
            ncms, columns = self.get_ncms()
            df_ncms = pd.DataFrame(
                ncms, columns=columns)

            df_ncms = df_ncms.drop(['id', 'status', 'properties'], axis=1)
            df_ncms['ncm'] = df_ncms['ncm'].astype(int)

            df['NCM'] = df['NCM'].astype(int)
            df = df.join(df_ncms.set_index('ncm'), on='NCM')

            # TODO tratar tipo das colunas
            df['NF'] = df['NF'].astype(int)
            df['DATA EMISSAO'] = pd.to_datetime(df['DATA EMISSAO'])
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df.sort_values(by='DATA EMISSAO', ascending=True)
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO'])
            df['DATA EMISSAO'] = pd.to_datetime(
                df['DATA EMISSAO']).dt.strftime('%d/%m/%Y')

            df["arquivo"] = df["arquivo"].astype(str)
            condicao = df["arquivo"].str.contains("cancelada", case=False)
            df["INSUMO"] = np.where(condicao, "NF cancelada", df["INSUMO"])
            
            # reordenar colunas
            nova_ordem = [
                'NF',
                'CHAVE',
                'DATA EMISSAO',
                'FORNECEDOR',
                'COMPRADOR_IE',
                'CFOP',
                'name',
                'uCom',            
                'qCom',
                'IE',
                'NCM',
                'INSUMO',
                'NATUREZA OPERACAO',
                'NOME FANTASIA',
                'COMPRADOR',                      
                'VEICULO',
                'VEICULO UF',
                'INFORMACOES ADICIONAIS',
                'INFORMACOES FISCO',                     
                'xProd',
                'tipo',
                'grupo',
                'TIPO NF',
                'CFOPs',
                'CPF',
                'CNPJ',
                'COMPRADOR_CPF_CNPJ',
                'vUnCom',
                'vProd',
                'uTrib',
                'qTrib',
                'vUnTrib',
                'cProd',
                'cEAN',
                'arquivo']

            df = df.reindex(columns=nova_ordem)

            df_com_grupo = df[df['grupo'].notna()]
            df_sem_grupo = df[df['grupo'].isna()]

            file_path = os.path.join("uploads", "processed", file + ".xlsx")

            # SEPARA EM DUAS ABAS, COM BASE NO CAMPO DO GRUPO DO NCM
            with ExcelWriter(file_path) as writer:
                df_com_grupo.to_excel(
                    writer, sheet_name='com_grupo_classificado', index=False, header=True)
                df_sem_grupo.to_excel(
                    writer, sheet_name='sem_grupo', index=False, header=True)

            return True, erros, total_sucesso, total_erros
        except Exception as ex:
            traceback.print_exc()
            return False, None, total_sucesso, total_erros
