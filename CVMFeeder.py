""" BY KEVIN PERGHER 
    PORTO ALEGRE, BRAZIL, APRIL 4th 2024 
"""
from CVMModules import CVMAsyncBackend
from unidecode import unidecode

import pandas
import requests
import warnings
import datetime
import time

import base64
import json

warnings.filterwarnings("ignore")
# %%

class CVM():
    """ CLASSE CVM()

        > HISTORICOS DE DOCUMENTOS ESTRUTURADOS CVM
        > HISTORICOS DE DOCUMENTOS NAO ESTRUTURADOS CVM
        > GERACAO DE BASE DE DADOS ESTRUTURADOS CVM
        > GERACAO DE BASE DE DADOS NAO ESTRUTURADOS CVM
    """

    """
    def log_file(self):
        #ARQUIVO DE LOG DE REFERENCIA
        #ARMAZENA TODAS AS INFORMACOES DAS RODAGENS DO DIA
        #INFORMACOES, WARNINGS, ERRORS E ETC.
        #POR DEFAULT O NOME DO ARQUIVO E RELATIVO AO DIA DA EXECUCAO

        self.logger = logging.getLogger("log_file")
        self.logger.setLevel(logging.INFO)
        try:
            file_handler = logging.FileHandler(f"log_files//{str(datetime.date.today())}.log")
        except FileNotFoundError:
            os.mkdir("log_files//")
            file_handler = logging.FileHandler(f"log_files//{str(date.today())}.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s > %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.info("Inicializando processamento de dados via Feed CVM")
        self.logger.info(f"By Kevin Pergher - GitHub: @KgPergher98 - Dia: {str(datetime.date.today())}")
    """

    def __init__(self) -> None:
        # CRIA ARQUIVO DE LOG
        #CVM.log_file(self)
        try:
            # INSTANCIA A CLASSE CMV Async
            self.cvm_http_client = CVMAsyncBackend()
            #self.logger.info("CVM Async (BrFinance) instanciada com sucesso")
        except Exception as exc:
            print(exc)
            #self.logger.error(f"CVM Async (BrFinance) nao instanciada - {exc}")

    def get_cvm_categories(self):
        """ EXTRAI AS INFORMAÇÕES DE CATEGORIAS
            I.E. OS TIPOS DE DOCUMENTOS DISPONÍVEIS
        """
        categories = self.cvm_http_client.get_consulta_externa_cvm_categories()
        categories = pandas.DataFrame.from_dict(categories, orient = "index").reset_index()
        categories.columns = ["code", "description"]
        categories["doc_type"] = "eventuais"
        categories.doc_type[categories.code.str.startswith("EST")] = "estruturados"
        return categories

    def get_cvm_codes(self):
        """ EXTRAI OS CODIGOS CVM + STATUS CVM

            UTILIZA A BIBLIOTECA BrFINANCE E DADOS DA CVM
        """
        codes = self.cvm_http_client.get_cvm_codes()
        codes = pandas.DataFrame.from_dict(codes, orient = "index")
        pattern = r'\((.*?)\)'
        codes[1] = codes[0].str.extract(pattern).fillna("-")
        codes[0] = codes[0].str.replace(pattern, "", regex = True)
        codes = codes.reset_index()
        codes.columns = ["cvm", "firm_name", "status"]
        return codes

    def format_cnpj(original_number):
        # FORMATACAO DE CNPJ
        if original_number != "-":
            formatted_cnpj = original_number[:2] + '.' + original_number[2:5] + '.' 
            return(formatted_cnpj + original_number[5:8] + '/' + original_number[8:12] + '-' + original_number[12:])
        else:
            return(original_number)

    def cvm_history(self, cvm_code:list = "000000", documents:list = [], start_date = datetime.date(1995,12,31), max_retry = 3, last_ref = True):
        """ CVM_DOCUMENTS()
            RECUPERA O HISTORICO DE DOCUMENTOS DISPONIVEIS PARA A COMPANHIA

            CVM_CODE   [STR]            - CODIGO CVM DO ATIVO
            DOCUMENTS  [LIST]           - LISTA DOS DOCUMENTOS A SEREM EXTRAIDOS
            START_DATE [DATETIME.DATE]  - DATA DE INICIO DA EXTRACAO DOS DOCUMENTOS
            MAX_RETRY  [INT]            - # MAXIMO DE TENTATIVAS PARA EXTRACAO
            LAS_REF    [BOOL]           - SE TRUE, APENAS OS DADOS MAIS RECENTES SAO EXTRAIDOS
        """

        def get_unique_code(docs): # CRIA OS CODIGOS DE ID DOS DOCUMENTOS
            dc = docs.copy()
            cd = dc.descTipo
            cd[cd == ""] = dc.categoria.str.split(" - ", expand = True)[0]
            cd += "_DR" + dc.ref_date.astype(str)
            cd += "_DE" + dc.data_entrega.astype(str).str.split(" ", expand = True)[0]
            cd += "_vs" + dc.version.astype(str)
            cd += "_" + dc.numProtocolo + "/" + dc.numSequencia
            cd = dc.cod_cvm + "_" + cd
            return cd.str.strip()
        
        got = False # VARIAVEL CONTROLE DO LOOP
        retries = 0 # TENTATIVAS REALIZADAS ATE ENTAO
        while not got:
            try:
                if retries < max_retry:
                    search_result = self.cvm_http_client.get_consulta_externa_cvm_results(
                        start_date = start_date, # DATA DE CORTE
                        end_date = datetime.date.today(), # DEFAULT: HOJE
                        cod_cvm = [cvm_code], # CODIGO DE INTERESSE
                        participant_type = [1],
                        category = documents, # LISTA DE DOCUMENTOS
                        last_ref_date = last_ref
                    )
                got = True
            except Exception as exc:
                print(exc)
                retries += 1 # INCREMENTA AS TENTATIVAS
                search_result = pandas.DataFrame() # RESULTADO DEFAULT
        if not search_result.empty: search_result["id"] = get_unique_code(docs = search_result)
        return search_result
    
    def get_document(self, doc:pandas.Series):
        """ EXTRAI DOCUMENTOS INDIVIDUAIS VIA CVM

            DOC [PANDAS.SERIES] - VETOR DE INFORMACOES DE ARQUIVOS CVM

            CREDITOS AO MAIKE MOTTA PELO AUXILIO!
        """
        url = "https://www.rad.cvm.gov.br/ENET/frmExibirArquivoIPEExterno.aspx/ExibirPDF" # SOURCE
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Referrer-Policy": "strict-origin-when-cross-origin"
        }
        protocol = doc.numProtocolo # PROTOCOLO EH A REFERENCIA PADRAO AOS DOCUMENTOS NAO ESTRUTURADOS
        data = {
            "codigoInstituicao": "1",
            "numeroProtocolo": protocol,
            "token": "",
            "versaoCaptcha": ""
        }
        response = requests.post(url, json=data, headers=headers) # REQUEST SIMPLES
        if response.status_code == 200: # SE BEM SUCEDIDO
            json_data = json.loads(response.text)
            encoded_pdf = json_data['d']
            pdf_data = base64.b64decode(encoded_pdf) # CARREGA JSON E CONVERTE PRA BASE 64
            return pandas.DataFrame([pdf_data], columns = ["file"])
        else: return pandas.DataFrame() # FAIL, RETORNA DEFAULT

    def get_documents(self, docs:pandas.DataFrame, access_time:int = 5):
        """ EXTRACAO CONTINUA DE DATAFRAMES DE DOCUMENTOS CVM

            DOCS        [PANDAS.DATAFRAME] - BASE DE DADOS PADRAO CVM DE EXTRACAO DE DOCUMENTOS
            ACCESS_TIME [INT]              - TEMPO DE DELAY ENTRE EXTRACOES 
        """
        df = pandas.DataFrame()
        dc = docs.copy()
        for _, row in dc.iterrows(): # AVALIA CADA DOCUMENTO
            aux = CVM().get_document(doc = row)
            time.sleep(access_time)
            if not aux.empty:
                aux["cvm_code"] = row.id.split("_")[0]
                aux["document"] = row.id.split("_")[1]
                aux["date_rel"] = row.id.split("_")[2].replace("DR","")
                aux["date_ref"] = row.id.split("_")[3].replace("DE","")
                aux["versions"] = row.id.split("_")[4].replace("vs","")
                aux["protocol"] = row.id.split("_")[5]
                df = pandas.concat([df, aux], axis = 0)
        return df.reset_index(drop = True)
    
    def get_report(self, doc):
        """ EXTRACAO DE RELATORIO INDIVIDUAL

            DOC [PANDAS.SERIES] - VETOR DE DOCUMENTO ESTRUTURADO CVM 
        """

        def format_2_df(info):
            """ FORMATACAO DE DADOS CONTABEIS PARA PROCESSAMENTO

                INFO [PANDAS.DATAFRAME] - MATRIZ DE DADOS 
            """
            if info.shape[1] == 4: # CASO PADRAO
                info.columns = ["conta", "descricao", "valor", "moeda"]
                info["detail"] = "-"
            else: # CASO EXTENDIDO
                info.columns = [unidecode(x.strip()).replace(" ","_").lower() for x in info.columns]
                new_info = pandas.DataFrame()
                for col in info.columns: # CONCATENA A MATRIZ EM UMA "TRIPA"
                    if col not in ["conta", "descricao", "currency_unit"]:
                        aux = info[["conta", "descricao", "currency_unit", col]]
                        aux.columns = ["conta", "descricao", "moeda", "valor"]
                        aux["detail"] = col.upper()
                        new_info = pandas.concat([new_info, aux], axis = 0)
                info = new_info.copy()
            codify = info.conta.str.split(".", expand = True).fillna(0).applymap(lambda x: str(x).zfill(3)) # AJUSTE DE CODIGO CONTABIL
            info.conta = codify.apply(lambda row: '.'.join(row), axis = 1) # AJUSTE DE DESCRICAO
            if codify.shape[1] == 4: info.conta += ".000" # QUESTAO FORMAL APENAS
            info.descricao = info.descricao.apply(lambda x: unidecode(x).upper().strip().replace(" ","_")) # PADRAO DE DESCRICAO
            info.valor = info.valor.fillna(0).astype(float).round(2) # AJUSTE DE VALOR
            info.moeda = info.moeda.str.replace("Reais Mil", "(Mil)R$") # AJUSTE DE MOEDA
            return info[["conta", "descricao", "detail", "valor", "moeda"]]

        documents_sheet = { # REPORTS DE INTERESSE
            'Balanço Patrimonial Ativo': 'BALANCO_PATRIMONIAL_ATIVO',
            'Balanço Patrimonial Passivo': 'BALANCO_PATRIMONIAL_PASSIVO',
            'Demonstração do Resultado': 'DEMONSTRACAO_RESULTADO',
            'Demonstração do Resultado Abrangente': 'DEMONSTRACAO_RESULTADO_ABRANGENTE',
            'Demonstração do Fluxo de Caixa': 'DEMONSTRACAO_FLUXO_CAIXA',
            'Demonstração das Mutações do Patrimônio Líquido': 'DEMONSTRACAO_MUTUACOES_PATRIMONIO_LIQUIDO',
            'Demonstração de Valor Adicionado': 'DEMONSTRACAO_VALOR_ADICIONADO'
        }
        documents_list = list(documents_sheet.keys()) # REFERENCIA DOS DOCUMENTOS

        try:
            reports = self.cvm_http_client.get_report( # EXTRACAO DO RELATORIO
                doc["numero_seq_documento"], doc["codigo_tipo_instituicao"], reports_list = documents_list
            )
            key_list = list(reports.keys()) # LISTA DE CHAVES NO DOCIONARIO
        except Exception as exc:
            key_list = [] # NAO HA DADO ESTRUTURADO

        if len(key_list) > 0: # CASO POSITIVO
            reps = pandas.DataFrame()
            for relatorio in documents_list:
                df = format_2_df(reports[relatorio]) # FORMATA O RELATORIO
                df["structure"] = documents_sheet[relatorio]
                reps = pandas.concat([reps, df], axis = 0) # CONCATENA
            return reps
        else: return pandas.DataFrame() # CASO NEGATIVO

    def get_reports(self, docs:pandas.DataFrame):
        """ EXTRACAO CONTINUA DE DATAFRAMES DE RELATORIOS CVM

            DOCS        [PANDAS.DATAFRAME] - BASE DE DADOS PADRAO CVM DE EXTRACAO DE RELATORIOS
        """
        df = pandas.DataFrame()
        dc = docs.copy()
        for _, row in dc.iterrows(): # AVALIA CADA DOCUMENTO
            aux = CVM().get_report(doc = row)
            if not aux.empty:
                aux["cvm_code"] = row.id.split("_")[0]
                aux["document"] = row.id.split("_")[1]
                aux["date_rel"] = row.id.split("_")[2].replace("DR","")
                aux["date_ref"] = row.id.split("_")[3].replace("DE","")
                aux["versions"] = row.id.split("_")[4].replace("vs","")
                aux["protocol"] = row.id.split("_")[5]
                df = pandas.concat([df, aux], axis = 0)
        return df.reset_index(drop = True)
    
if __name__ == "__main__":
    pass