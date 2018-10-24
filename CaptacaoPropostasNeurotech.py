import logging
from logging.handlers import TimedRotatingFileHandler
import requests
# import json
import pandas as pd
# from pprint import pprint
import datetime
import time
import pyodbc
import textwrap
import sys
import cx_Oracle

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# cria o handler de arquivos
handler = TimedRotatingFileHandler("LOG_CAPTACAO.txt", when="d", interval=1, backupCount=7)
handler.setLevel(logging.INFO)

# cria o formatador do logger
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

logger.info('Logger iniciado.')


def definirPeriodoConsulta():
    data = datetime.datetime.now()
    hora = data.hour

    if hora >= 0 and hora < 8:
        logger.info("Não há operação de loja no horário da execução.", hora)
        sys.exit()

    horaInicio = " {0:02d}:00:00.000".format(hora - 1)
    horaFim = " {0:02d}:59:59.999".format(hora - 1)

    dataInicio = data.strftime("%d/%m/%Y") + horaInicio
    dataFim = data.strftime("%d/%m/%Y") + horaFim

    #dataInicio = '08/10/2018 03:00:00.000'
    #dataFim = '08/10/2018 03:59:59.999'

    logger.info('Período da consulta entre %s e %s', dataInicio, dataFim)

    return dataInicio, dataFim


def obterPropostas(dataInicio, dataFim):
    urlPropostas = "https://dr-historico-api.neurotech.com.br/PRD/listarPropostas"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    requestPropostas = {
        "credenciais": {"codigoAssociado": 46, "senha": "abcd@1234"},
        # caso seja necessário recuperar as propostas de uma política específica, adicionar a tag abaixo
        # "nomePolitica": "AVANTICARD_P1",
        "instanteInicio": dataInicio,
        "instanteFim": dataFim}

    try:

        while True:

            # iterador para repetir as tentativas de chamadas à API quando der timeout
            i = 1

            t0 = time.time()
            obj = requests.post(urlPropostas, json=requestPropostas, headers=headers)
            logger.info("Tempo para consultar cabeçalho das propostas: %s segundos.", time.time() - t0)

            dados = obj.json()

            if 'errorMessage' in dados:

                logger.info(dados['errorMessage'])

                if i > 5:
                    logger.error(dados['errorMessage'])
                    raise Exception(dados['errorMessage'])

                logger.info("Iniciando nova tentativa de chamada à API (listarPropostas).")
                i += 1
                continue

            logger.info('A API retornou %s propostas.', dados["quantidadeTotal"])

            if dados["quantidadeTotal"] > 0:
                i = 0
                listaPropostas = []

                while i < len(dados["propostas"]):
                    listaPropostas.append(
                        {
                            'codigoOperacao': dados["propostas"][i].get("codigoOperacao"),
                            'codigoProposta': dados["propostas"][i].get("codigoProposta"),
                            'instante': dados["propostas"][i].get("instante"),
                            'instanteFim': dados["propostas"][i].get("instanteFim"),
                            'nomePolitica': dados["propostas"][i].get("nomePolitica"),
                            'resultado': dados["propostas"][i].get("resultado")
                        })

                    i = i + 1
            else:
                logger.info("Não houveram propostas no período solicitado.")

                return pd.DataFrame()

            # with open('cabecalho.json', 'a') as out:
            #    pprint(obj.json(), stream=out)

            dfPropostas = pd.DataFrame(listaPropostas)

            return dfPropostas

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def obterVariaveisPropostas(dfPropostas, qtdPropostasAPI=20):
    try:

        # lista de códigos que serão enviadas no request à API
        listaCodigos = []

        # lista contendo as variáveis retornadas na API
        listaVariaveis = []

        # novo DataFrame com os dados retornados na API de variáveis da proposta
        dfVariaveisPropostas = pd.DataFrame()

        # quantidade de propostas no DataFrame
        qtdPropostas = dfPropostas.count(0).iloc[1]

        logger.info("Quantidade de propostas no período: %s.", qtdPropostas)

        urlVariaveis = "https://dr-historico-api.neurotech.com.br/PRD/listarVariaveisProposta"

        while qtdPropostas > 0:

            # recupera as primeiras "qtdPropostasAPI" do DataFrame de propostas
            df = dfPropostas[:qtdPropostasAPI]

            # armazena os índices recuperados do DataFrame original
            listaIndex = df.index

            # cria a lista com os 'codigoOperacao' que serão enviados à API de variáveis da proposta
            listaCodigos = df['codigoOperacao'].tolist()

            requestVariaveis = {
                "credenciais": {"codigoAssociado": 46, "senha": "abcd@1234"},
                "codigoOperacao": listaCodigos}
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

            while True:

                # iterador para repetir as tentativas de chamadas à API quando der timeout
                i = 1

                t0 = time.time()
                obj = requests.post(urlVariaveis, json=requestVariaveis, headers=headers)
                logger.info("Tempo para consultar variáveis de %s propostas: %s segundos. Restam %s propostas.",
                            qtdPropostasAPI, time.time() - t0, qtdPropostas - qtdPropostasAPI)

                # with open('variaveis.json', 'a') as out:
                #    pprint(obj.json(), stream=out)

                listaEntradas = []

                # t0 = time.time()

                dados = obj.json()

                if 'errorMessage' in dados:

                    logger.info(dados['errorMessage'])

                    if i > 5:
                        logger.error(dados['errorMessage'])
                        raise Exception(dados['errorMessage'])

                    logger.info("Iniciando nova tentativa de chamada à API (listarVariaveisProposta).")
                    i += 1
                    continue

                for proposta in dados["propostas"]:

                    # armazena lista de dicionários contendo as variáveis de entrada
                    listaEntradas = proposta["proposta"]["entradas"]

                    # para cada dicionário na lista
                    for i in range(0, len(listaEntradas) - 1):

                        # obtém o dicionário
                        dicEntradas = dict(listaEntradas[i])

                        # procura as chaves CPF do Titular e código da loja
                        for chave, valor in dicEntradas.items():
                            if chave == "PROP_CPF_TITULAR":
                                numCpf = valor
                            elif chave == "PROP_COD_LOJA":
                                dscLoja = valor
                            else:
                                continue

                        i += 1

                    logger.debug('LOJA: %s - CPF: %s ', dscLoja, numCpf)

                    listaVariaveis.append({
                        'codigoOperacao': proposta.get('codigoOperacao'),
                        'CPFTitular': numCpf,
                        'codigoLoja': dscLoja,
                    })

                # cria novo DataFrame com as variáveis retornadas
                dfParcial = pd.DataFrame(listaVariaveis)

                # "inner join" com o DataFrame original e o de variáveis
                dfParcial = dfPropostas.merge(dfParcial, on='codigoOperacao', how='inner')

                # concatena (append) os DataFrames de variáveis
                dfVariaveisPropostas = pd.concat([dfVariaveisPropostas, dfParcial])

                dfPropostas = dfPropostas.drop(listaIndex)
                qtdPropostas = dfPropostas.count(0).iloc[1]

                break

            # print("Tempo para percorrer as primeiras 20 propostas: {0:0.2f}".format(time.time()-t0))

        return dfVariaveisPropostas

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def gerarArquivoPropostas(dfVariaveisPropostas, diretorioArquivo):
    try:
        data = datetime.datetime.now()
        dataArquivo = data.strftime("%Y%m%d_%H")

        arquivo = diretorioArquivo + '\propostasNeurotech' + dataArquivo + '.csv'
        with open(arquivo, 'w') as out:
            dfVariaveisPropostas.to_csv(out, header=True, sep=';', index=False)

        logger.info('Arquivo "%s" gerado.', arquivo)
        return arquivo

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def gerarParcialCaptacoes(nomeArquivo):
    try:

        logger.info("Início da geração da parcial de cartões.")
        arquivoParciais = pd.read_csv(nomeArquivo, sep=";")
        arquivoParciais = arquivoParciais[arquivoParciais["nomePolitica"] == "AVANTICARD_P1"]

        # se não há nenhuma proposta da política P1, encerra o processo.
        if arquivoParciais.count()[0] == 0:
            return False

        logger.info("Removidas as propostas da política P2. Quantidade de propostas da política P1: %s",
                    arquivoParciais.count()[0])

        arquivoParciais = arquivoParciais.drop(
            ['codigoOperacao', 'codigoProposta', 'instante', 'instanteFim', 'nomePolitica', 'resultado'], axis=1)

        logger.info('Quantidade de propostas da política P1 após remoção dos CPFs duplicados: %s',
                    arquivoParciais.count()[0])

        arquivoParciais = arquivoParciais.drop_duplicates('CPFTitular', keep='first')

        arquivoParciais = arquivoParciais.reset_index(drop=True)

        df = arquivoParciais.groupby('codigoLoja')
        lista = list(df.groups.keys())

        dfFiliais = obterNomesFiliais(lista)
        # Remove filiais duplicadas no DataFrame de/para
        dfFiliais = dfFiliais.drop_duplicates(keep='first')
        arquivoParciais = arquivoParciais.merge(dfFiliais, left_on='codigoLoja', right_on='AVANTI', how='inner')
        arquivoParciais = arquivoParciais.drop(['codigoLoja', 'AVANTI'], axis=1)
        arquivoParciais = arquivoParciais.groupby('FILIAL', as_index=False).count().reset_index(drop=True)

        df = obterDadosLoja()
        # Substitui o Ç da filial "Jaçanã"
        arquivoParciais['FILIAL'] = arquivoParciais['FILIAL'].map(lambda x: 'JACANA' if x == 'JAÇANA' else x)
        arquivoParciais = arquivoParciais.merge(df, left_on='FILIAL', right_on='NOME', how='inner')
        arquivoParciais = arquivoParciais.drop(['NOME'], axis=1)

        inserirParcialCaptacoes(arquivoParciais)

        return True

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def connODBC(bd):
    try:
        if bd == 'sqlserver':
            # produção
            return pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=172.16.6.57;DATABASE=CAEDU_BI;UID=LGN_BI;PWD=H6*jE@ª5fq87')

            # homologação
            #return pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=172.16.25.15;DATABASE=CAEDU_BI;UID=LGN_BI;PWD=H6*jE@ª5fq87')

        elif bd == 'oracle':
            # produção
            return cx_Oracle.connect('caedu/caedu@172.16.6.55/PDVPALMA')

            # homologação
            #return cx_Oracle.connect('caedu/caedu@oracledbdev.caedu.com.br/CAEDUDEV')

        else:
            sys.exit('Não existe conexão para este banco de dados.')
    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def obterNomesFiliais(listaFiliais):
    try:

        qmarks = ','.join('?' * len(listaFiliais))
        sql = 'SELECT FILIAL, AVANTI FROM DIM_DEPARA WHERE AVANTI IN ({})'.format(qmarks)

        conn = connODBC('sqlserver')
        cursor = conn.cursor()
        cursor.execute(sql, listaFiliais)

        rows = cursor.fetchall()

        lista = []

        for row in rows:
            lista.append(
                {
                    'FILIAL': row.FILIAL,
                    'AVANTI': row.AVANTI
                }
            )

        return pd.DataFrame(lista)

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def obterDadosLoja():
    try:
        conn = connODBC('oracle')
        cursor = conn.cursor()
        cursor.execute("SELECT ID, NOME FROM loja")
        rows = cursor.fetchall()
        cursor.close()

        lista = []

        for row in rows:
            lista.append(
                {
                    'CODIGO': row[0],
                    'NOME': row[1]
                }
            )

        return pd.DataFrame(lista)

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def inserirParcialCaptacoes(arquivoParciais):
    try:

        conn = connODBC('oracle')
        cursor = conn.cursor()

        listaParams = []

        sqlInsert = textwrap.dedent("""
                INSERT INTO TEMP_CAPTACAO_PROPOSTAS (
                  ID_LOJA
                  ,DT_CARGA
                  ,CREATE_DATE
                  ,UPDATE_DATE
                  ,QTD_CPF_CONSULTADO)
                VALUES (
                  :1
                  ,:2
                  ,SYSDATE
                  ,SYSDATE
                  ,:3
                )""")

        listaParams = []

        data = datetime.datetime.now()
        data = data.date()

        for index, row in arquivoParciais.iterrows():
            listaParams.append(
                (row.CODIGO
                 , data
                 , row.CPFTitular
                 ))

        cursor.bindarraysize = len(listaParams)
        cursor.setinputsizes(int, cx_Oracle.DATETIME, int)
        cursor.executemany(sqlInsert, listaParams)

        logger.info('Executado o insert na tabela TEMP_CAPTACAO_PROPOSTAS.')

        sqlMerge = textwrap.dedent("""
                MERGE INTO ppp_base_captacao T1
                USING (
                  SELECT
                    ID_LOJA
                    ,DT_CARGA
                    ,QTD_CPF_CONSULTADO
                    ,CREATE_DATE
                    ,UPDATE_DATE
                  FROM TEMP_CAPTACAO_PROPOSTAS) T2
                ON (
                  T1.LOJA_ID = T2.ID_LOJA
                  AND T1.DT_CARGA = T2.DT_CARGA)
                WHEN MATCHED THEN
                  UPDATE SET
                     T1.QND_CPF_CONSULTADOS = T1.QND_CPF_CONSULTADOS + T2.QTD_CPF_CONSULTADO
                    ,T1.CREATE_DATE = T2.CREATE_DATE
                    ,T1.UPDATE_DATE = T2.UPDATE_DATE

                WHEN NOT MATCHED THEN
                  INSERT (
                     ID
                    ,LOJA_ID
                    ,DT_CARGA
                    ,QND_CPF_CONSULTADOS
                    ,CREATE_DATE
                    ,UPDATE_DATE)
                  VALUES (
                    SQS_BASE_CAPTACAO.nextval
                    ,T2.ID_LOJA
                    ,T2.DT_CARGA
                    ,t2.QTD_CPF_CONSULTADO
                    ,T2.CREATE_DATE
                    ,T2.UPDATE_DATE)""")

        cursor.execute(sqlMerge)
        conn.commit()
        cursor.close()

        logger.info(
            'Executado o comando merge para inserção/atualização da quantidade de propostas na tabela ppp_base_captacao.')

        return True

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        raise


def main(diretorioArquivo):
    try:

        dataInicio, dataFim = definirPeriodoConsulta()
        dfPropostas = obterPropostas(dataInicio, dataFim)

        if not dfPropostas.empty:

            dfVariaveisPropostas = obterVariaveisPropostas(dfPropostas=dfPropostas)

            resultado = gerarArquivoPropostas(dfVariaveisPropostas, diretorioArquivo)

            logger.info("Arquivo de captações gerado com sucesso!")

            if (gerarParcialCaptacoes(resultado)):
                logger.info("Parcial de captação de cartões gravada no banco de dados com sucesso!")
                sys.exit()
            else:
                logger.info("Não haviam propostas da política P1. Processo concluído.")
                sys.exit()

        else:
            logger.info("Não houveram propostas no período.")
            sys.exit()

    except Exception as e:
        logger.error('Erro na execução.', exc_info=True)
        sys.exit("Error.")
        raise


if __name__ == "__main__":
    logger.info('Início da execução.')
    main(diretorioArquivo=sys.argv[1])
