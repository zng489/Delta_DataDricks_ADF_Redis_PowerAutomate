# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

import os
import requests, zipfile
import shutil
import pandas as pd
import glob
import subprocess
from threading import Timer
import shlex
import logging
import json
from core.bot import log_status
from core.adls import upload_file
import pyspark.sql.functions as f

import re
import shlex
import time
from time import strftime
from datetime import datetime
from datetime import datetime, date
from unicodedata import normalize
from collections import namedtuple
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
import warnings
warnings.simplefilter('ignore')

# COMMAND ----------

params = json.loads(re.sub("\'", '\"', dbutils.widgets.get("params")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# READING FILES IN DATALAKE
# buscaArquivoDeConfiguracao


def buscaArquivoDeConfiguracao(dls, tmp, LND):
  area_ = 'false'
  try:
    '''
    file = "API_TOKEN.csv"
    file_path = LND + "/trello_oni/config/" + file
    # Download the file to a local temporary directory
    with dls.get_file_client(f"{file_path}") as file_client:
      with open(file=os.path.join(tmp, file), mode="wb") as local_file:
        download = file_client.download_file()
        local_file.write(download.readall())
      print(f"{file} downloaded from Azure Data Lake Storage.")
    area_ = pd.read_csv(tmp + file, delimiter=";")
    '''
    var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    path = '{uri}/uds/uniepro/oni/trello/API_TOKEN.csv'.format(uri=var_adls_uri)
    area_ = spark.read.format("csv").option("header","true").option("sep", ";").load(path)
    area_ = area_.toPandas()
  except Exception as e:
    #print(f"Error while downloading {file}: {e}")
    return None
  return area_


def __to_parquet(df, parquet_output):
  parquet_output = parquet_output + '.parquet'
  try:
    df.to_parquet(parquet_output, index=False)
  except Exception as e:
    print('parquet_output: {0} \n {1}'.format(parquet_output, e))
    if os.path.exists(parquet_output):
      os.remove(parquet_output)


def __escreve_arquivo(LND, adl, df, output):
  # __to_csv(df, output)
  __to_parquet(df, output)
  '''TEMPORARY
  # UPLOAD
  try:
    parquet_output = output + '.parquet'
    __upload_files(LND, adl, parquet_output)
    #print('SUCESSO: __upload_files: {0}'.format(parquet_output))
    logging.info('SUCESSO: __upload_files: {0}'.format(parquet_output))
  except Exception as e:
    #print('ERROR: __upload_files: {0} - LOG: {1}'.format(parquet_output, e))
    logging.info('ERROR: __upload_files: {0} - LOG: {1}'.format(parquet_output, e))
  '''


# READING FILES FROM OUT
# buscaArquivoDeQuadros não é mais utilizado
def buscaArquivoDeQuadros():
  area_ = 'false'
  try:
    file = "quadros.csv"
    '''TEMPORARY
    file_path = LND + "/trelloi/config/" + file
    # Download the file to a local temporary directory
    with dls.get_file_client(f"{file_path}") as file_client:
        with open(file=os.path.join(tmp, file), mode="wb") as local_file:
            download = file_client.download_file()
            local_file.write(download.readall())
        print(f"{file} downloaded from Azure Data Lake Storage.")
    '''
    ############### IT'LL DELETED    
    #tmp = 'C:/Users/zhang.yuan/Desktop/bot_trello/'
    var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    path = '{uri}/uds/uniepro/trello/trello/config/quadros.csv'.format(uri=var_adls_uri)
    df = spark.read.format("csv").option("header","false").load(path)
    df = df.toPandas()
    df = df[['_c1','_c4']].rename(columns={"_c1": 1, "_c4": 4})
    area_ = df

    #area_ = pd.read_csv(tmp + file, delimiter=",", header=None, usecols=[1,4])
    area_.columns = ['id', 'data']
    #print(tmp + file)

  except Exception as e:
      #print(f"Error while downloading {file}: {e}")
      return None
      
  return area_


def limpaUnicode(df, campo):
  df[campo] = df[campo].str.replace("\u2705", "")
  df[campo] = df[campo].str.replace("\u2726", "")
  df[campo] = df[campo].str.replace("\U0001f525", "")
  df[campo] = df[campo].str.replace("\U0001f680", "")
  df[campo] = df[campo].str.replace("\U0001f44d", "")
  df[campo] = df[campo].str.replace("\u201c", "")
  df[campo] = df[campo].str.replace("\u201d", "")
  df[campo] = df[campo].str.replace("\U0001f446", "")
  df[campo] = df[campo].str.replace("\u2013", "")
  df[campo] = df[campo].str.replace("\u2022", "")
  df[campo] = df[campo].str.replace("\uf0a7", "")
  df[campo] = df[campo].str.replace("\u0301", "")
  df[campo] = df[campo].str.replace("\u0327", "")
  df[campo] = df[campo].str.replace("\u0303", "")
  
  return df

# COMMAND ----------

# Busca idMember para consultas
def get_member_token(key, token, area_):

  url_api_board = "https://api.trello.com/1/members/me?key={0}&token={1}";

  t = requests.Session()
  response = t.get(url_api_board.format(key, token), verify=False)
  df1 = pd.DataFrame()

  try:
    dados = response.json()
    df = pd.json_normalize(dados)
    df['area'] = area_
    df1 = df[['area', 'idEnterprise', 'idOrganizations', 'id', 'idMemberReferrer', 'username', 'fullName', 'initials', 'email', 'idBoards']]

    df1 = df1.astype({'idMemberReferrer': 'string', 'idBoards': 'string', 'idOrganizations': 'string'})
    df1 = df1.rename(columns={'id': 'idMember'})
  except Exception as e:
    #print('get_member_token: area:{0} - ERROR: {1}'.format(area_, e))
    logging.info('get_member_token: area:{0} - ERROR: {1}'.format(area_, e))
    
  return df1


# LISTA DE BOARDS
def detalhe_lista_boards_por_members(key, token, idMembers, area_):
  url_api_board = "https://api.trello.com/1/members/{0}/boards?key={1}&token={2}&cards=all";
  t = requests.Session()
  df1 = pd.DataFrame()

  try:
    response = t.get(url_api_board.format(idMembers, key, token), verify=False)
    ###print('url_api_board')
    ###print(url_api_board.format(idMembers, key, token))
    ###print(f'idMembers ==> {idMembers}')
    # 609ed748b564b126a65389fe
    # response.encoding = 'ISO-8859-1'
    dados = response.json()
    df = pd.json_normalize(dados)
    df = limpaUnicode(df, 'name')
    df = limpaUnicode(df, 'desc')
    df['area'] = area_

    df1 = df[
      ['area', 'id', 'name', 'desc', 'dateLastActivity', 'starred', 'url', 'shortUrl', 'shortLink', 'idMemberCreator', 'idOrganization',
      'idEnterprise', 'closed', 'labelNames.green', 'labelNames.yellow', 'labelNames.orange', 'labelNames.red', 'labelNames.purple',
      'labelNames.blue', 'labelNames.sky', 'labelNames.lime', 'labelNames.pink', 'labelNames.black', 'labelNames.green_dark',
      'labelNames.yellow_dark', 'labelNames.orange_dark', 'labelNames.red_dark', 'labelNames.purple_dark', 'labelNames.blue_dark',
      'labelNames.sky_dark', 'labelNames.lime_dark', 'labelNames.pink_dark', 'labelNames.black_dark', 'labelNames.green_light',
      'labelNames.yellow_light', 'labelNames.orange_light', 'labelNames.red_light', 'labelNames.purple_light', 'labelNames.blue_light',
      'labelNames.sky_light', 'labelNames.lime_light', 'labelNames.pink_light', 'labelNames.black_light']]

    df1 = df1.astype({'idEnterprise':'string'})
    df1 = df1.rename(columns={'id': 'idBoard'})
  except Exception as e:
    #print('detalhe_lista_boards_por_members: ERROR: {1}'.format(e))
    logging.info('detalhe_lista_boards_por_members:  ERROR: {1}'.format(e))

  return df1


# PARA CADA BOARD BUSCA DOS CARDS
def detalhe_organizations(key, token, idBoard, idOrganizations, area_):
  url_api_board = "https://api.trello.com/1/organizations/{0}?key={1}&token={2}&cards=all";
  t = requests.Session()
  response = t.get(url_api_board.format(idOrganizations, key, token), verify=False)
  #print('PARA CADA BOARD BUSCA DOS CARDS')
  #print((url_api_board.format(idOrganizations, key, token)))
  df1 = pd.DataFrame()
  try:
    dados = response.json()
    df = pd.json_normalize(dados)
    df['area'] = area_
    df['idBoard'] = idBoard
    df1 = df[['area', 'idBoard', 'id', 'name', 'displayName', 'website', 'teamType', 'desc', 'url', 'logoHash',
            'logoUrl']]

    df1 = df1.astype({'logoHash': 'string', 'logoUrl': 'string'})
    df1 = df1.rename(columns={'id': 'idOrganizations'})
  except Exception as e:
    #print('detalhe_organizations: idOrganizations:{0} - ERROR: {1}'.format(idOrganizations, e))
    logging.info('detalhe_organizations: idOrganizations:{0} - ERROR: {1}'.format(idOrganizations, e))
  return df1

# quadros = buscaArquivoDeQuadros(authenticate_datalake(), tmp, LND)
# dados = dados.merge(quadros, left_on='idBoard', right_on='id', how='left')
# dados = dados[dados['dateLastActivity'] != dados['data']]

# somente os dados fornecidos pela LISTA DE BOARDS já é o suficiente para 
# suprir as necessidades do crawler, não há necessidade de obter o quadros.csv


# MEMBERS OF BOARDS
def detalhe_board_members(key, token, idBoard, area_):
  url_api = "https://api.trello.com/1/boards/{2}/members?fields=id,idMemberReferrer,idEnterprise,username,activityBlocked,avatarHash,fullName,initials,nonPublicAvailable&key={0}&token={1}";
  t = requests.Session()
  response = t.get(url_api.format(key, token, idBoard), verify=False)
  #print(url_api.format(key, token, idBoard))
  #print(f'MEMBERS OF BOARDS idBoard ==> {idBoard}')
  df1 = pd.DataFrame()
  try:
    dados = response.json()
    df = pd.json_normalize(dados)
    df['area'] = area_
    df['idBoard'] = idBoard
    df1 = df
    df1 = df1.rename(columns={'id': 'idMember'})
  except Exception as e:
    #print('detalhe_board_members: {0}'.format(e))
    logging.info('detalhe_board_members: {0}'.format(e))
  return df1



# ACTIONS
def detalhe_board_actions(key, token, idBoard, area_, dt_inicio, dt_final):

  url_api = "https://api.trello.com/1/boards/{2}/actions?filter={3}&fields=id,idMemberCreator,type,date,data&key={0}&token={1}&limit=1000&since=" + dt_inicio + "&before=" + dt_final;

  # COMMENTCARD
  df1 = pd.DataFrame()
  t = requests.Session()
  response = t.get(url_api.format(key, token, idBoard, 'commentCard'), verify=False)

  try:
    dados = response.json()
    df = pd.json_normalize(dados)

    df = df.rename(columns={'data.text': 'data_text'})
    df = df.rename(columns={'data.card.id': 'idCard'})
    df = df.rename(columns={'data.board.id': 'idBoard'})
    df = df.rename(columns={'data.dateLastEdited': 'data_dateLastEdited'})
    df = df.rename(columns={'id': 'idAction'})

    df['area'] = area_
    df['card_old_due'] = ''
    df['card_due'] = ''

    df1 = df

    df1 = limpaUnicode(df1, 'data_text')

    df1 = df1[
        ['area', 'idBoard', 'idCard', 'idAction', 'idMemberCreator', 'type', 'date', 'data_text', 'card_old_due',
        'card_due', 'data_dateLastEdited']]

  except Exception as e:
    #print('detalhe_board_cards_actions:commentCard: {0}'.format(e))
    logging.info('detalhe_board_cards_actions:commentCard: {0}'.format(e))

  # UPDATECARD:DUE
  df2 = pd.DataFrame()
  t2 = requests.Session()
  response2 = t2.get(url_api.format(key, token, idBoard, 'updateCard:due'), verify=False)

  try:
    dados2 = response2.json()
    df_2 = pd.json_normalize(dados2)

    df_2 = df_2.rename(columns={'data.text': 'data_text'})
    df_2 = df_2.rename(columns={'data.card.id': 'idCard'})
    df_2 = df_2.rename(columns={'data.board.id': 'idBoard'})
    df_2 = df_2.rename(columns={'data.dateLastEdited': 'data_dateLastEdited'})
    df_2 = df_2.rename(columns={'id': 'idAction'})

    # OLD
    df_2 = df_2.rename(columns={'data.old.due': 'card_old_due'})
    df_2 = df_2.rename(columns={'data.card.due': 'card_due'})

    df_2['area'] = area_
    df_2['data_text'] = '-'
    df_2['data_dateLastEdited'] = ''

    df2 = df_2

    df2 = df2[
      ['area', 'idBoard', 'idCard', 'idAction', 'idMemberCreator', 'type', 'date', 'data_text', 'card_old_due',
       'card_due', 'data_dateLastEdited']]

  except Exception as e:
    #print('detalhe_board_cards_actions:updateCard:due: {0}'.format(e))
    logging.info('detalhe_board_cards_actions:updateCard:due: {0}'.format(e))

  frames = [df1, df2]
  result = pd.concat(frames)
  return result  



# BUSCA CARDS PARA O BOARD
def detalhe_board_cards(key, token, shortLink_board, idBoard, area_):
  url_api_board = "https://api.trello.com/1/boards/{0}/Cards?key={1}&token={2}&cards=all";
  t = requests.Session()
  response = t.get(url_api_board.format(shortLink_board, key, token), verify=False)
  df1 = pd.DataFrame()
  try:
    dados = response.json()
    if len(dados) > 0:
      df = pd.json_normalize(dados)
      df = limpaUnicode(df, 'name')
      df = limpaUnicode(df, 'desc')
      df['area'] = area_

      df1 = df[['area',
              'idBoard',
              'id',
              'idMembers',
              'idList',
              'idChecklists',
              'idLabels',
              'name',
              'desc',
              'start',
              'closed',
              'dateLastActivity',
              'due',
              'url',
              'labels',
              'pos'
              ]].astype(str)
      df1 = df1.rename(columns={'id': 'idCard'})
    else:
      '''print('detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board,'BOARD SEM CARDS'))'''
      logging.info('detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board,'BOARD SEM CARDS'))
  except Exception as e:
    '''print(
      'detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board, e))'''
    logging.info('detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board, e))
    
  return df1



# CUSTOMFIELDS
def detalhe_card_customfields(key, token, idCard, area_):
  url_api_board = "https://api.trello.com/1/cards/{0}/customFieldItems?key={1}&token={2}";

  t = requests.Session()
  response = t.get(url_api_board.format(idCard, key, token), verify=False)
  df1 = pd.DataFrame()

  try:
    dados = response.json()
    df = pd.json_normalize(dados)
    df['idCard'] = idCard
    df['area'] = area_

    df1 = df
    df1 = df1.rename(columns={'value.text': 'value_text'})
    df1 = df1.rename(columns={'value.date': 'value_date'})
  except Exception as e:
    #print('detalhe_card_customfields: Card: {0} - ERRO:{1}'.format(idCard, e))
    logging.info('detalhe_card_customfields: Card: {0} - ERRO:{1}'.format(idCard, e))

  return df1


def detalhe_customfields(key, token, idCard, idCustomField, area_):
  url_api_board = "https://api.trello.com/1/customFields/{0}?key={1}&token={2}";

  retry = Retry(connect=3, backoff_factor=0.5)
  adapter = HTTPAdapter(max_retries=retry)

  t = requests.Session()

  t.mount('http://', adapter)
  t.mount('https://', adapter)

  response = t.get(url_api_board.format(idCustomField, key, token), verify=False)
  df1 = pd.DataFrame()

  try:
    dados = response.json()
    df = pd.json_normalize(dados)
    df['idCard'] = idCard
    df['area'] = area_
    df1 = df
  except Exception as e:
      #print('detalhe_customfields: Card: {0} - CustomField: {1}  - ERRO:{2}'.format(idCard, idCustomField, e))
      logging.info('detalhe_customfields: Card: {0} - CustomField: {1}  - ERRO:{2}'.format(idCard, idCustomField, e))



def detalhe_board_cards_lists(key, token, idBoard, idCard, area_):
    url_api_board = "https://api.trello.com/1/cards/{0}/list?key={1}&token={2}";
    t = requests.Session()
    response = t.get(url_api_board.format(idCard, key, token), verify=False)

    df1 = pd.DataFrame()
    try:
        dados = response.json()
        if len(dados) > 0:
            df = pd.json_normalize(dados)
            df['idCard'] = idCard
            df['area'] = area_

            df1 = df[['area',
                      'idBoard',
                      'pos',
                      'id',
                      'closed',
                      'name',
                      'idCard'
                      ]].astype(str)
            df1 = df1.rename(columns={'id': 'idList'})
        else:
            #print('detalhe_board_cards_lists: idBoard:{0}  -  ERROR: {1}'.format(idBoard, 'LISTA LIMPA'))
            logging.info('detalhe_board_cards_lists: idBoard:{0}  -  ERROR: {1}'.format(idBoard, 'LISTA LIMPA'))

    except Exception as e:
        #print('detalhe_board_cards_lists: idBoard:{0} - ERROR: {1}'.format(idBoard, e))
        logging.info('detalhe_board_cards_lists: idBoard:{0} - ERROR: {1}'.format(idBoard, e))

    return df1
  

def detalhe_board_cards_checklist(key, token, idChecklist, idBoard, idCard, area_):
  url_api = "https://api.trello.com/1/checklists/{2}?key={0}&token={1}";
  t = requests.Session()
  response = t.get(url_api.format(key, token, idChecklist), verify=False)

  try:
      dados = response.json()
      df1 = pd.DataFrame()
      if len(dados) > 0:
          # NOME DA LISTA
          dfLista = pd.json_normalize(dados)
          nomeLista = ''

          try:
              nomeLista = str(dfLista['name'][0])
          except Exception as e:
              nomeLista = ''

          # DADOS DO CHECKLIST
          df = pd.json_normalize(dados, 'checkItems')
          if df.empty:
              print('sem resultados')
          else:
              df = limpaUnicode(df, 'name')
              df['idBoard'] = idBoard
              df['idCard'] = idCard
              df['nameList'] = nomeLista
              df['area'] = area_

              df1 = df[
                  ['area', 'idChecklist', 'id', 'state', 'name', 'due', 'pos', 'idMember', 'idBoard', 'idCard',
                    'nameList']].astype(str)

              # AJUSTA COLUNAS
              CHECKLIST_ = pd.DataFrame(columns=[
                  "area",
                  "idBoard",
                  "idCard",
                  "idMember",
                  "idChecklist",
                  "nameList",
                  "id",
                  "state",
                  "name",
                  "due",
                  "pos"
              ])

              for check in df1.itertuples():
                  CHECKLIST_ = CHECKLIST_.append(
                      {
                          "area": check.area,
                          "idBoard": check.idBoard,
                          "idCard": check.idCard,
                          "idMember": check.idMember,
                          "idChecklist": check.idChecklist,
                          "nameList": nomeLista,
                          "id": check.id,
                          "state": check.state,
                          "name": check.name,
                          "due": check.due,
                          "pos": check.pos
                      }, ignore_index=True)

              df1 = CHECKLIST_

  except Exception as e:
      '''print('detalhe_board_cards_checklist: idBoard:{0} - idChecklist :{1} -ERROR: {2} -OBJ: {3}\n'.format(idBoard,
                                                                                                            idChecklist,
                                                                                                            e, df))'''
      logging.info(
          'detalhe_board_cards_checklist: idBoard:{0} - idChecklist :{1} -ERROR: {2}\n'.format(idBoard, idChecklist,
                                                                                                e))

  return df1 

# COMMAND ----------

AREA= 'UNIEPRO'
CHAVE='890f4ae4256649cc46abc7a07ccec631'
TOKEN='9cdcdff3bb81109f050445d3f438692e716bd754f5aced0a52f5f90cf8818e5e'
USUARIO='observatorionacional.trello@cni.com.br'

'''TEMPORARY
bot=kwargs.get('bot')
LND=kwargs.get('LND')
adl=kwargs.get('adl')
tmp=kwargs.get('tmp')
'''

#print('AREA:{0} - TOKEN:{1} - CHAVE:{2} - USUARIO:{3}'.format(kwargs.get('AREA'), kwargs.get('TOKEN'), kwargs.get('CHAVE'), kwargs.get('USUARIO')))
#key = kwargs.get('CHAVE')
#token = kwargs.get('TOKEN')
#idMember = kwargs.get('USUARIO')

area = AREA
key = CHAVE
token = TOKEN
idMember = USUARIO
area = AREA
area_ = AREA

# Busca idMember para consultas
# get_member_token()
############### IT'LL DELETED 
#print('TESTING')

# Busca idMember para consultas
membro = get_member_token(key, token, area)
idMember = membro['idMember'][0]
# '609ed748b564b126a65389fe'

tmp = 'trello/'
# cria os diretorio para armazenar os arquivos
tmp_list = tmp + 'trello__list'
tmp_checklist = tmp + 'trello__checklist'
tmp_log = tmp + 'trello'

os.makedirs(tmp, mode=0o777, exist_ok=True)
os.makedirs(tmp_checklist, mode=0o777, exist_ok=True)
os.makedirs(tmp_list, mode=0o777, exist_ok=True)
os.makedirs(tmp_log, mode=0o777, exist_ok=True)


#logging.basicConfig(filename='/tmp/oni/trello/trello/log_trello.log', level=logging.INFO, filemode='w')
#logging.info('Iniciando Processo: ' + str(datetime.today()))

# remove subdiretorio
# tmp_organizations = tmp
# tmp_board = tmp
# tmp_card = tmp
# tmp_member = tmp
# tmp_actions = tmp
# tmp_list = tmp
# tmp_customfields = tmp
# tmp_checklist = tmp

output = "{file}.parquet"

# OBJETOS DE APOIO
LISTA_CUSTOMFIELDS = pd.DataFrame(columns=[
    "area",
    "idBoard",
    "idCards",
    "idCustomField",
    "name",
    "type",
    "value"
])

LISTA_LIST = pd.DataFrame(columns=[
    "area",
    "idBoard",
    "idCard",
    "idList",
    "pos",
    "closed",
    "name"
])

LISTA_CHECKLIST_IN_CARDS = pd.DataFrame(columns=[
    "area",
    "idBoard",
    "idCard",
    "idMember",
    "idChecklist",
    "nameList",
    "id",
    "state",
    "name",
    "due",
    "pos"
])


# ENTERPRISE
dados_enterprise = membro
filename = 'enterprise_' + area
#dados_enterprise.to_parquet(tmp_enterprise + output.format(file=filename))
#dados_enterprise_ = spark.createDataFrame(dados_enterprise)
#dados_enterprise_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_enterprise}/', mode='overwrite')


# LISTA DE BOARDS
dados = detalhe_lista_boards_por_members(key, token, idMember, area)
dados['idEnterprise'] = membro['idEnterprise'][0]
# '5ffeef183c014e442d7bbb0c'
if len(dados) > 0:
  filename = 'lista_boards_' + area
  dados = dados
  #dados.to_parquet(tmp_board + output.format(file=filename))
  '''TEMPORARY
  __escreve_arquivo(LND, authenticate_datalake(), dados, tmp_board + output.format(file=filename))
  '''
  #dados_ = spark.createDataFrame(dados)
  #dados_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_board}/', mode='overwrite')




LISTA_ACTIONS = pd.DataFrame(columns=[
"area",
"idBoard",
"idCard",
"idAction",
"idMemberCreator",
"type",
"date",
"data_text",
"card_old_due",
"card_due",
"data_dateLastEdited"])

# PARA CADA BOARD BUSCA DOS CARDS
for row in dados.itertuples():
  #coletar dados somente do quadro de demandas e quadro de compras
  if row.idBoard == '63e3ea369ba56d741da36330' or row.idBoard == '61e6b049b7ca7305318c5180': 
    idBoard = row.idBoard
    nome_board = row.name
    shortLink_board = row.shortLink
    idOrganizations = row.idOrganization
    print(f'nome_board ==> {nome_board}')
    print(f'idBoard ==> {idBoard}')
    print(f'idOrganizations ==> {idOrganizations}')
  
    LISTA_MEMBERS = pd.DataFrame(columns=[
      "area",
      "idBoard",
      "idMember",
      "idMemberReferrer",
      "idEnterprise",
      "username",
      "activityBlocked",
      "avatarHash",
      "avatarUrl",
      "fullName",
      "initials",
      "nonPublicAvailable"])


    if shortLink_board != '-1':
      #print('BOARD: {0}'.format(nome_board))
      logging.info('BOARD: {0}'.format(nome_board))
      
      # BUSCA ORGANIZACAO
      if idOrganizations:
        dados_organizations = detalhe_organizations(key, token, idBoard, idOrganizations, area)
        if len(dados_organizations) > 0:
          filename = 'boards_organizations_' + shortLink_board
          '''TEMPORARY
          __escreve_arquivo(LND, authenticate_datalake(), dados_organizations, tmp_organizations + output.format(file=filename))
          '''
          #dados_organizations.to_parquet(tmp_organizations + output.format(file=filename))
          schema = StructType(
              [
                  StructField('area', StringType(), nullable=True),
                  StructField('idBoard', StringType(), nullable=True),
                  StructField('idOrganizations', StringType(), nullable=True),
                  StructField('name', StringType(), nullable=True),
                  StructField('displayName', StringType(), nullable=True),
                  StructField('website', StringType(), nullable=True),
                  StructField('teamType', StringType(), nullable=True),
                  StructField('desc', StringType(), nullable=True),
                  StructField('url', StringType(), nullable=True),
                  StructField('logoHash', StringType(), nullable=True),
                  StructField('logoUrl', StringType(), nullable=True)
              ])
          
          dados_organizations_ = spark.createDataFrame(dados_organizations, schema=schema)
          #dados_organizations_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_organizations}/', mode='overwrite')


      # MEMBERS OF BOARDS
      dados_members = detalhe_board_members(key, token, idBoard, area)
      if len(dados_members) > 0:
        #print("MEMBERS: {0} IN BOARD {1}".format(len(dados_members), idBoard))
        logging.info("MEMBERS: {0} IN BOARD {1}".format(len(dados_members), idBoard))
        for member in dados_members.itertuples():
          LISTA_MEMBERS = LISTA_MEMBERS.append(
            {
              "area": member.area,
              "idBoard": member.idBoard,
              "idMember": member.idMember,
              "idMemberReferrer": str(member.idMemberReferrer),
              "idEnterprise": str(member.idEnterprise),
              "username": member.username,
              "activityBlocked": member.activityBlocked,
              "avatarHash": member.avatarHash,
              "fullName": member.fullName,
              "initials": member.initials,
              "nonPublicAvailable": member.nonPublicAvailable
            }, ignore_index=True)
          
          filename_member = "boards_members_" + shortLink_board
          #__escreve_arquivo(LND, authenticate_datalake(), LISTA_MEMBERS, tmp_member + output.format(file=filename_member))
          #LISTA_MEMBERS.to_parquet(tmp_member + output.format(file=filename_member))
          #LISTA_MEMBERS_ = spark.createDataFrame(LISTA_MEMBERS)
          #LISTA_MEMBERS_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_member}/', mode='overwrite')


      # ACTIONS
      #fday = date(2023, 10, 1)
      fday = date(2019, 1, 1)
      today = date.today()
      datelist = pd.date_range(fday, today, freq='M')

      for i in datelist:
        dt_inicio = '{y}-{m}-{d}'.format(y=i.year, m=i.month, d='01')
        dt_final = '{y}-{m}-{d}'.format(y=i.year, m=i.month, d=i.day)
        name_file_aux = '{i}_t{t}'.format(i=dt_inicio, t=i.month)

        dados_actions = detalhe_board_actions(key, token, idBoard, area, dt_inicio, dt_final)
        if len(dados_actions) > 0:
          #print("ACTIONS: {0} IN BOARD {1}".format(len(dados_actions), idBoard))
          logging.info("ACTIONS: {0} IN BOARD {1}".format(len(dados_actions), idBoard))
          for item_action in dados_actions.itertuples():
            try:
              LISTA_ACTIONS = LISTA_ACTIONS.append(
                  {
                    "area": item_action.area,
                    "idBoard": item_action.idBoard,
                    "idCard": item_action.idCard,
                    "idAction": item_action.idAction,
                    "idMemberCreator": item_action.idMemberCreator,
                    "type": item_action.type,
                    "date": item_action.date,
                    "data_text": item_action.data_text,
                    "card_old_due": str(item_action.card_old_due),
                    "card_due": str(item_action.card_due),
                    "data_dateLastEdited": str(item_action.data_dateLastEdited)}, ignore_index=True)
              
            except Exception as e:
              LISTA_ACTIONS = LISTA_ACTIONS.append(
                  {
                      "area": item_action.area,
                      "idBoard": item_action.idBoard,
                      "idCard": item_action.idCard,
                      "idAction": item_action.idAction,
                      "idMemberCreator": item_action.idMemberCreator,
                      "type": item_action.type,
                      "date": item_action.date,
                      "data_text": "",
                      "card_old_due": str(item_action.card_old_due),
                      "card_due": str(item_action.card_due),
                      "data_dateLastEdited": str(item_action.data_dateLastEdited)}, ignore_index=True)

          filename_action = "boards_cards_actions_" + shortLink_board + "_" + name_file_aux
          #__escreve_arquivo(LND, authenticate_datalake(), LISTA_ACTIONS, tmp_actions + output.format(file=filename_action))
          #LISTA_ACTIONS.to_parquet(tmp_actions + output.format(file=filename_action))
          #LISTA_ACTIONS_ = spark.createDataFrame(LISTA_ACTIONS)
          #LISTA_ACTIONS_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_actions}/', mode='overwrite')



      # BUSCA CARDS PARA O BOARD
      board_card = detalhe_board_cards(key, token, shortLink_board, idBoard, area)
      if len(board_card) > 0:
        #print('CARDS: {0} IN BOARD {1}'.format(len(board_card), nome_board))
        logging.info('CARDS: {0} IN BOARD {1}'.format(len(board_card), nome_board))
        filename = 'boards_cards_' + shortLink_board
        #__escreve_arquivo(LND, authenticate_datalake(), board_card, tmp_card + output.format(file=filename))
        #board_card.repartition(10).to_parquet(tmp_card + output.format(file=filename))
        #board_card_ = spark.createDataFrame(board_card)
        #board_card_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_card}/', mode='overwrite')

      # PARA CADA CARD BUSCA : LIST ,CUSTOMFIELDS ,CHECKLISTS
      for cards in board_card.itertuples():
        idCard = cards.idCard
        idChecklist = cards.idChecklists

        # LIST OF CARDS
        dados_lista_in_cards = detalhe_board_cards_lists(key, token, idBoard, idCard, area)
        if len(dados_lista_in_cards) > 0:
          #print("LIST: {0} IN CARD {1}".format(len(dados_lista_in_cards), idCard))
          logging.info("LIST: {0} IN CARD {1}".format(len(dados_lista_in_cards), idCard))
          for item in dados_lista_in_cards.itertuples():
            LISTA_LIST = LISTA_LIST.append(
              {
                "area": item.area,
                "idBoard": item.idBoard,
                "idCard": item.idCard,
                "idList": item.idList,
                "pos": item.pos,
                "closed": item.closed,
                "name": item.name}, ignore_index=True).astype(str)
      
        # CHECKLISTS
        if len(eval(idChecklist)) > 0:
          #print('CHECKLISTS: {0} IN CARD: {1} '.format(eval(idChecklist), idCard))
          logging.info('CHECKLISTS: {0} IN CARD: {1} '.format(eval(idChecklist), idCard))
          try:
            for check_items in eval(idChecklist):
              #print("check_items: idChecklist: {0}".format(check_items))
              logging.info("check_items: idChecklist: {0}".format(check_items))
              dados_checklist = detalhe_board_cards_checklist(key, token, check_items, idBoard, idCard,
                                                              area)
              if len(dados_checklist) > 0:
                for check in dados_checklist.itertuples():
                  LISTA_CHECKLIST_IN_CARDS = LISTA_CHECKLIST_IN_CARDS.append(
                      {
                        "area": check.area,
                        "idBoard": check.idBoard,
                        "idCard": check.idCard,
                        "idMember": check.idMember,
                        "idChecklist": check.idChecklist,
                        "nameList": check.nameList,
                        "id": check.id,
                        "state": check.state,
                        "name": check.name,
                        "due": check.due,
                        "pos": check.pos
                        }, ignore_index=True)
          except Exception as e:
            #print("Checklist em  branco")
            logging.info("Checklist em  branco")


        # GERA ARQUIVO FINAL POR BOARD
      if len(LISTA_LIST) > 0:
        filename_list = "boards_cards_list_" + shortLink_board
        #__escreve_arquivo(LND, authenticate_datalake(), LISTA_LIST, tmp_list + output.format(file=filename_list))
        LISTA_LIST_ = LISTA_LIST
        #LISTA_LIST_ = spark.createDataFrame(LISTA_LIST)
        #LISTA_LIST_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_list}/', mode='overwrite')


      if len(LISTA_CHECKLIST_IN_CARDS) > 0:
        filename_check = "boards_cards_checklists_" + shortLink_board
        LISTA_CHECKLIST_IN_CARDS_ = LISTA_CHECKLIST_IN_CARDS
        #__escreve_arquivo(LND, authenticate_datalake(), LISTA_CHECKLIST_IN_CARDS, tmp_checklist + output.format(file=filename_check))
        #LISTA_CHECKLIST_IN_CARDS_ = spark.createDataFrame(LISTA_CHECKLIST_IN_CARDS)
        #LISTA_CHECKLIST_IN_CARDS_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_checklist}/', mode='overwrite')



LISTA_LIST_.to_parquet(f'{tmp_list}/LISTA_LIST_.parquet', compression='snappy')
LISTA_LIST_ = pd.read_parquet(f'{tmp_list}') 
_LISTA_LIST_ = spark.createDataFrame(LISTA_LIST_)
schema = "oni/trello"
table = "list"
upload_file(spark=spark, dbutils=dbutils, df=_LISTA_LIST_, schema=schema, table=table)


LISTA_CHECKLIST_IN_CARDS_.to_parquet(f'{tmp_checklist}/LISTA_CHECKLIST_IN_CARDS_.parquet', compression='snappy')
LISTA_CHECKLIST_IN_CARDS_ = pd.read_parquet(f'{tmp_checklist}') 
_LISTA_CHECKLIST_IN_CARDS_ = spark.createDataFrame(LISTA_CHECKLIST_IN_CARDS_)
schema = "oni/trello"
table = "checklist"
upload_file(spark=spark, dbutils=dbutils, df=_LISTA_CHECKLIST_IN_CARDS_, schema=schema, table=table)


shutil.rmtree(tmp_list)
shutil.rmtree(tmp_checklist)

# COMMAND ----------

_LISTA_LIST_.count()

# COMMAND ----------

_LISTA_CHECKLIST_IN_CARDS_.count()

# COMMAND ----------


