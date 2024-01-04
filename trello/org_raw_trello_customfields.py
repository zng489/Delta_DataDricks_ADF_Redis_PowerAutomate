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


#def main(**kwargs):
#
#  '''TEMPORARY
#  bot = initialize()
#  LND: str = bot.lnd
#  adl: FileSystemClient = bot.adl
#
#  tmp = '/tmp/trello_oni/'
#  os.makedirs(tmp, mode=0o777, exist_ok=True)
#
#  configs = buscaArquivoDeConfiguracao(authenticate_datalake(), tmp, LND)
#  for index, config in configs.iterrows():
#    if config["AREA"] == 'UNIEPRO':
#      logging.info(config["AREA"])
#      run(
#          AREA=config["AREA"],
#          CHAVE=config["CHAVE"],
#          TOKEN=config["TOKEN"],
#          USUARIO=config["USUARIO"],
#          bot=bot,
#          LND=LND,
#          adl=adl,
#          tmp=tmp,
#          **kwargs,
#          )

    
#  run(
#    AREA= 'UNIEPRO',
#    CHAVE='890f4ae4256649cc46abc7a07ccec631',
#    TOKEN='9cdcdff3bb81109f050445d3f438692e716bd754f5aced0a52f5f90cf8818e5e',
#    USUARIO='observatorionacional.trello@cni.com.br', **kwargs,
#    )

# COMMAND ----------

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

# Function
# get_member_token()
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

membro = df1
idMember = membro['idMember'][0]
idMember

time.sleep(10)

# COMMAND ----------

tmp = 'trello/'
# cria os diretorio para armazenar os arquivos
tmp_customfields = tmp + 'trello__customfields'

tmp_log = tmp + 'trello__log'

os.makedirs(tmp, mode=0o777, exist_ok=True)
os.makedirs(tmp_customfields, mode=0o777, exist_ok=True)
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

time.sleep(10)

# COMMAND ----------

 # ENTERPRISE
dados_enterprise = membro
filename = 'enterprise_' + area
#dados_enterprise.to_parquet(tmp_enterprise + output.format(file=filename))
#dados_enterprise_ = spark.createDataFrame(dados_enterprise)
#dados_enterprise_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_enterprise}/', mode='overwrite')

dados_enterprise

time.sleep(10)

# COMMAND ----------


# LISTA DE BOARDS
# detalhe_lista_boards_por_members()
url_api_board = "https://api.trello.com/1/members/{0}/boards?key={1}&token={2}&cards=all";
t = requests.Session()
df1 = pd.DataFrame()

try:
  response = t.get(url_api_board.format(idMember, key, token), verify=False)
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

dados = df1
dados['idEnterprise'] = membro['idEnterprise'][0]
# '5ffeef183c014e442d7bbb0c'
if len(dados) > 0:
  filename = 'lista_boards_' + area
  #dados.to_parquet(tmp_board + output.format(file=filename))
  '''TEMPORARY
  __escreve_arquivo(LND, authenticate_datalake(), dados, tmp_board + output.format(file=filename))
  '''
  #dados_ = spark.createDataFrame(dados)
  #dados_.repartition(10).write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_board}/', mode='overwrite')

dados['idEnterprise'][0]

time.sleep(10)

# COMMAND ----------

# PARA CADA BOARD BUSCA DOS CARDS
for row in dados.itertuples():
  #coletar dados somente do quadro de demandas e quadro de compras
  if row.idBoard == '63e3ea369ba56d741da36330' or row.idBoard == '61e6b049b7ca7305318c5180': 
    idBoard = row.idBoard
    nome_board = row.name
    shortLink_board = row.shortLink
    idOrganizations = row.idOrganization
  
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
  

    if shortLink_board != '-1':
      #print('BOARD: {0}'.format(nome_board))
      print(idBoard)
      print(nome_board)
      print(shortLink_board)
      print(idOrganizations)      
      logging.info('BOARD: {0}'.format(nome_board))
      
      # BUSCA CARDS PARA O BOARD
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



      board_card = df1
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



        dados_customfields_in_card = detalhe_card_customfields(key, token, idCard, area)
        if len(dados_customfields_in_card) > 0:
          #print("CUSTOM: {0} IN CARD {1}".format(len(dados_customfields_in_card), idCard))
          logging.info("CUSTOM: {0} IN CARD {1}".format(len(dados_customfields_in_card), idCard))
          # ITEM DENTRO DO CUSTOM FIELD
          for custom in dados_customfields_in_card.itertuples():
            dados_customfields_detalhes = detalhe_customfields(key, token, custom.idCard, custom.idCustomField, area)   
            if len(dados_customfields_detalhes) > 0:
              #print("CUSTOM ITEM: {0} IN CARD {1}".format(len(dados_customfields_detalhes), idCard))
              logging.info(
                  "CUSTOM ITEM: {0} IN CARD {1}".format(len(dados_customfields_detalhes), idCard))
              # TRATA RESULTADO
              custom_idboard_ = idBoard
              custom_idcard_ = dados_customfields_detalhes['idCard'][0]
              custom_idCustomField_ = dados_customfields_detalhes['id'][0]
              custom_name_ = dados_customfields_detalhes['name'][0]
              custom_type_ = dados_customfields_detalhes['type'][0]
              custom_area = dados_customfields_detalhes['area'][0]

              custom_options_ = ''
              custom_value = ''
              custom_idValue = ''

              if custom_type_ == 'list':
                custom_options_ = dados_customfields_detalhes['options'][0]
                custom_idValue = custom.idValue

                # SELECIONA APENAS ITEM ESCOLHIDO
                df_options = pd.json_normalize(custom_options_)
                df_options = df_options.rename(columns={'value.text': 'value_text'})
                for option_item in df_options.itertuples():
                  if option_item.id == custom_idValue:
                    custom_value = option_item.value_text

              if custom_type_ == 'text':
                  custom_value = custom.value_text
              if custom_type_ == 'date':
                  custom_value = custom.value_date

                  #print('idCustomField: {0} name:{1} type:{2} value:{3}'.format(custom_idCustomField_,custom_name_,custom_type_,custom_value))

              logging.info('idCustomField: {0} name:{1} type:{2} value:{3}'.format(custom_idCustomField_,
                                                                                    custom_name_,
                                                                                    custom_type_,
                                                                                    custom_value))
              LISTA_CUSTOMFIELDS = LISTA_CUSTOMFIELDS.append({
                  "area": custom_area,
                  "idBoard": custom_idboard_,
                  "idCards": custom_idcard_,
                  "idCustomField": custom_idCustomField_,
                  "name": custom_name_,
                  "type": custom_type_,
                  "value": custom_value
              }, ignore_index=True)        

      if len(LISTA_CUSTOMFIELDS) > 0:
        filename_customfield = "boards_cards_customfields_" + shortLink_board
        LISTA_CUSTOMFIELDS_ = LISTA_CUSTOMFIELDS

LISTA_CUSTOMFIELDS_.to_parquet(f'{tmp_customfields}/LISTA_CUSTOMFIELDS.parquet', compression='snappy')
LISTA_CUSTOMFIELDS_ = pd.read_parquet(f'{tmp_customfields}') 
_LISTA_CUSTOMFIELDS_ = spark.createDataFrame(LISTA_CUSTOMFIELDS_)
schema = "oni/trello"
table = "customfields"
upload_file(spark=spark, dbutils=dbutils, df=_LISTA_CUSTOMFIELDS_, schema=schema, table=table)



shutil.rmtree(tmp_customfields)

# COMMAND ----------

LISTA_CUSTOMFIELDS_.count()

# COMMAND ----------


