# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic"
)

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


#Parameters
#Parece que não precisará de parameters
 
adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"org_raw_tse_resultados_2022","adf_pipeline_run_id":"181ed1d1-3bce-471e-b9fb-f2062f021296","adf_trigger_id":"9a3958e25cd44c95bb1be066e9ee53d2","adf_trigger_name":"Sandbox","adf_trigger_time":"2023-08-18T21:16:02.5764238Z","adf_trigger_type":"Manual"}
 
dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"}
 
params = { 'namespace': '/oni', 'file_folder': '/empresas_exportadoras/imp_cnpj14', 'file_folder_raw':'/oni/mdic/comex/emp_bras_exp_imp/importadoras', 'extension': 'csv', 'column_delimiter': ';', 'encoding': 'UTF-8', 'null_value': '' }
#var_fiel = file = {"namespace":"oni","file_folder":"tse/resultados/2022","extension":"csv","column_delimiter":";","encoding":"UTF-8","null_value":""}

# COMMAND ----------

uld_path = f"{dls['folders']['landing']}{params['namespace']}{params['file_folder']}"
print(uld_path)

# COMMAND ----------

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

path = '{uri}/tmp/dev/lnd/crw/trello/config/API_TOKEN.csv'.format(uri=var_adls_uri)
trello = spark.read.format("csv").option("header", "true").option("sep", ";").load(path)
trello = trello.filter(trello["AREA"] == 'UNIEPRO').collect()

for row in trello:
    AREA = row[0]
    TOKEN = row[1]
    CHAVE = row[2]
    USUARIO = row[3]

area = AREA
key = CHAVE
token = TOKEN
idMember = USUARIO
area = AREA
area_ = AREA

'''TEMPORARY
bot=kwargs.get('bot')
LND=kwargs.get('LND')
adl=kwargs.get('adl')
tmp=kwargs.get('tmp')
'''

############### IT'LL DELETED 
#print('TESTING')#


#tmp = 'trello/'
#tmp_enterprise = tmp + 'trello__enterprise'
#tmp_board = tmp + 'trello__board'
#tmp_organizations = tmp + 'trello__organizations'
#tmp_log = tmp + 'trello__log'
#shutil.rmtree('trello/trello__log')
#%ls trello/
#os.makedirs(tmp, mode=0o777, exist_ok=True)
#os.makedirs(tmp_enterprise, mode=0o777, exist_ok=True)
#os.makedirs(tmp_board, mode=0o777, exist_ok=True)
#os.makedirs(tmp_organizations, mode=0o777, exist_ok=True)
#os.makedirs(tmp_log, mode=0o777, exist_ok=True)

tmp_enterprise = 'trello__enterprise'
tmp_board = 'trello__board'
tmp_organizations = 'trello__organizations'
#tmp_log = tmp + 'trello__log'
#shutil.rmtree('trello/trello__log')
#%ls trello/
#os.makedirs(tmp, mode=0o777, exist_ok=True)
os.makedirs(tmp_enterprise, mode=0o777, exist_ok=True)
os.makedirs(tmp_board, mode=0o777, exist_ok=True)
os.makedirs(tmp_organizations, mode=0o777, exist_ok=True)
#os.makedirs(tmp_log, mode=0o777, exist_ok=True)


# Busca idMember para consultas
# GET_MEMBER_TOKEN()
url_api_board = "https://api.trello.com/1/members/me?key={0}&token={1}";
t = requests.Session()
response = t.get(url_api_board.format(key, token), verify=False)
df1 = pd.DataFrame()

try:
  try:
    dados = response.json()
    df = pd.json_normalize(dados)
    df['area'] = area_
    df1 = df[['area', 'idEnterprise', 'idOrganizations', 'id', 'idMemberReferrer', 'username', 'fullName', 'initials', 'email', 'idBoards']]

    df1 = df1.astype({'idMemberReferrer': 'string', 'idBoards': 'string', 'idOrganizations': 'string'})
    df1 = df1.rename(columns={'id': 'idMember'})
  except Exception as e:
    print('get_member_token: area:{0} - ERROR: {1}'.format(area_, e))
    logging.info('get_member_token: area:{0} - ERROR: {1}'.format(area_, e))

  membro = df1
  membro.to_parquet(f'{tmp_enterprise}/enterprise.parquet', compression='snappy')

  membro = pd.read_parquet(f'{tmp_enterprise}/enterprise.parquet')
  idMember = membro['idMember'][0]
  dados_enterprise = spark.createDataFrame(membro)

  schema = "oni/trello"
  table = "enterprise"
  upload_file(spark=spark, dbutils=dbutils, df=dados_enterprise, schema=schema, table=table)
except Exception as e:
    raise e
finally:
    shutil.rmtree(tmp_enterprise)











# LISTA DE BOARDS
# detalhe_lista_boards_por_members()
try:
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
    print('detalhe_lista_boards_por_members: ERROR: {1}'.format(e))
    logging.info('detalhe_lista_boards_por_members:  ERROR: {1}'.format(e))

  dados = df1
  dados['idEnterprise'] = membro['idEnterprise'][0]

  if len(dados) > 0:
    filename = 'lista_boards_' + area
    dados.to_parquet(f'{tmp_board}/board.parquet', compression='snappy')

    board = spark.createDataFrame(dados)
    schema = "oni/trello"
    table = "board"
    upload_file(spark=spark, dbutils=dbutils, df=board, schema=schema, table=table)
except Exception as e:
    raise e
finally:
    shutil.rmtree(tmp_board)





# PARA CADA BOARD BUSCA DOS CARDS
try:
  for row in dados.itertuples():
    #coletar dados somente do quadro de demandas e quadro de compras
    if row.idBoard == '63e3ea369ba56d741da36330' or row.idBoard == '61e6b049b7ca7305318c5180': 
      idBoard = row.idBoard
      nome_board = row.name
      shortLink_board = row.shortLink
      idOrganizations = row.idOrganization
    
      if shortLink_board != '-1':
        #print('BOARD: {0}'.format(nome_board))
        logging.info('BOARD: {0}'.format(nome_board))
        
        # BUSCA ORGANIZACAO
        if idOrganizations:
          url_api_board = "https://api.trello.com/1/organizations/{0}?key={1}&token={2}&cards=all";
          t = requests.Session()
          response = t.get(url_api_board.format(idOrganizations, key, token), verify=False)
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
            
          dados_organizations = df1
          if len(dados_organizations) > 0:
            filename = 'boards_organizations_' + shortLink_board
            dados_organizations.to_parquet(f'{tmp_organizations}/{filename}.parquet', compression='snappy')
            schema = StructType(
                [
                    StructField('area', StringType(), nullable=True),
                    StructField('idBoard', StringType(), nullable=True),
                    StructField('idOrganizations', StringType(), nullable=True),
                    StructField('name', StringType(), nullable=True),
                    StructField('displayName', StringType(), nullable=True),
                    StructField('website', IntegerType(), nullable=True),
                    StructField('teamType', IntegerType(), nullable=True),
                    StructField('desc', StringType(), nullable=True),
                    StructField('url', StringType(), nullable=True),
                    StructField('logoHash', StringType(), nullable=True),
                    StructField('logoUrl', StringType(), nullable=True)
                ])
            organizations = spark.createDataFrame(dados_organizations, schema=schema)
            schema = "oni/trello"
            table = "organizations"
            upload_file(spark=spark, dbutils=dbutils, df=organizations, schema=schema, table=table)
except Exception as e:
    raise e
finally:
    shutil.rmtree(tmp_organizations)

# COMMAND ----------


