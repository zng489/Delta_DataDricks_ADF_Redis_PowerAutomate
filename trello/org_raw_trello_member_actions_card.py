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
            '''print(
                'detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board,
                                                                                              'BOARD SEM CARDS'))'''

            logging.info(
                'detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board,
                                                                                              'BOARD SEM CARDS'))
    except Exception as e:
        '''print(
            'detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board, e))'''
        logging.info(
            'detalhe_board_cards: idBoard:{0} - shortLink_board:{1} -  ERROR: {2}'.format(idBoard, shortLink_board, e))
    return df1

# COMMAND ----------

shutil.rmtree(tmp_list)

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

tmp_card = 'trello__card'
tmp_member = 'trello__member'
tmp_actions = 'trello__actions'

os.makedirs(tmp_card, mode=0o777, exist_ok=True)
os.makedirs(tmp_member, mode=0o777, exist_ok=True)
os.makedirs(tmp_actions, mode=0o777, exist_ok=True)



############### IT'LL DELETED 
#print('TESTING')#


# Busca idMember para consultas
# GET_MEMBER_TOKEN()
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
dados_enterprise = membro

# LISTA DE BOARDS
# detalhe_lista_boards_por_members()
url_api_board = "https://api.trello.com/1/members/{0}/boards?key={1}&token={2}&cards=all";
t = requests.Session()
df1 = pd.DataFrame()

try:
  response = t.get(url_api_board.format(idMember, key, token), verify=False)
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

dados = df1
dados['idEnterprise'] = membro['idEnterprise'][0]


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
    "data_dateLastEdited"
])

df_card = pd.DataFrame()
# PARA CADA BOARD BUSCA DOS CARDS
for row in dados.itertuples():
  #coletar dados somente do quadro de demandas e quadro de compras
  if row.idBoard == '63e3ea369ba56d741da36330' or row.idBoard == '61e6b049b7ca7305318c5180': 
    idBoard = row.idBoard
    nome_board = row.name
    shortLink_board = row.shortLink
    idOrganizations = row.idOrganization
    #print(idOrganizations)



    if shortLink_board != '-1':
      #print('BOARD: {0}'.format(nome_board))
      logging.info('BOARD: {0}'.format(nome_board))


      # ################# #
      # MEMBERS OF BOARDS #
      # ################# #
      url_api = "https://api.trello.com/1/boards/{2}/members?fields=id,idMemberReferrer,idEnterprise,username,activityBlocked,avatarHash,fullName,initials,nonPublicAvailable&key={0}&token={1}";
      t = requests.Session()
      response = t.get(url_api.format(key, token, idBoard), verify=False)
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

      # ################# #
      # MEMBERS OF BOARDS #
      # ################# #
      dados_members = df1
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




      # ####### # 
      # ACTIONS #
      # ####### #
      #fday = date(2019, 10, 1)
      fday = date(2021, 1, 1)
      today = date.today()
      datelist = pd.date_range(fday, today, freq='M')


      for i in datelist:
        dt_inicio = '{y}-{m}-{d}'.format(y=i.year, m=i.month, d='01')
        #print(dt_inicio)
        dt_final = '{y}-{m}-{d}'.format(y=i.year, m=i.month, d=i.day)
        #print(dt_final)
        name_file_aux = '{i}_t{t}'.format(i=dt_inicio, t=i.month)
        area_ = 'UNIEPRO'

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
                      "data_dateLastEdited": str(item_action.data_dateLastEdited)
                  }, ignore_index=True)
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
                        "data_dateLastEdited": str(item_action.data_dateLastEdited)
                    }, ignore_index=True)
                

      # ######################## #
      # BUSCA CARDS PARA O BOARD #
      # ######################## #
      #board_card = detalhe_board_cards(key, token, shortLink_board, idBoard, area)
      board_card = detalhe_board_cards(key, token, shortLink_board, idBoard, area)

      df_card = df_card.append(board_card, ignore_index=True)
      #board_card = df1
      if len(df_card) > 0:
        #print('CARDS: {0} IN BOARD {1}'.format(len(board_card), nome_board))
        logging.info('CARDS: {0} IN BOARD {1}'.format(len(df_card), nome_board))
        filename = 'boards_cards_' + shortLink_board
        #__escreve_arquivo(LND, authenticate_datalake(), board_card, tmp_card + output.format(file=filename))
        #df_card.to_parquet(f'{tmp_card}/df_card.parquet', compression='snappy')


#print(LISTA_MEMBERS.count()) = 73
LISTA_MEMBERS.to_parquet(f'{tmp_member}/LISTA_MEMBERS.parquet', compression='snappy')
LISTA_MEMBERS = pd.read_parquet(f'{tmp_member}')
schema = StructType(
  [
    StructField('area', StringType(), nullable=True),
    StructField('idBoard', StringType(), nullable=True),
    StructField('idMember', StringType(), nullable=True),
    StructField('idMemberReferrer', StringType(), nullable=True),
    StructField('idEnterprise', StringType(), nullable=True),
    StructField('username', StringType(), nullable=True),
    StructField('activityBlocked', BooleanType(), nullable=True),
    StructField('avatarHash', StringType(), nullable=True),
    StructField('avatarUrl', StringType(), nullable=True),
    StructField('fullName', StringType(), nullable=True),
    StructField('initials', StringType(), nullable=True),
    StructField('nonPublicAvailable', BooleanType(), nullable=True)
    ])
LISTA_MEMBERS_ = spark.createDataFrame(LISTA_MEMBERS, schema=schema)
schema = "oni/trello"
table = "member"
upload_file(spark=spark, dbutils=dbutils, df=LISTA_MEMBERS_, schema=schema, table=table)


LISTA_ACTIONS.to_parquet(f'{tmp_actions}/LISTA_ACTIONS.parquet', compression='snappy')
LISTA_ACTIONS = pd.read_parquet(f'{tmp_actions}')
LISTA_ACTIONS_ = spark.createDataFrame(LISTA_ACTIONS)
schema = "oni/trello"
table = "actions"
upload_file(spark=spark, dbutils=dbutils, df=LISTA_ACTIONS_, schema=schema, table=table)

          #LISTA_ACTIONS_SPARK = spark.createDataFrame(LISTA_ACTIONS)

          #LISTA_ACTIONS_SPARK.write.option("encoding", "ISO-8859-1").option('header','true').parquet(f'{var_adls_uri}/uds/uniepro/oni/{tmp_actions}/', mode='overwrite')

df_card.to_parquet(f'{tmp_card}/df_card.parquet', compression='snappy')
LISTA_CARD = pd.read_parquet(f'{tmp_card}') 
LISTA_CARD_ = spark.createDataFrame(LISTA_CARD)
schema = "oni/trello"
table = "card"
upload_file(spark=spark, dbutils=dbutils, df=LISTA_CARD_, schema=schema, table=table)


shutil.rmtree(tmp_member)
shutil.rmtree(tmp_actions)
shutil.rmtree(tmp_card)

# COMMAND ----------


