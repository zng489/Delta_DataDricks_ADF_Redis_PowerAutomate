# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

import json
import re
import os
import traceback
import unicodedata
import pandas as pd
from trs_control_field import trs_control_field as tcf
from difflib import SequenceMatcher
from unicodedata import normalize
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.functions import lower, col
from pyspark.sql.types import *

spark.conf.set("spark.sql.execution.arrow.enabled", "false")

#objeto de controle do log
metadata = {'finished_with_errors': False}
lista_metadata = []

# COMMAND ----------

table = {"path_origin":"oni/trello","path_destination":"oni/trello/compras/members_qtd_compras","destination":"/trello/compras/members_qtd_compras","table_member":"member","table_juncao_boards_cards_members":"juncao_boards_cards_members","table_fact":"fact","table_members_qtd_compras":"members_qtd_compras","databricks":{"notebook":"/biz/oni/trello_adb/compras/biz_biz_dim_compras_members_qtd_compras_adb"},"copy_sqldw":"false"}

adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"trs_biz_rfb_cno","adf_pipeline_run_id":"c158e9dd-98df-4f7b-a64d-d13c76669868","adf_trigger_id":"67c514a7245449b984eb4aadd55bfbff","adf_trigger_name":"Sandbox","adf_trigger_time":"2023-08-22T21:22:42.5769844Z","adf_trigger_type":"Manual"}
 
dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

trs = dls['folders']['trusted']
biz = dls['folders']['business']

trs_path = trs+'/{path_origin}/'.format(path_origin=table['path_origin'])
biz_path = biz+'/{path_origin}/'.format(path_origin=table['path_origin'])
biz_target_path = biz+'/{path_destination}/'.format(path_destination=table['path_destination'])

arquivos_tratados_trs = var_adls_uri+trs_path+'{table}/'
carrega_biz = var_adls_uri+biz_path
arquivos_tratados_biz = var_adls_uri+biz_target_path

# COMMAND ----------

# DBTITLE 1,Leitura dados TRS/BIZ
# tabelas  
member = spark.read.parquet(arquivos_tratados_trs.format(table = table['table_member']))
juncao_boards_cards_members = spark.read.parquet(arquivos_tratados_trs.format(table = table['table_juncao_boards_cards_members']))  
fact = spark.read.parquet(carrega_biz + '{table}/'.format(table=table['table_fact'])) 

#member = spark.read.parquet(var_adls_uri + '/tmp/dev/trs/oni/trello/member/')
#juncao_boards_cards_members = spark.read.parquet(var_adls_uri + '/tmp/dev/trs/oni/trello/juncao_boards_cards_members/')  
#fact = spark.read.parquet(var_adls_uri + '/tmp/dev/biz/oni/trello/compras/fact/') 

# COMMAND ----------

# DBTITLE 1,FUNÇÕES
#***************** IDENTIFICA SIMILARIDADE ENTRE DUAS STRINGS *****************#

def sml(x,y):
    return SequenceMatcher(None, x, y).ratio()
  
#***************** RETORNA FULLNAME DO RESPONSÁVEL *****************#
  
def return_member(response_data, member_fullName):
  membro_final =''
  atual_result = sml(response_data,member_fullName)
  if response_data == 'Caroline Vianna' and member_fullName == 'Caroline Retameiro Rocha':
    membro_final = 'Caroline Retameiro Rocha'
  elif atual_result >= 0.6:
    membro_final = member_fullName
  return membro_final
  
# new_f1 = f.udf(return_member, StringType())

# COMMAND ----------

# coletar lista de dados (distintos) de Responsavel_Compras da tabela fact
dim_responses = fact.select('idCard','Responsavel_Compras').distinct()
dim_responses_pd = dim_responses.select(dim_responses.idCard,dim_responses.Responsavel_Compras).toPandas()
# print(list(dim_responses_pd['Responsavel_Compras']))

# coletar lista de dados (distintos) de fullName da tabela member
dim_fullName = member.select('idMember','fullName').distinct()
dim_fullName_pd = dim_fullName.select(dim_fullName.idMember,dim_fullName.fullName).toPandas()
# print(list(dim_fullName_pd['idMember']),list(dim_fullName_pd['fullName']))

# display(member.select('idMember','fullName').distinct())

create_table = True #Indica que se deve criar a tabela

for i in zip(list(dim_fullName_pd['idMember']),list(dim_fullName_pd['fullName'])):
  for response in zip(list(dim_responses_pd['idCard']),list(dim_responses_pd['Responsavel_Compras'])):
    result = None
    resp = str(response[1])
    # print(f'Entrou {resp} | {i[1]}')
    result = return_member(resp,i[1])
    # print(f'Saiu {result}')

    if result:

      if create_table == True:
        create_table = False #Indica que tabela já foi criada para as próximas execuções
        DIM_RESPONSES = pd.DataFrame({'idCard_response':response[0],'idResponse':i[0],'Responsavel_fullName':i[1],'Responsavel_Compras':response[1]}, index=[0])
      else:
        new_row = pd.Series({'idCard_response':response[0],'idResponse':i[0],'Responsavel_fullName':i[1],'Responsavel_Compras':response[1]})
        DIM_RESPONSES = pd.concat([DIM_RESPONSES, new_row.to_frame().T], ignore_index=True)

dim_responses_full = spark.createDataFrame(DIM_RESPONSES)
# display(dim_responses_full)

# COMMAND ----------

# DBTITLE 1,TRATAMENTO MEMBERS_OF_CARD
#CRIAÇÃO DIMENSÃO MEMBROS POR CARD

#dimensão com informações de membros
dim_member = member.select('idMember','username','fullName','initials').distinct()

#Renomeação de coluna para diferenciação de nome de colunas
juncao_boards_cards_members = juncao_boards_cards_members.withColumnRenamed('idMember','idMember_juncao')

#Junção de dados p/ obter dados completos dos membros
cards_members = juncao_boards_cards_members.join(dim_member, dim_member.idMember == juncao_boards_cards_members.idMember_juncao,'inner').where(f.col('idBoard') == '61e6b049b7ca7305318c5180')

#seleção de colunas em cards_members
cards_members = cards_members.select('idCard','idMember','username','fullName','initials')

#criação de lista de membros em cada card
members_of_card = (cards_members.groupBy('idCard')
              .agg(collect_set('idMember').alias('idMembers'),
                   collect_set('username').alias('usernames'),
                   collect_set('fullname').alias('members'),
                   collect_set('initials').alias('initials_members'))
              )


members_of_card_ofc = members_of_card.join(dim_responses_full, dim_responses_full.idCard_response == members_of_card.idCard,'left')
# display(members_of_card_ofc)

# COMMAND ----------

# DBTITLE 1,TRATAMENTO MEMBERS_QTD_DEMANDAS
# Coletar lista de membros presentes no Board de Demandas
filter_idBoard = member.where(f.col('idBoard') == '61e6b049b7ca7305318c5180')
distinct_member = filter_idBoard.select('idMember','fullName').distinct()
distinct_members_list = distinct_member.rdd.map(lambda x: x.idMember).collect()
distinct_fullName_list = distinct_member.rdd.map(lambda x: x.fullName).collect()
members_list = members_of_card_ofc.rdd.map(lambda x: x.idMembers).collect()
responsavel_list = members_of_card_ofc.rdd.map(lambda x: x.idResponse).collect()

#contagem de quantas vezes o id de cada membro aparece (como responsavel ou membro)
columns = ['idMember','fullName','qtd_reponsavel','qtd_membro']
data = []

soma_tudo = 0

for idMembro, nome in zip(distinct_members_list,distinct_fullName_list):
  contagem_responsavel = 0
  contagem_membro = 0
  for idResponsavel,lista_membros in zip(responsavel_list,members_list):
    idResponsavel = str(idResponsavel)
    # print(f'{idMembro} - {nome}|{idResponsavel} - {lista_membros}')
    # for i in lista_membros:
      # if nome == 'Ana Beatriz Souto Oliveira':
        # print(f'{idMembro} - {nome}|{idResponsavel} - {lista_membros}')
        # print(f'{idMembro} - {nome}|{idResponsavel} - {i}')
        # if idMembro == idResponsavel:
        #   contagem_responsavel += 1
        #   if idMembro == i:
        #     contagem_membro += 1
        # elif idMembro != idResponsavel:
        #   if idMembro == i:
        #     contagem_membro += 1
      
    if idMembro == idResponsavel:
      contagem_responsavel += 1
      soma_tudo += 1
    else:
      for i in lista_membros:
        if idMembro == i:
          contagem_membro += 1
          soma_tudo += 1
  
  data.append((idMembro,nome,contagem_responsavel,contagem_membro))
  
members_qtd_compras = spark.createDataFrame(data).toDF(*columns)

# REMOVER DADOS NULOS
members_qtd_compras = members_qtd_compras.dropna(how='all')

# COMMAND ----------

# DBTITLE 1,INCLUSÃO DADOS DE CONTROLE
#Exclusão de dados antigos de controle e de colunas inutilizadas
members_qtd_compras = members_qtd_compras.drop(*["nr_reg", "nm_arq_in", "dh_arq_in", "kv_process_control", "dh_insercao_raw", "dh_insercao_trs"])

#Inclusão dos dados de controle do adf
members_qtd_compras = tcf.add_control_fields(members_qtd_compras, adf)
members_qtd_compras = members_qtd_compras.withColumnRenamed("dh_insercao_trs","dh_insercao_biz")


#display(members_qtd_compras)

# COMMAND ----------

# DBTITLE 1,ESCRITA DATALAKE
members_qtd_compras.coalesce(1).write.mode("Overwrite").parquet(arquivos_tratados_biz)
