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

table = {"path_origin":"trello","path_destination":"oni/trello/compras/members_of_card","destination":"/trello/compras/members_of_card","table_member":"member","table_juncao_boards_cards_members":"juncao_boards_cards_members","table_fact":"fact","table_members_of_card":"members_of_card","databricks":{"notebook":"/biz/trello/compras/biz_biz_dim_compras_members_of_card"},"copy_sqldw":"false"}

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
biz_path = biz+'/{path_destination}/'.format(path_destination=table['path_destination'][:14])
biz_target_path = biz+'/{path_destination}/'.format(path_destination=table['path_destination'])

arquivos_tratados_trs = var_adls_uri+trs_path+'{table}/'
carrega_biz = var_adls_uri+biz_path
arquivos_tratados_biz = var_adls_uri+biz_target_path

# COMMAND ----------

# DBTITLE 1,Leitura dados TRS
# tabelas 
#member = spark.read.parquet(arquivos_tratados_trs.format(table = table['table_member']))
member = spark.read.parquet(var_adls_uri+'/tmp/dev/trs/oni/trello/member/')


#juncao_boards_cards_members = spark.read.parquet(arquivos_tratados_trs.format(table = table['table_juncao_boards_cards_members']))
juncao_boards_cards_members = spark.read.parquet(var_adls_uri+'/tmp/dev/trs/oni/trello/juncao_boards_cards_members/')  


#fact = spark.read.parquet(carrega_biz + '{table}/'.format(table=table['table_fact'])) 
fact = spark.read.parquet(var_adls_uri+'/tmp/dev/biz/oni/trello/compras/fact/') 

# COMMAND ----------

# DBTITLE 1,FUNÇÕES
#***************** IDENTIFICA SIMILARIDADE ENTRE DUAS STRINGS *****************#

def sml(x,y):
    return SequenceMatcher(None, x, y).ratio()
  
#***************** RETORNA FULLNAME DO RESPONSÁVEL *****************#
  
def return_member(responsavel_column, members_column):
  membro_final =''
  atual_result = 0
  for membro in members_column:
    atual_result = sml(responsavel_column,membro)
    if atual_result >= 0.5:
      result = atual_result
      membro_final = membro
  return membro_final
  
new_f1 = f.udf(return_member, StringType())

# COMMAND ----------

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


#Renomeação de coluna para diferenciação de nome de colunas
members_of_card = members_of_card.withColumnRenamed('idCard','idCard_members_of_card')

# COMMAND ----------

# ENCONTRAR INFORMAÇÕES DO RESPONSÁVEL

#dimensão com informações de membros e coleta de lista
distinct_member = member.select('idBoard','idMember','fullName').distinct().where(f.col('idBoard') == '61e6b049b7ca7305318c5180')
distinct_members_list = distinct_member.rdd.map(lambda x: x.idMember).collect()
distinct_fullName_list = distinct_member.rdd.map(lambda x: x.fullName).collect()

#coletar nomes de responsáveis distintos da fato
fact_responses = fact.select('Responsavel_Compras').distinct()
fact_responses_list = fact_responses.rdd.map(lambda x: x.Responsavel_Compras).collect()

#Encontra idMember do responsável
columns = ['idMember','Responsavel_Compras_fullName','Responsavel_Compras_dim']
data = []

for idMembro, nome in zip(distinct_members_list,distinct_fullName_list):
  # print(f'{idMembro} - {nome}')

  #excessão, função não consegue coletar este id
  if nome == 'Caroline Retameiro Rocha':
      data.append((idMembro,nome,'Caroline Vianna'))

  for nomeResponsavel in fact_responses_list:
    # print(f'{idMembro} - {nome} - {nomeResponsavel}')
    idMembro = str(idMembro)
    nome = str(nome)
    nomeResponsavel = str(nomeResponsavel)
    if sml(nome,nomeResponsavel) >= 0.6: #faz comparação de similaridade entre os nomes, se for maior que 0.6 passa
      data.append((idMembro,nome,nomeResponsavel))
  
df = spark.createDataFrame(data).toDF(*columns)

# COMMAND ----------

# ETAPAS NECESSÁRIAS PARA PRÓXIMA DIMENSÃO A SER CRIADA

#necessário informações de responsável do card, seu id (coletado por meio de uma taxa de similaridade entre RESPONSAVEL e fullName)
members_of_card2 = members_of_card.join(fact.select('idCard','Responsavel_Compras'), fact.idCard == members_of_card.idCard_members_of_card,'right')
members_of_card_ofc = members_of_card2.join(df, df.Responsavel_Compras_dim == members_of_card2.Responsavel_Compras,'left')

#organização de colunas
members_of_card_ofc =members_of_card_ofc.select('idCard','idMember','Responsavel_Compras','Responsavel_Compras_fullName','idMembers','usernames','members','initials_members')

# TRATAMENTO STRINGTYPE
cols_transform = ['idMembers','usernames','members','initials_members']

for column in cols_transform:
  members_of_card_ofc = members_of_card_ofc.withColumn(column, f.col(column).cast(StringType()))
  members_of_card_ofc = members_of_card_ofc.withColumn(column, f.regexp_replace(column,r'[\[]',""))
  members_of_card_ofc = members_of_card_ofc.withColumn(column, f.regexp_replace(column,r'[\]]',""))
  members_of_card_ofc = members_of_card_ofc.withColumn(column, f.regexp_replace(column,r'["]',""))

# REMOVER DADOS NULOS
members_of_card_ofc = members_of_card_ofc.dropna(how='all')

# COMMAND ----------

# DBTITLE 1,INCLUSÃO DADOS DE CONTROLE
#Exclusão de dados antigos de controle e de colunas inutilizadas
members_of_card_ofc = members_of_card_ofc.drop(*["nr_reg", "nm_arq_in", "dh_arq_in", "kv_process_control", "dh_insercao_raw", "dh_insercao_trs"])

#Inclusão dos dados de controle do adf
members_of_card_ofc = tcf.add_control_fields(members_of_card_ofc, adf)
members_of_card_ofc = members_of_card_ofc.withColumnRenamed("dh_insercao_trs","dh_insercao_biz")


display(members_of_card_ofc)

# COMMAND ----------

# DBTITLE 1,ESCRITA DATALAKE
#members_of_card_ofc.coalesce(1).write.mode("Overwrite").parquet(arquivos_tratados_biz)
members_of_card_ofc.coalesce(1).write.mode("Overwrite").parquet(var_adls_uri + '/tmp/dev/biz/oni/trello/compras/members_of_card')


# COMMAND ----------


