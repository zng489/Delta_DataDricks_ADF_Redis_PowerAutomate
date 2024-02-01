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



table = {"path_origin":"trello","path_destination":"oni/trello/compras/rateio","destination":"/trello/compras/rateio","table_fact":"fact","table_rateio":"rateio","databricks":{"notebook":"/biz/trello/compras/biz_biz_dim_compras_rateio"},"copy_sqldw":"false"}

adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"trs_biz_rfb_cno","adf_pipeline_run_id":"c158e9dd-98df-4f7b-a64d-d13c76669868","adf_trigger_id":"67c514a7245449b984eb4aadd55bfbff","adf_trigger_name":"Sandbox","adf_trigger_time":"2023-08-22T21:22:42.5769844Z","adf_trigger_type":"Manual"}
 
dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

trs = dls['folders']['trusted']
biz = dls['folders']['business']

biz_path = biz+'/{path_destination}/'.format(path_destination=table['path_destination'][:14])
biz_target_path = biz+'/{path_destination}/'.format(path_destination=table['path_destination'])

carrega_biz = var_adls_uri+biz_path
arquivos_tratados_biz = var_adls_uri+biz_target_path

# COMMAND ----------

# DBTITLE 1,Leitura dados BIZ
# tabelas
#fact = spark.read.parquet(carrega_biz + '{table}/'.format(table=table['table_fact']))
fact = spark.read.parquet(var_adls_uri + '/tmp/dev/biz/oni/trello/compras/fact/')

# COMMAND ----------

fact = fact.toPandas()  # tratativa realizada em pandas

create_table = True  # Indica que se deve criar a tabela
percentualPadraoIEL = 0.0534
percentualPadraoSENAI = 0.4106
percentualPadraoSESI = 0.536

for i, column_value in fact.iterrows():

    if column_value['Rateio'] == None:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'], 'CNI': 0.0,
                  'IEL': 0.0, 'SENAI': 0.0, 'SESI': 0.0, 'OUTRO': 0.0, 'Valor_Total': column_value['Valor_Total']}

    elif 'CNI' in column_value['Rateio']:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'],
                  'CNI': column_value['Valor_Total'], 'IEL': 0.0, 'SENAI': 0.0, 'SESI': 0.0, 'OUTRO': 0.0,
                  'Valor_Total': column_value['Valor_Total']}

    elif 'IEL' in column_value['Rateio']:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'], 'CNI': 0.0,
                  'IEL': column_value['Valor_Total'], 'SENAI': 0.0, 'SESI': 0.0, 'OUTRO': 0.0,
                  'Valor_Total': column_value['Valor_Total']}

    elif 'SENAI' in column_value['Rateio']:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'], 'CNI': 0.0,
                  'IEL': 0.0, 'SENAI': column_value['Valor_Total'], 'SESI': 0.0, 'OUTRO': 0.0,
                  'Valor_Total': column_value['Valor_Total']}

    elif 'SESI' in column_value['Rateio']:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'], 'CNI': 0.0,
                  'IEL': 0.0, 'SENAI': 0.0, 'SESI': column_value['Valor_Total'], 'OUTRO': 0.0,
                  'Valor_Total': column_value['Valor_Total']}

    elif 'Padrão' in column_value['Rateio']:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'], 'CNI': 0.0,
                  'IEL': column_value['Valor_Total'] * percentualPadraoIEL,
                  'SENAI': column_value['Valor_Total'] * percentualPadraoSENAI,
                  'SESI': column_value['Valor_Total'] * percentualPadraoSESI, 'OUTRO': 0.0,
                  'Valor_Total': column_value['Valor_Total']}

    elif 'Outro' in column_value['Rateio'] and column_value['Outro_rateio'] == None:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'], 'CNI': 0.0,
                  'IEL': 0.0, 'SENAI': 0.0, 'SESI': 0.0, 'OUTRO': column_value['Valor_Total'],
                  'Valor_Total': column_value['Valor_Total']}

    elif 'Outro' in column_value['Rateio'] and column_value['Outro_rateio'] != None:
        values = {'idCard': column_value['idCard'], 'Entidade': column_value['Entidade'],
                  'Rateio': column_value['Rateio'], 'Outro_rateio': column_value['Outro_rateio'], 'CNI': 0.0,
                  'IEL': 0.0, 'SENAI': 0.0, 'SESI': 0.0, 'OUTRO': 0.0, 'Valor_Total': column_value['Valor_Total']}

        value = column_value['Outro_rateio']  # recebe valor de coluna Outro_rateio

        # identifica delimitador
        if ';' in value:
            delimiter = ';'
        elif 'e' in value:
            delimiter = 'e'
        elif ';' not in value and 'e' not in value and ' ' in value:
            delimiter = ' '

        # separa as Entidade e seus respectivos percentuais
        new_value = column_value['Outro_rateio'].split(delimiter)

        if delimiter == ' ':  # se delimitador for igual a ' ' os percentuais e as Entidades ficarão separada. Necessário tratamento específico.

            percentage = None
            entity = None

            for i in new_value:
                i = i.lstrip()  # retira espaço inicial da string

                if '%' in i:
                    # tratamento do valor de porcentagem
                    num_percentage = i.find('%')  # retorna endereço de porcentagem(%) encontrado
                    percentage = int(
                        i[num_percentage - 2:num_percentage])  # coleta o valor da porcentagem e transforma em inteiro
                    percentage = percentage / 100  # torna o valor em porcentagem para cálculo em cima do valor total
                    # print(percentage)

                else:
                    # identifica Entidade
                    if 'CNI' in i:
                        entity = 'CNI'
                    elif 'IEL' in i:
                        entity = 'IEL'
                    elif 'SENAI' in i:
                        entity = 'SENAI'
                    elif 'SESI' in i:
                        entity = 'SESI'

                # quando obtivermos as duas informações (percentual e Entidade), atualizar dict de valores
                if percentage and entity:
                    values[entity] = column_value[
                                         'Valor_Total'] * percentage  # atualiza dict {"Entidade":Valor_Total*PERCENTUAL}
                    percentage = None
                    entity = None

        else:

            for i in new_value:
                i = i.lstrip()  # retira espaço inicial da string

                # tratamento do valor de porcentagem
                num_percentage = i.find('%')  # retorna endereço de porcentagem(%) encontrado
                percentage = int(
                    i[num_percentage - 2:num_percentage])  # coleta o valor da porcentagem e transforma em inteiro
                percentage = percentage / 100  # torna o valor em porcentagem para cálculo em cima do valor total
                # print(percentage)

                # identifica Entidade
                if 'CNI' in i:
                    entity = 'CNI'
                elif 'IEL' in i:
                    entity = 'IEL'
                elif 'SENAI' in i:
                    entity = 'SENAI'
                elif 'SESI' in i:
                    entity = 'SESI'

                values[entity] = column_value['Valor_Total'] * percentage

    if create_table == True:
        create_table = False  # Indica que tabela já foi criada para as próximas execuções
        DIM_RATEIO = pd.DataFrame(values, index=[0])
    elif create_table == False:
        new_row = pd.Series(values)
        DIM_RATEIO = pd.concat([DIM_RATEIO, new_row.to_frame().T], ignore_index=True)

df_dim_rateio = spark.createDataFrame(DIM_RATEIO)

# REMOVER DADOS NULOS
df_dim_rateio = df_dim_rateio.dropna(how='all')

# COMMAND ----------

# DBTITLE 1,INCLUSÃO DADOS DE CONTROLE
#Exclusão de dados antigos de controle e de colunas inutilizadas
df_dim_rateio = df_dim_rateio.drop(*["nr_reg", "nm_arq_in", "dh_arq_in", "kv_process_control", "dh_insercao_raw", "dh_insercao_trs"])

#Inclusão dos dados de controle do adf
df_dim_rateio = tcf.add_control_fields(df_dim_rateio, adf)
df_dim_rateio = df_dim_rateio.withColumnRenamed("dh_insercao_trs","dh_insercao_biz")


display(df_dim_rateio)

# COMMAND ----------

# DBTITLE 1,ESCRITA DATALAKE
#df_dim_rateio.coalesce(1).write.mode("Overwrite").parquet(arquivos_tratados_biz)
df_dim_rateio.coalesce(1).write.mode("Overwrite").parquet(var_adls_uri + '/tmp/dev/biz/oni/trello/compras/rateio') 

# COMMAND ----------


