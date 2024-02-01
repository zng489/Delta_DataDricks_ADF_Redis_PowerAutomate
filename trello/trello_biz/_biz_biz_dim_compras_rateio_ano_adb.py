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

table = {"path_origin":"trello","path_destination":"oni/trello/compras/rateio_ano","destination":"/trello/compras/rateio_ano","table_fact":"fact","table_checklist_pagamento":"checklist_pagamento","table_rateio_ano":"rateio_ano","databricks":{"notebook":"/biz/trello/compras/biz_biz_dim_compras_rateio_ano"},"copy_sqldw":"false"}

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
print(biz_path)

biz_target_path = biz+'/{path_destination}/'.format(path_destination=table['path_destination'])
print(biz_target_path)

carrega_biz = var_adls_uri+biz_path
arquivos_tratados_biz = var_adls_uri+biz_target_path

# COMMAND ----------

# DBTITLE 1,Leitura dados BIZ
# tabelas
#fact = spark.read.parquet(carrega_biz + '{table}/'.format(table=table['table_fact']))
fact = spark.read.parquet(var_adls_uri + '/tmp/dev/biz/oni/trello/compras/fact/')

#checklist_pagamento = spark.read.parquet(carrega_biz + '{table}/'.format(table=table['table_checklist_pagamento']))
checklist_pagamento = spark.read.parquet(var_adls_uri + '/tmp/dev/biz/oni/trello/compras/checklist_pagamento/')

# COMMAND ----------

fact = fact.withColumnRenamed('idCard','idCard_fact')
checklist_pagamento = checklist_pagamento.drop('dh_insercao_biz','kv_process_control')
union_df = checklist_pagamento.join(fact.select('idCard_fact','Entidade','Rateio','Outro_rateio','Valor_Total'), fact.idCard_fact == checklist_pagamento.idCard,'inner')
union_df.display()
test_df = union_df.toPandas()

# COMMAND ----------

create_table = True  # Indica que se deve criar a tabela

percentualPadraoIEL = 0.0534
percentualPadraoSENAI = 0.4106
percentualPadraoSESI = 0.536

for i, column_value in test_df.iterrows():

    # if column_value['idCard'] == '62509474c8fb842a6606f861':
    #   print(column_value['pagamento'])
    #   print(type(column_value['pagamento']))

    if str(column_value['pagamento']) == 'nan':
        values = {'idCard': [column_value['idCard']], 'Entidade': column_value['Entidade'],
                  'id_pagamento': column_value['id'], 'valor': 0, 'state': column_value['state'], 'data_pagamento': column_value['due'], 'ano_pagamento': column_value['ano_pagamento']}

    elif column_value['Rateio'] == None:
        if column_value['Outro_rateio'] == None:
            values = {'idCard': [column_value['idCard']], 'Entidade': column_value['Entidade'],
                      'id_pagamento': column_value['id'], 'valor': column_value['pagamento'],
                      'state': column_value['state'], 'data_pagamento': column_value['due'], 'ano_pagamento': column_value['ano_pagamento']}

        elif column_value['Outro_rateio'] != None:
            values = {'idCard': [], 'Entidade': [], 'id_pagamento': [], 'valor': [], 'state': [], 'data_pagamento': [], 'ano_pagamento': []}

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
                        percentage = int(i[
                                         num_percentage - 2:num_percentage])  # coleta o valor da porcentagem e transforma em inteiro
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
                        values['idCard'].append(column_value['idCard'])
                        values['Entidade'].append(entity)
                        values['id_pagamento'].append(column_value['id'])
                        values['valor'].append(column_value[
                                                   'pagamento'] * percentage)  # acrescenta dado na lista valor {"valor":pagamento*PERCENTUAL}
                        values['state'].append(column_value['state'])
                        values['data_pagamento'].append(column_value['due'])
                        values['ano_pagamento'].append(column_value['ano_pagamento'])
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

                    values['idCard'].append(column_value['idCard'])
                    values['Entidade'].append(entity)
                    values['id_pagamento'].append(column_value['id'])
                    values['valor'].append(column_value[
                                               'pagamento'] * percentage)  # acrescenta dado na lista valor {"valor":pagamento*PERCENTUAL}
                    values['state'].append(column_value['state'])
                    values['data_pagamento'].append(column_value['due'])
                    values['ano_pagamento'].append(column_value['ano_pagamento'])

    elif ('Outro' in column_value['Rateio'] and column_value['Outro_rateio'] == None) or (
            column_value['Rateio'] == None and column_value['Outro_rateio'] == None):
        values = {'idCard': column_value['idCard'], 'Entidade': None, 'id_pagamento': column_value['id'], 'valor': None,
                  'state': None, 'data_pagamento': None, 'ano_pagamento': None}

    elif ('Outro' in column_value['Rateio'] and column_value['Outro_rateio'] != None) or (
            column_value['Rateio'] == None and column_value['Outro_rateio'] != None):
        values = {'idCard': [], 'Entidade': [], 'id_pagamento': [], 'valor': [], 'state': [], 'data_pagamento': [], 'ano_pagamento': []}

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
                    values['idCard'].append(column_value['idCard'])
                    values['Entidade'].append(entity)
                    values['id_pagamento'].append(column_value['id'])
                    values['valor'].append(column_value[
                                               'pagamento'] * percentage)  # acrescenta dado na lista valor {"valor":pagamento*PERCENTUAL}
                    values['state'].append(column_value['state'])
                    values['data_pagamento'].append(column_value['due'])
                    values['ano_pagamento'].append(column_value['ano_pagamento'])
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

                values['idCard'].append(column_value['idCard'])
                values['Entidade'].append(entity)
                values['id_pagamento'].append(column_value['id'])
                values['valor'].append(column_value[
                                           'pagamento'] * percentage)  # acrescenta dado na lista valor {"valor":pagamento*PERCENTUAL}
                values['state'].append(column_value['state'])
                values['data_pagamento'].append(column_value['due'])
                values['ano_pagamento'].append(column_value['ano_pagamento'])


    elif 'Padrão' in column_value['Rateio']:
        values = {'idCard': [column_value['idCard'], column_value['idCard'], column_value['idCard']],
                  'Entidade': ['IEL', 'SENAI', 'SESI'], 'id_pagamento': column_value['id'],
                  'valor': [column_value['pagamento'] * percentualPadraoIEL,
                            column_value['pagamento'] * percentualPadraoSENAI,
                            column_value['pagamento'] * percentualPadraoSESI],
                  'state': [column_value['state'], column_value['state'], column_value['state']],
                  'data_pagamento': [column_value['due'], column_value['due'],column_value['due']],
                  'ano_pagamento': [column_value['ano_pagamento'], column_value['ano_pagamento'],
                                    column_value['ano_pagamento']]}

    elif 'CNI' in column_value['Rateio']:
        values = {'idCard': [column_value['idCard']], 'Entidade': 'CNI', 'id_pagamento': column_value['id'],
                  'valor': column_value['pagamento'], 'state': column_value['state'], 'data_pagamento': column_value['due'],
                  'ano_pagamento': column_value['ano_pagamento']}

    elif 'IEL' in column_value['Rateio']:
        values = {'idCard': [column_value['idCard']], 'Entidade': 'IEL', 'id_pagamento': column_value['id'],
                  'valor': column_value['pagamento'], 'state': column_value['state'], 'data_pagamento': column_value['due'],
                  'ano_pagamento': column_value['ano_pagamento']}

    elif 'SENAI' in column_value['Rateio']:
        values = {'idCard': [column_value['idCard']], 'Entidade': 'SENAI', 'id_pagamento': column_value['id'],
                  'valor': column_value['pagamento'], 'state': column_value['state'], 'data_pagamento': column_value['due'],
                  'ano_pagamento': column_value['ano_pagamento']}

    elif 'SESI' in column_value['Rateio']:
        values = {'idCard': [column_value['idCard']], 'Entidade': 'SESI', 'id_pagamento': column_value['id'],
                  'valor': column_value['pagamento'], 'state': column_value['state'], 'data_pagamento': column_value['due'],
                  'ano_pagamento': column_value['ano_pagamento']}

    # print(values)

    if create_table == True and len(values['idCard']) > 1:
        create_table = False  # Indica que tabela já foi criada para as próximas execuções
        DIM_RATEIO = pd.DataFrame(values)
    elif create_table == True and len(values['idCard']) == 1:
        create_table = False  # Indica que tabela já foi criada para as próximas execuções
        DIM_RATEIO = pd.DataFrame(values, index=[0])
    elif create_table == False:
        DIM_RATEIO_TEMP = pd.DataFrame(values)
        DIM_RATEIO = pd.concat([DIM_RATEIO, DIM_RATEIO_TEMP], ignore_index=True)

df_dim_rateio = spark.createDataFrame(DIM_RATEIO)

# REMOVER DADOS NULOS
df_dim_rateio = df_dim_rateio.dropna(how='all')

# COMMAND ----------

df_dim_rateio = df_dim_rateio.withColumn("state",
                                         when(df_dim_rateio.state == "complete","pago") \
                                        .when(df_dim_rateio.state == "incomplete","planejado"))

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
df_dim_rateio.coalesce(1).write.mode("Overwrite").parquet(var_adls_uri + '/tmp/dev/biz/oni/trello/compras/rateio_ano' ) 

# COMMAND ----------


