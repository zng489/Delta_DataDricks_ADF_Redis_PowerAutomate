# Databricks notebook source
file = {"namespace":"/oni","type_raw":"/usr", "file_folder":"/mte/rais", "file_subfolder":"/rais_estabelecimentos_2018_2023", "prm_path": "/prm/usr/me/KC2332_ME_RAIS_ESTABELECIMENTO_mapeamento_raw.xlsx", "extension":"txt","column_delimiter":"","encoding":"","null_value":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')
     

# COMMAND ----------

# Biblioteca cni_connectors, que dá acesso aos dados no datalake
from cni_connectors import adls_connector as adls_conn

# A biblioteca criada para facilitar a declaração dos testes. É necessário importá-la.
### from datatest.gx_context_provider import GXContextProvider

# Essa declaração é necessária em todos os notebooks. Somente através dela podemos acessar os dados no datalake.
var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from trs_control_field import trs_control_field as tcf
import pyspark.sql.functions as f
import crawler.functions as cf
from pyspark.sql import SparkSession
import time
import pandas as pd
from pyspark.sql.functions import col, when, explode, lit
import json
from unicodedata import normalize 
import datetime
import re
from core.string_utils import normalize_replace
from pyspark.sql.functions import concat, lit, col
from datetime import date, datetime

# COMMAND ----------

import os
import datetime
import crawler.functions as cf
import pyspark.sql.functions as f
import re
from core.redis import call_redis
from datetime import date, datetime
from unicodedata import normalize
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

var_dls = notebook_params.var_dls
var_adf = notebook_params.var_adf
var_param_folder = notebook_params.var_file


# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']

# COMMAND ----------

var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_param_folder['namespace'], file_folder=var_param_folder['file_folder'])
var_source

# COMMAND ----------

def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())

# COMMAND ----------

def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)

# COMMAND ----------

def __get_last_year_PRM(input_dict, target_value):
  filtered_values = [int(key) for key in input_dict.keys() if  int(key) < target_value]
  filtered_values = sorted(filtered_values, reverse=True)
  if len(filtered_values) == 0:
    return None
  return filtered_values[0]

# COMMAND ----------

def replace_invalid_values(df):
    for column in df.columns:
        df = df.withColumn(column, f.when(f.trim(f.col(column)) == "{ñ class}", None).otherwise(f.col(column)))

    return df

# COMMAND ----------

var_file_prm = "{path_prefix}{prm_file}".format(path_prefix=var_dls['path_prefix'], prm_file=var_param_folder['prm_path'])
var_file_prm

# COMMAND ----------

adl_lnd_path = '{adl_path}{lnd_path}/{namespace}/{file_folder}'.format(adl_path=var_adls_uri, lnd_path=lnd, namespace=var_param_folder['namespace'], file_folder=var_param_folder['file_folder'])


adl_lnd_path

# COMMAND ----------


var_year = var_param_folder['file_folder'].rstrip('/').split('/')[-1]
var_year

# COMMAND ----------

var_path = var_param_folder['file_folder'].replace(var_year,'').rstrip('/')

# COMMAND ----------

var_sink = "{adl_path}{raw}/usr/{namespace}/{file_folder}".format(adl_path=var_adls_uri, raw=raw, 
                                                                   namespace=var_param_folder['namespace'], 
                                                                   file_folder=var_path)
var_sink

# COMMAND ----------


print(f'{datetime.now()} - INICIO DO PROCESSAMENTO.')
print(f'============================')

print(f'{datetime.now()} > Processando o ano {var_year}.')
#try:

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, var_file_prm, headers=headers)

#verifica se o ano existe no PRM atual
var_prm_year = None
if var_year not in var_prm_dict:
  
  print(f'{datetime.now()} >> O ano {var_year} não existe no PRM atual.')
  
  print(f'{datetime.now()} >> Verificando o mapeamento do ano anterior a {var_year}.')
  #recupera o ano anterior do PRM para tentar executar
  var_prm_year = __get_last_year_PRM(var_prm_dict, int(var_year))
  
  if var_prm_year is None:
    print(f'{datetime.now()} >> Fornçando erro por não ter mapeamento no PRM.')
    raise ValueError("Não foi possível processar, pois não há mapeamento no PRM.")
  
  print(f'{datetime.now()} >> Tentando processar utilizando o mapeamento do ano {var_prm_year}.')

var_source_txt_files = f'{adl_lnd_path}/*.txt'

print(f'{datetime.now()} >> Carregando arquivos do ano {var_year} do path {var_source_txt_files}.')
df = spark.read.csv(path=var_source_txt_files, sep=';', encoding='iso-8859-1', header=True)
qtd = df.count()
print(f'{datetime.now()} >> O {var_year} gerou um dataframe com {qtd} linhas.')

# COMMAND ----------

print(f'{datetime.now()} >> Normalizando as colunas do arquivo.')
for column in df.columns:
  df = df.withColumnRenamed(column, __normalize_str(column))

print(f'{datetime.now()} >> Substituindo valores não classificados por null.')
df = replace_invalid_values(df)

var_prm_year = var_prm_year if var_prm_year is not None else var_year

print(f'{datetime.now()} >> Verificando se existem variáveis não mapeadas no PRM.')


list_of_prm_columns = [row[0] for row in var_prm_dict[str(var_prm_year)]]

for column in set(df.columns).difference(list_of_prm_columns):
  print(f'{datetime.now()} >>> Variável {column} não mapeada no PRM. Adicionando ao dicionário como string.')
  var_prm_dict[var_prm_year].append(f"['{column}','{column}','string']")

# COMMAND ----------

print(f'{datetime.now()} >> Selecionando os campos mapeados do PRM. {var_prm_year}')
cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=str(var_prm_year))
df = df.select(*__select(var_prm_dict, str(var_prm_year)))

# COMMAND ----------

print(f'{datetime.now()} >> Aplicando as regras de negócio de transformação.')
    
df = (df
  .withColumn('ANO', f.lit(var_year).cast('Int'))
  .withColumn("CD_CNAE20_DIVISAO", f.substring("CD_CNAE20_CLASSE", 0, 2))
  .withColumn("CD_CBO4", f.substring("CD_CBO", 0, 4))
  .withColumn("CD_UF", substring("CD_MUNICIPIO", 0, 2))
)

print(f'{datetime.now()} >> Inserindo os campos de controle.')

dh_insertion_raw = var_adf["adf_trigger_time"].split(".")[0]
df = cf.append_control_columns(df, dh_insertion_raw)

df_adl_files = cf.list_adl_files(spark, dbutils, var_source)
df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

# COMMAND ----------


qtd2 = df.count()
print(f'{datetime.now()} >> Salvando arquivo parquet com {qtd2} linhas.')
df.repartition(40).write.partitionBy('ANO').parquet(path=var_sink, mode='overwrite')
print(f'{datetime.now()} >> Sucesso no processamento do ano {var_year}.')
  
print(f'{datetime.now()} - FIM DO PROCESSAMENTO.')
print(f'===========================')