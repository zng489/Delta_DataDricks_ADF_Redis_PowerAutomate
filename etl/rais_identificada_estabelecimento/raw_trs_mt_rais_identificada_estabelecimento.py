# Databricks notebook source
tables = {"schema":"","table":"","raw_path":"/usr/oni/mte/rais/identificada/rais_estabelecimento/","trusted_path":"/oni/mte/rais/identificada/rais_estabelecimento/","prm_path":"/tmp/dev/prm/usr/oni/mte/rais_identificada_estabelecimento/mte_rais_estabelecimento_mapeamento_unificado_trusted.xlsx"}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------


dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn
var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index, regexp_replace, lower
from pyspark.sql.functions import col, lit, lower, regexp_replace, when, months_between, to_date, lpad
from pyspark.sql.functions import col, lower, lit, substring, when
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
import os
from core.string_utils import normalize_replace
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
print(raw)

trusted = dls['folders']['trusted']
print(trusted)

prm_path = tables['prm_path']

usr = dls['systems']['raw']
print(usr)

# COMMAND ----------

raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
adl_raw

# COMMAND ----------

trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
adl_trusted

# COMMAND ----------

prm_path = tables['prm_path']
prm_path

# COMMAND ----------

name = 'RAIS IDENTIFICADA ESTAB'

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path, headers=headers, sheet_names=name)

df = spark.read.parquet(adl_raw)

# COMMAND ----------

def __select(parse_ba_doc, sheet_name):
  for org, dst, _type in parse_ba_doc[sheet_name]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)    
      
df = df.select(*__select(var_prm_dict, name)).drop("CD_UF")

# COMMAND ----------


from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

df = df.withColumn("ID_CEPAO_ESTAB", col("ID_CEPAO_ESTAB").cast(DoubleType()))

# COMMAND ----------

# Transformations 

df = df.withColumn('CD_CNAE20_CLASSE', lpad(df['CD_CNAE20_CLASSE'], 5, '0'))
df = df.withColumn('CD_CNAE20_SUBCLASSE', lpad(df['CD_CNAE20_SUBCLASSE'], 7, '0'))
df = df.withColumn('CD_CNAE10_CLASSE', lpad(df['CD_CNAE10_CLASSE'], 5, '0'))

# COMMAND ----------

transformation_all = {"CD_CNAE20_DIVISAO": substring("CD_CNAE20_CLASSE", 0, 2), 
                      "CD_UF": substring("CD_MUNICIPIO", 0, 2)
                     }

for key in transformation_all:
  df = df.withColumn(key, transformation_all[key])

# COMMAND ----------

# Limpar espaços extras nas colunas do tipo string:
for column in df.columns:
  if df.select(column).dtypes[0][1] == 'string':
    df = df.withColumn(column, f.trim(f.col(column)))

# Replace empty strings with null values:
df = df.replace('', None)

# Replace "{ñ class}" and variations with null values:
values_to_replace = [
  '{ñ class}',
  '{ñ class',#
  '{ñ clas',
  '{ñ cla',#
  '{ñ cl',
  '{ñ c',
  '{ñ'
]
for value in values_to_replace:
  df = df.replace(value, None)

# Extract the first two digits
df = df.withColumn("SG_UF", substring(col("CD_MUNICIPIO").cast("string"), 1, 2))

map_dict = {
  '11': 'RO',
  '12': 'AC',
  '13': 'AM',
  '14': 'RR',
  '15': 'PA',
  '16': 'AP',
  '17': 'TO',
  '21': 'MA',
  '22': 'PI',
  '23': 'CE',
  '24': 'RN',
  '25': 'PB',
  '26': 'PE',
  '27': 'AL',
  '28': 'SE',
  '29': 'BA',
  '31': 'MG',
  '32': 'ES',
  '33': 'RJ',
  '35': 'SP',
  '41': 'PR',
  '42': 'SC',
  '43': 'RS',
  '50': 'MS',
  '51': 'MT',
  '52': 'GO',
  '53': 'DF'
}
df = df.replace(map_dict, subset=['SG_UF'])

map_dict = {
  '01': 'ZERO',
  '02': 'ATE 4',
  '03': 'DE 5 A 9',
  '04': 'DE 10 A 19',
  '05': 'DE 20 A 49',
  '06': 'DE 50 A 99',
  '07': 'DE 100 A 249',
  '08': 'DE 250 A 499',
  '09': 'DE 500 A 999',
  '10': '1000 OU MAIS',
  '-1': 'IGNORADO'
}
df = df.replace(map_dict, subset=['DS_TAMANHO_ESTABELECIMENTO'])

df.createOrReplaceTempView("df")

columns_to_select = [col for col in df.columns if col != 'ANO']

columns_to_select.append("ANO AS NR_ANO")

select_query = f"SELECT {', '.join(columns_to_select)} FROM df"

df = spark.sql(select_query)



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

df = df.withColumn("DT_BAIXA", lpad(col("DT_BAIXA"), 8, "0")) \
    .withColumn("DT_ABERTURA", lpad(col("DT_ABERTURA"), 8, "0")) \
    .withColumn("DT_ENCERRAMENTO", lpad(col("DT_ENCERRAMENTO"), 8, "0"))


# Converte para tipo data (DateType)

df_ = df.withColumn("DT_BAIXA", to_date(col("DT_BAIXA"), "ddMMyyyy")) \
                  .withColumn("DT_ABERTURA", to_date(col("DT_ABERTURA"), "ddMMyyyy")) \
                  .withColumn("DT_ENCERRAMENTO", to_date(col("DT_ENCERRAMENTO"), "ddMMyyyy"))

# COMMAND ----------

df_.display()

# COMMAND ----------

for path in cf.list_subdirectory(dbutils, trusted_path):
  #print(f"{var_adls_uri}/{path}")
  for item in dbutils.fs.ls(f"{var_adls_uri}/{path}"):
    if item.name == "_SUCCESS" and not item.isDir():
        print(f"Deletando arquivo: {item.path}")
        dbutils.fs.rm(item.path, recurse=False)

# COMMAND ----------

df = tcf.add_control_fields(df_, adf)
df.write.mode('overwrite').partitionBy('NR_ANO').parquet(path=adl_trusted)

# COMMAND ----------

