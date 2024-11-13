# Databricks notebook source
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
from pyspark.sql.functions import concat, lit, col, substring

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
raw

trusted = dls['folders']['trusted']
trusted

prm_path = tables['prm_path']

usr = dls['systems']['raw']
usr

# COMMAND ----------

raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
adl_raw

# COMMAND ----------

trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
adl_trusted

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path, headers=headers, sheet_names='RAIS_Identifiad_ESTABELECIMENTO')


# COMMAND ----------

df = spark.read.parquet(adl_raw)

# COMMAND ----------

def __transform_columns(var_prm_dict__sheet_name: dict):
  for org, dst, _type in var_prm_dict__sheet_name:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

sheet_name = 'RAIS_Identifiad_ESTABELECIMENTO'

cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=sheet_name)
df = df.select(*__transform_columns(var_prm_dict[sheet_name]))

# COMMAND ----------

# var_prm_dict

# COMMAND ----------

# Limpar espaços extras nas colunas do tipo string:
for column in df.columns:
  if df.select(column).dtypes[0][1] == 'string':
    df = df.withColumn(column, f.trim(f.col(column)))

# Replace empty strings with null values:
df = df.replace('', None)

# COMMAND ----------

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

# COMMAND ----------

# Extract the first two digits
df = df.withColumn("SG_UF", substring(col("CD_MUNICIPIO").cast("string"), 1, 2))

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

df.createOrReplaceTempView("df")

columns_to_select = [col for col in df.columns if col != 'ANO']

columns_to_select.append("ANO AS NR_ANO")

select_query = f"SELECT {', '.join(columns_to_select)} FROM df"

df = spark.sql(select_query)


# COMMAND ----------

df = tcf.add_control_fields(df, adf)
df.write.mode('overwrite').partitionBy('NR_ANO').parquet(path=adl_trusted)
