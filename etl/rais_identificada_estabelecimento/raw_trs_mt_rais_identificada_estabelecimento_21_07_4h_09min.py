# Databricks notebook source
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

df = tcf.add_control_fields(df, adf)

# COMMAND ----------

df.write.mode('overwrite').partitionBy('NR_ANO').parquet(path=adl_trusted)
