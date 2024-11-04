# Databricks notebook source
tables = {"schema":"", "table":"", "raw_path":"/usr/oni/mte/rais/rais_estabelecimentos_2018_2023/","trusted_path":"/oni/mte/rais/rais_estabelecimentos_2018_2023/", "prm_path": "/prm/usr/oni/mte/rais_publica_estabelecimento/FIEC_mte_rais_publica_estabelecimento_mapeamento_unificado_trusted.xlsx"}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

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
from pyspark.sql.functions import concat, lit, col, substring

# COMMAND ----------

# Esses são os dicionários de configuração da transformação enviados pelo ADF e acessados via widgets. Os diretórios de origem e destino das tabelas são compostos por valores em 'dls' e 'tables'.
# Parametros necessario para na ingestão

#tables = notebook_params.var_tables
#dls = notebook_params.var_dls
#adf = notebook_params.var_adf

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
print(adl_raw)

# COMMAND ----------

trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
print(adl_trusted)

# COMMAND ----------

# prm_path = tables['prm_path']
prm_path = '/tmp/dev/prm/usr/oni/mte/rais_estabelecimento_2018_2023/mte_rais_estabelecimento_mapeamento_unificado_trusted.xlsx'
prm_path

# COMMAND ----------

headers = {
  'name_header': 'Campo Origem',
  'pos_header': 'C',
  'pos_org': 'C',
  'pos_dst': 'E',
  'pos_type': 'F'
}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

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

# sheet_name = table['table'].upper()
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
df.write.mode('overwrite').partitionBy('NR_ANO').parquet(path=adl_trs)
