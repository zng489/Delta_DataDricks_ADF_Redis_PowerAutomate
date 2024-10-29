# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

tables = {"schema":"", "table":"", "raw_path":"/usr/oni/observatorio_nacional/monitor_investimentos/painel_monitor/","trusted_path":"/oni/observatorio_nacional/monitor_investimentos/painel_monitor/","prm_path":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn
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
import os
from core.string_utils import normalize_replace

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
trusted = dls['folders']['trusted']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

# raw_path = "{raw}{schema}{table}{raw_path}".format(raw=dls['folders']['raw'], schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
# raw_path = "{raw}{raw_path}".format(raw=raw, raw_path=tables['raw_path'])
raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
print(adl_raw)


# trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
# trusted_path = "{trusted}{trusted_path}".format(trusted=trusted, trusted_path=tables['trusted_path'])
trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
print(adl_trusted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply transformations and save dataframe

# COMMAND ----------

for file_path in cf.list_subdirectory(dbutils, raw_path):
  print(file_path)

# COMMAND ----------

for file_path in cf.list_subdirectory(dbutils, raw_path):
  file_name = file_path.split('/')[-1]
  df = spark.read.parquet(f'{adl_raw}{file_name}')
  print(f'{adl_trusted}{file_name}')
  for c in df.columns:
    df = df.withColumnRenamed(c, re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', c).encode('ASCII', 'ignore').decode('ASCII').replace(' ', '_').replace('-', '_').upper()))

  df = tcf.add_control_fields(df, adf)
  df.write.parquet(path=f'{adl_trusted}{file_name}', mode='overwrite')
