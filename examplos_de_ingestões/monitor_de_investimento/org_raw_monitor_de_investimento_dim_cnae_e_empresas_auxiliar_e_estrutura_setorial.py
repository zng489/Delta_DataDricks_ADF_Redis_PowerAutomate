# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

file = {"namespace":"/oni", "file_folder":"/observatorio_nacional/monitor_investimentos/painel_monitor", "raw_path":"/usr/oni/observatorio_nacional/monitor_investimentos/painel_monitor/", "prm_path":"", "extension":"csv","column_delimiter":"","encoding":"","null_value":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders": {"landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach"}, "systems": {"raw": "usr"}, "path_prefix": "/tmp/dev/"}

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
from core.string_utils import normalize_replace

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

file = notebook_params.var_file
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

# COMMAND ----------

uld_path = "{uld}{namespace}{file_folder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'])
adl_uld = f"{var_adls_uri}{uld_path}"
print(adl_uld)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply transformations and save dataframe

# COMMAND ----------

import crawler.functions as cf

if not cf.directory_exists(dbutils, uld_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % uld_path)

# COMMAND ----------

for files in cf.list_subdirectory(dbutils, uld_path):
  adl_uld = f"{var_adls_uri}/{files}"
  df = spark.read.format("csv").option("header","true").option("encoding", "UTF-8").option('sep', ';').load(adl_uld, mode="FAILFAST", ignoreLeadingWhiteSpace=True, 
  ignoreTrailingWhiteSpace=True,inferSchema=True)

  dh_insercao_raw = adf['adf_trigger_time']
  dh_insercao_raw = dh_insercao_raw.split(".")[0]
  df = cf.append_control_columns(df, dh_insercao_raw=dh_insercao_raw)

  file_name = files.split('/')[-1]
  raw_usr_path = "{raw}{raw_path}{file_name}".format(raw=raw, raw_path=file['raw_path'], file_name=file_name)
  adl_raw = f"{var_adls_uri}{raw_usr_path}"
  print(adl_raw)

  df.write.parquet(path=adl_raw, mode='overwrite')


