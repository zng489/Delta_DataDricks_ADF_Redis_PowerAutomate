# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

import crawler.functions as cf
import json
import re
from unicodedata import normalize

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(dbutils.widgets.get("dls").replace("\'",'\"'))
var_adf = json.loads(dbutils.widgets.get("adf").replace("\'",'\"'))

# COMMAND ----------

var_adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"org_raw_tse_resultados_2022","adf_pipeline_run_id":"181ed1d1-3bce-471e-b9fb-f2062f021296","adf_trigger_id":"9a3958e25cd44c95bb1be066e9ee53d2","adf_trigger_name":"Sandbox","adf_trigger_time":"2023-08-18T21:16:02.5764238Z","adf_trigger_type":"Manual"}

var_dls = {"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"}

var_file = { 'namespace': '/oni', 'file_folder': '/pesquisas/scg/bd_satisfacao', 'file_folder_raw':'/oni/pesquisas/scg/bd_satisfacao', 'extension': 'csv', 'column_delimiter': ';', 'encoding': 'UTF-8', 'null_value': ''}

# COMMAND ----------

uld_path = f"{var_adls_uri}{var_dls['folders']['landing']}{var_file['namespace']}{var_file['file_folder']}"
raw_path = f"{var_adls_uri}{var_dls['folders']['raw']}/{var_dls['systems']['raw']}{var_file['file_folder_raw']}"

# COMMAND ----------

import crawler.functions as cf

validation = f"{var_dls['folders']['landing']}{var_file['namespace']}{var_file['file_folder']}"
if not cf.directory_exists(dbutils, validation):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % validation)

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("encoding", "UTF-8").option('sep', ',').load(uld_path, mode="PERMISSIVE", ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True,inferSchema=True)

# COMMAND ----------

dh_insertion_raw = var_adf["adf_trigger_time"].split(".")[0]
df = cf.append_control_columns(df, dh_insercao_raw=dh_insertion_raw)

# COMMAND ----------

df.write.format('parquet').save(raw_path, header = True, mode='overwrite')
