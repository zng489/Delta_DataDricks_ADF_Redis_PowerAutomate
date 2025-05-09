# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index

from trs_control_field import trs_control_field as tcf
import crawler.functions as cf
from unicodedata import normalize 
import pyspark.sql.functions as f
import datetime
import json
import re


var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

var_file = {
   'namespace': 'oni',
   'file_folder': 'oms/cid_10/subcategorias',
   'file_folder_biz':'bases_referencia/oms/cid_10/subcategorias',
   'extension': 'csv',
   'column_delimiter': ';',
   'encoding': 'UTF-8',
   'null_value': ''
 }
 
var_adf = {
   "adf_factory_name": "cnibigdatafactory",
   "adf_pipeline_name": "org_raw_base_escolas",
   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
   "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
   "adf_trigger_type": "PipelineActivity"
 }
 
var_dls = {"folders":{"landing":"/tmp/dev/uld", "error":"/tmp/dev/err/", "staging":"/tmp/dev/stg/", "log":"/tmp/dev/log/", "raw":"/tmp/dev/raw", "trusted":"/tmp/dev/trs" , "business":"/tmp/dev/biz"},"systems":{"raw":"usr"}}
 
#files = {
#   'namespace': 'uniepro',
#   'file_folder': 'base_escolas',
#   'extension': 'csv',
#   'column_delimiter': ';',
#   'encoding': 'UTF-8',
#   'null_value': ''
# }
#           
#databricks = {"notebook":"uniepro/rfb_siafi org_raw_rfb_siafi"}

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']
trs = var_dls['folders']['trusted']
biz = var_dls['folders']['business']
#sys = var_dls['systems']['raw']

# COMMAND ----------

var_source = "{trs}/{namespace}/{file_folder}/".format(trs=trs, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
print(var_source)


var_sink = "{adl_path}{biz}/{namespace}/{file_folder}/".format(adl_path=var_adls_uri, biz=biz, namespace=var_file['namespace'], file_folder=var_file['file_folder_biz'])
print(var_sink)

# COMMAND ----------

import crawler.functions as cf
 
if not cf.directory_exists(dbutils, var_source):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % var_source)

# COMMAND ----------

df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").load(var_adls_uri + var_source, mode="FAILFAST", ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True,inferSchema=True)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(var_adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.coalesce(1).write.format('parquet').save(var_sink, header = True, mode='overwrite')

# COMMAND ----------


