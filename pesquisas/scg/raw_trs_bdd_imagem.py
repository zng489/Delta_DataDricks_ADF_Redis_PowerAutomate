# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

import crawler.functions as cf
from trs_control_field import trs_control_field as tcf
import json
import pyspark.sql.functions as f
from pyspark.sql.types import *
import re

# COMMAND ----------

try:
  table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
  dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
  adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
except:
  table = {"schema":"oni/pesquisas/scg","table":"bdd_imagem"}

  adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"raw_trs_pnadc_a_visita5_f","adf_pipeline_run_id":"04a40e47-07bd-4415-a3a9-2b77158f490b","adf_trigger_id":"7adb91d09feb444d9c383c002feea0d0","adf_trigger_name":"Sandbox","adf_trigger_time":"2023-06-28T13:41:09.760834Z","adf_trigger_type":"Manual"}
  
  dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

path_raw = f"{var_adls_uri}{dls['folders']['raw']}/{dls['systems']['raw']}/{table['schema']}/{table['table']}"
print(path_raw)

path_trs = f"{var_adls_uri}{dls['folders']['trusted']}/{table['schema']}/{table['table']}"
print(path_trs)

# COMMAND ----------

df = spark.read.format("parquet").load(path_raw)
df = tcf.add_control_fields(df, adf)

# COMMAND ----------

df.write.format('parquet').save(path_trs, header = True, mode='overwrite') 

# COMMAND ----------

df.display()

# COMMAND ----------


