# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")


# COMMAND ----------

import json
import re
import crawler.functions as cf
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import IntegerType
import datetime
from datetime import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from trs_control_field import trs_control_field as tcf
from pyspark.sql.types import IntegerType

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

table = {"copy_sqldw":"false",
 "params":"oni/pesquisas/scg/absenteismo",
 "path_destination":"oni/painel_indicadores_scg/absenteismo",
 "destination":"/oni/painel_indicadores_scg/absenteismo",
 "databricks":{"notebook":"/biz/oni/painel_indicadores_scg/trs_biz_absenteismo"}}

adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"trs_biz_rfb_cno","adf_pipeline_run_id":"c158e9dd-98df-4f7b-a64d-d13c76669868","adf_trigger_id":"67c514a7245449b984eb4aadd55bfbff","adf_trigger_name":"Sandbox","adf_trigger_time":"2023-08-22T21:22:42.5769844Z","adf_trigger_type":"Manual"}
 
dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

absenteismo_path = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['params'])

# COMMAND ----------

df = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(absenteismo_path, mode="FAILFAST")

# COMMAND ----------

df = tcf.add_control_fields(df, adf, layer="biz")

# COMMAND ----------

df.display()

# COMMAND ----------

var_sink = "{adl_path}{biz}/{path_destination}/".format(adl_path=var_adls_uri, biz=dls['folders']['business'], path_destination=table["path_destination"])
print(var_sink)

# COMMAND ----------

df.write.format('parquet').save(var_sink, header = True, mode='overwrite')

# COMMAND ----------


