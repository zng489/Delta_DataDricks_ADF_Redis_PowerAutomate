# Databricks notebook source
dbutils.widgets.text('user_parameters', '{"null": "null"}')

dbutils.widgets.text('env', 'dev')

dbutils.widgets.text('storage', '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


# COMMAND ----------

tables = {"schema":"oni/mongodb_monitor","table":"investimento","raw_path":"/crw/oni/observatorio_nacional/monitor_investimentos/painel_monitor/mongodb_monitor__investimento","prm_path":""}

adf = { 'adf_factory_name': 'cnibigdatafactory', 'adf_pipeline_name': 'raw_trs_tb_email', 'adf_pipeline_run_id': '61fc4f3c-c592-426d-bb36-c85cb184bb82', 'adf_trigger_id': '92abb4ec-2b1f-44e0-8245-7bc165f91016', 'adf_trigger_name': '92abb4ec-2b1f-44e0-8245-7bc165f91016', 'adf_trigger_time': '2024-05-07T00:58:48.0960873Z', 'adf_trigger_type': 'PipelineActivity' }

dls = {'folders':{'landing':'/tmp/dev/lnd','error':'/tmp/dev/err','archive':'/tmp/dev/ach','staging':'/tmp/dev/stg','log':'/tmp/dev/log','raw':'/tmp/dev/raw','trusted':'/tmp/dev/trs','business':'/tmp/dev/biz','prm':'/tmp/dev/prm','historico':'/tmp/dev/hst','gov':'/tmp/dev/gov'},'path_prefix':'tmp','uld':{'folders':{'landing':'/tmp/dev/uld','error':'/tmp/dev/err','staging':'/tmp/dev/stg','log':'/tmp/dev/log','raw':'/tmp/dev/raw','archive':'/tmp/dev/ach'},'systems':{'raw':'usr'},'path_prefix':'/tmp/dev/'},'systems':{'raw':'usr'}}

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
# MAGIC ### Raw specific parameter section

# COMMAND ----------

tables = notebook_params.var_tables 
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

prm = dls['folders']['prm']
lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

# lnd_path = "{lnd}{type_raw}{lnd_path}".format(lnd=dls['folders']['landing'], type_raw=tables['type_raw'], lnd_path=tables['lnd_path'])
# lnd_path = f'crw/{table["schema"]}__{table["table"]}'
# tmp/dev/lnd/crw/oni/mongodb_monitor__investimento/
# lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=dls['folders']['landing'],schema=tables['schema'], table=tables['table'])
lnd_path = os.path.join(lnd, f'crw/{tables["schema"]}__{tables["table"]}')
adl_lnd = f"{var_adls_uri}{lnd_path}"


raw_path = "{raw}{raw_path}".format(raw=raw, raw_path=tables['raw_path'])
adl_raw = f"{var_adls_uri}{raw_path}"


# COMMAND ----------

print(adl_lnd)

# COMMAND ----------

print(adl_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply transformations and save dataframe

# COMMAND ----------

df = spark.read.option("mergeSchema", "true").parquet(adl_lnd)

# COMMAND ----------

dh_insercao_raw = adf['adf_trigger_time']
if dh_insercao_raw is not None:
  dh_insercao_raw = dh_insercao_raw.split(".")[0]

# COMMAND ----------

df = cf.append_control_columns(df, dh_insercao_raw=dh_insercao_raw)

# COMMAND ----------

df.write.mode('overwrite').parquet(path=adl_raw)
