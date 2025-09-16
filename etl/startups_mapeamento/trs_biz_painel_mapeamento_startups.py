# Databricks notebook source
# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

tables = {"schema":"", "table":"", 
          "trusted_path_1":"/oni/base_startups_brasil/",          
          "destination":"/oni/painel_mapeamento_startups/", 
          "databricks":{"notebook":"/biz/oni/painel_mapeamento_startups/trs_biz_painel_mapeamento_startups"}, 
          "prm_path":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

# COMMAND ----------

# Configurações iniciais
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import (
    col, substring, lpad, when, lit, sum, trim, concat, 
    regexp_replace, round, format_number
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType
from trs_control_field import trs_control_field as tcf
from functools import reduce
import pandas as pd
import re
import os

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

trusted = dls['folders']['trusted']
business = dls['folders']['business']
sink = dls['folders']['business']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

# COMMAND ----------

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'
print(adl_trusted_1)

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination'])
adl_destination_path = f'{var_adls_uri}{destination_path}'
print(adl_destination_path)


# COMMAND ----------

# COMMAND ----------

df = spark.read.parquet(adl_trusted_1)

# COMMAND ----------

df = df.drop('dh_insercao_trs')
final_df = tcf.add_control_fields(df, adf, layer="biz")
final_df.write.mode('overwrite').parquet(adl_destination_path, compression='snappy')

# COMMAND ----------

