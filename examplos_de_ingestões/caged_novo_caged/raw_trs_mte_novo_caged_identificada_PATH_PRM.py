# Databricks notebook source
# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

tables = {"schema":"","table":"","raw_path":"/usr/oni/mte/novo_caged/identificada/","trusted_path":"/oni/mte/novo_caged/identificada/","prm_path":"/usr/oni/mte/novo_caged_identificada/FIEC_me_novo_caged_exc_for_mov_mapeamento_unificado_trusted.xlsx"}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

#dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}
dls = {"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"}

# COMMAND ----------
# COMMAND ----------

from cni_connectors import adls_connector as adls_conn
var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType, IntegerType
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

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
trusted = dls['folders']['trusted']
prm_path = tables['prm_path']

# COMMAND ----------

raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
adl_raw


trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
adl_trusted

# COMMAND ----------

prm_path = "{path_dls}{prm_path}".format(path_dls = dls['folders']['prm'], prm_path=tables['prm_path'])
prm_path

# COMMAND ----------

df = spark.read.parquet(adl_raw)

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)
 
# NOVO_CAGED_EXC = NOVO_CAGED_EXC_FOR_MOV
# Problemas e falta de lógica na função parse_ba_doc Logo foi obrigado a trocar os valores

sheet_name='NOVO_CAGED_EXC'

cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet='NOVO_CAGED_EXC')

df = df.select(*__transform_columns())

# COMMAND ----------

df = df.withColumn('CD_ANO_COMPETENCIA', f.substring(f.col('CD_ANO_COMPETENCIA'), 0, 4).cast(IntegerType()))


# COMMAND ----------

df = df.withColumn('CD_SUBCLASSE', f.when(f.length(f.col('CD_CNAE20_SUBCLASSE'))<7, f.lpad('CD_CNAE20_SUBCLASSE', 7, "0")).otherwise(f.col('CD_CNAE20_SUBCLASSE')))

# COMMAND ----------

df = tcf.add_control_fields(df, adf)

# COMMAND ----------

df.write.partitionBy('CD_ANO_COMPETENCIA','CD_ANO_MES_COMPETENCIA_MOVIMENTACAO').mode('overwrite', ).parquet(path=adl_trusted, mode='overwrite')
