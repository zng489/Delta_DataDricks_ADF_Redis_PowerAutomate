# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

file = {'namespace':'/oni', 'file_folder':'/brazil_startups_on_startupblink/',          'file_subfolder':'', 'file_name':'Brazil_Startups_on_StartupBlink_2025_1.csv', 'sheet_name':'',         'raw_path':'/usr/oni/base_startups_brasil/',         'prm_path': '',          'extension':'csv','column_delimiter':',','encoding':'utf-8','null_value':''}
        
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }


# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

import os
import re
import datetime
from unicodedata import normalize

import pandas as pd
import pyspark.pandas as ps
import crawler.functions as cf
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index, col, when, explode, concat

from core.string_utils import normalize_replace

# COMMAND ----------

file = notebook_params.var_file
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

file_name = file['file_name']
sheet_name = file['sheet_name']

uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

print(file_name)
print(sheet_name)

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
print(adl_uld)

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
print(adl_raw)

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
print(prm_path)

# COMMAND ----------

if not cf.directory_exists(dbutils, uld_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % uld_path)


def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())

# COMMAND ----------

df = (spark.read
                .option("delimiter", ",")
                .option("header", "true")
                .option("encoding", "utf-8")
                .csv(adl_uld))

# COMMAND ----------

# Mapeamento: Campo Origem -> Campo Destino
col_rename_map = {
    "NOME_DA_STARTUP": "NM_STARTUP",
    "SITE_DA_EMPRESA": "NM_SITE_EMPRESA",
    "NOME_DO_SETOR_INDUSTRIAL": "NM_SETOR_INDUST",
    "NOME_DA_ATUAÇÃO_DA_STARTUP_NO_SETOR_INDUSTRIAL": "NM_ATU_STARTUP_SETOR_INDUST",
    "DESCRICAO_MODELO_DE_NEGOCIO": "DES_MODELO_NEGOCIO",
    "NOME_DA_REGIAO_BRASILEIRA_DA_STARTUP": "NM_REG_STARTUP",
    "NOME_DA_ESTADO_BRASILEIRO_DA_STARTUP": "NM_EST_STARTUP",
    "NOME_DA_CIDADE_BRASILEIRA_DA_STARTUP": "NM_CID_STARTUP"
}

# Suponha que seu DataFrame PySpark se chame "df"
for origem, destino in col_rename_map.items():
    df = df.withColumnRenamed(origem, destino)

# COMMAND ----------

df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))
df.write.parquet(path=adl_raw, mode='overwrite')