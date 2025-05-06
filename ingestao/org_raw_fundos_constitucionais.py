# Databricks notebook source

file = {"namespace":"/oni", "file_folder":'/midr', 
        "file_subfolder":'/dados_fundos_constitucionais/',
        "raw_path":"/usr/oni/midr/",
        "prm_path": "", 
        "extension":"txt","column_delimiter":"","encoding":"iso-8859-1","null_value":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

# Biblioteca cni_connectors, que dá acesso aos dados no datalake
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
from pyspark.sql.functions import concat, lit, col

# COMMAND ----------

file = notebook_params.var_file
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
adl_uld

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
adl_raw

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
prm_path

adl_raw

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

from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BinaryType, BooleanType, ArrayType, MapType
import re
from pyspark.sql import DataFrame

def load_csv_to_dataframe(file_path: str) -> DataFrame:
    return (spark.read
                .option("delimiter", ";")
                .option("header", "true")
                .option("encoding", "utf-8")
                .csv(file_path))

def process_files(base_path: str, parameter: str):
    dataframes = []  
    for path in cf.list_subdirectory(dbutils, base_path):
        for file_path in cf.list_subdirectory(dbutils, path):
            for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
                if arquivos_txt.endswith('.csv') and parameter in file_path:
                    df = load_csv_to_dataframe(f"{var_adls_uri}/{arquivos_txt}")
                    #print('*************************************')
                    #print(parameter)
                    #print(df.columns)
                    #print(df.count())
                    dataframes.append(df)
    if dataframes:
        final_df = dataframes[0]
        for df in dataframes[1:]:
            final_df = final_df.unionByName(df, allowMissingColumns=True)
        return final_df
    else:
        print("No CSV files found.")
        return None

# COMMAND ----------

from collections import Counter
import datetime

parameters = ['FCO/Carteira', 'FCO/Contratações', 'FCO/Desembolsos', 'FNE/Carteira', 'FNE/Contratações', 'FNE/Desembolsos', 'FNO/Carteira', 'FNO/Contratações', 'FNO/Desembolsos']

for parameter in parameters:
    df = process_files(uld_path, parameter)
    for column in df.columns:
        df = df.withColumnRenamed(column, __normalize_str(column))


    cols = df.schema.names
    df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
    for i in range(len(cols)):
        if cols.count(cols[i])>1:
            df.schema[i].name = cols[i]+'_dup'
            cols[i] = cols[i]+'_dup'
    output = spark.createDataFrame([], df.schema).union(df)


    #print(f'{adl_raw}{parameter}')
    #print(output.columns)
    output.write.parquet(path=f'{adl_raw}{parameter}', mode='overwrite')