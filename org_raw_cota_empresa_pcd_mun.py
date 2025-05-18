# Databricks notebook source
'''
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')
'''

# COMMAND ----------

'''
file = {"namespace":"/oni", "file_folder":"/observatorio_nacional", 
        "file_subfolder":"/cota_empresa_pcd/", 
        "raw_path":"/usr/oni/observatorio_nacional/cota_empresa_pcd/",
        "prm_path": "", 
        "extension":"csv","column_delimiter":"","encoding":"iso-8859-1","null_value":""}
        
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }
'''

# COMMAND ----------

'''
from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

# PySpark imports
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import (
    udf, 
    from_utc_timestamp, 
    current_timestamp, 
    lit, 
    input_file_name, 
    monotonically_increasing_id, 
    substring_index,
    to_date,
    regexp_replace,
    col,
    when,
    explode,
    concat
)
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    IntegerType, 
    DoubleType, 
    DateType, 
    TimestampType, 
    BinaryType, 
    BooleanType, 
    ArrayType, 
    MapType
)

# Custom imports
import crawler.functions as cf
from core.string_utils import normalize_replace

# Other utility imports
import pandas as pd
import json
import time
import datetime
import re
from unicodedata import normalize
'''

# COMMAND ----------

'''
file = notebook_params.var_file
dls = notebook_params.var_dls
adf = notebook_params.var_adf
'''

# COMMAND ----------

'''
uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
print(adl_uld)

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
print(adl_raw)

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
print(prm_path)
'''

# COMMAND ----------

'''
if not cf.directory_exists(dbutils, uld_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % uld_path)
'''

# COMMAND ----------

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

dataframes = []
for path in cf.list_subdirectory(dbutils, uld_path):
  for file_path in cf.list_subdirectory(dbutils, path):
    for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
      if arquivos_txt.endswith('.csv'):
        print(f'  Explorando subdiretório: {arquivos_txt}')
        df = (spark.read
                      .option("delimiter", ";")
                      .option("header", "true")
                      .option("encoding", "iso-8859-1")
                      .csv(f"{var_adls_uri}/{file_path}")
                      )
        print('There is file here')
        for column in df.columns:
          df = df.withColumnRenamed(column, __normalize_str(column))
        dataframes.append(df)
      else:
        print(f'There is no file txt in {path}')

if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

final_df.display()

# COMMAND ----------


df = cf.append_control_columns(final_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
#dh_insercao_raw = datetime.datetime.now()
#df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

adl_raw

# COMMAND ----------

df.write.parquet(path=adl_raw, mode='overwrite')

# COMMAND ----------

