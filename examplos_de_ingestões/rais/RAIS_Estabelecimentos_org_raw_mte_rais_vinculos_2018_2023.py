# Databricks notebook source
file = {"namespace":"/oni", "file_folder":"/mte/rais", 
        "file_subfolder":"/rais_vinculo_2018_2023/", 
        "raw_path":"/usr/oni/mte/rais/rais_vinculo_2018_2023/",
        "prm_path": "/tmp/dev/prm/usr/oni/mte/rais_vinculos_2018_2023/prm_rais_vinculo_2018_2023.xlsx", 
        "extension":"txt","column_delimiter":"","encoding":"","null_value":""}
                                                                            # file_subfolder: "mte/rais/publica/vinculos/2022/" sempre deixar a "/" para a leitura dos diretorios da funcao cf
# ["{'namespace':'oni','file_folder':'mte/rais/publica/vinculos/2022','prm_path':'/prm/usr/oni/mte/rais_publica_vinculos/MTE_RAIS_PUBLICA_VINCULO_mapeamento_raw.xlsx', 'extension':'TXT', 'column_delimiter': ';', 'encoding': 'UTF-8', 'null_value': ''}"]

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }


dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

# Databricks notebook source
# Biblioteca cni_connectors, que dá acesso aos dados no datalake
from cni_connectors import adls_connector as adls_conn

# A biblioteca criada para facilitar a declaração dos testes. É necessário importá-la.
### from datatest.gx_context_provider import GXContextProvider

# Essa declaração é necessária em todos os notebooks. Somente através dela podemos acessar os dados no datalake.
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

# COMMAND ----------

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
adl_uld

# COMMAND ----------

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
adl_raw

# COMMAND ----------

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
prm_path

# COMMAND ----------

cf.list_adl_files(spark, dbutils, uld_path)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import re


common_schema = StructType([
    StructField("Column1", StringType(), True),
    StructField("Column2", StringType(), True),

])

dataframes = []
for path in cf.list_subdirectory(dbutils, uld_path):
    for file_path in cf.list_subdirectory(dbutils, path):
        full_path = f"{var_adls_uri}/{file_path}"
        year = path.split('/')[-1]

        pattern = r"\d+"
        numbers = re.findall(pattern, year)[0]
    
        df = (spark.read
            .option("delimiter", ";")
            .option("header", "true")
            .option("encoding", "ISO-8859-1")
            .csv(full_path)
            ).withColumn("ANO", lit(f"{numbers}"))
        dataframes.append(df)

combined_df = dataframes[0] if dataframes else spark.createDataFrame([],  schema=None)
for df in dataframes[1:]:
    combined_df = combined_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

combined_df = combined_df.withColumnRenamed(combined_df.columns[39], "CNAE 2_0 Classe")
combined_df = combined_df.withColumnRenamed(combined_df.columns[40], "CNAE 2_0 Subclasse")


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path , headers=headers, sheet_names='RAW')

def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)

df = combined_df.select(*__select(var_prm_dict, 'RAW'))

# COMMAND ----------

df = cf.append_control_columns(combined_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = combined_df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

# COMMAND ----------

df.write.partitionBy('ANO').parquet(path=adl_raw, mode='overwrite')
