# Databricks notebook source
# Databricks notebook source
# Databricks notebook source
file = {"namespace":"/oni", "file_folder":"/mte/caged_trabalhador/", 
        "file_subfolder":"identificada/", 
        "raw_path":"/usr/oni/mte/caged_trabalhador/identificada/",
        "prm_path": "/usr/oni/mte/caged_trabalhador_identificada/mte_caged_trabalhador_identificada_raw_trs.xlsx",
        #"prm_file_name":["NOVO_CAGED_EXC", "NOVO_CAGED_FOR", "NOVO_CAGED_MOV"], 
        "extension":"txt","column_delimiter":"","encoding":"utf-8","null_value":""}
                                                                            # file_subfolder: "mte/rais/publica/vinculos/2022/" sempre deixar a "/" para a leitura dos diretorios da funcao cf
# ["{'namespace':'oni','file_folder':'mte/rais/publica/vinculos/2022','prm_path':'/prm/usr/oni/mte/rais_publica_vinculos/MTE_RAIS_PUBLICA_VINCULO_mapeamento_raw.xlsx', 'extension':'TXT', 'column_delimiter': ';', 'encoding': 'UTF-8', 'null_value': ''}"]

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"}

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

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


from pyspark.sql.functions import col, substring
from pyspark.sql.types import StringType
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BinaryType, BooleanType, ArrayType, MapType

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

prm_path = "{path_dls}{prm_path}".format(path_dls = dls['folders']['prm'], prm_path=file['prm_path'])
prm_path

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
  for nivel_1 in cf.list_subdirectory(dbutils, path):
    for nivel_2 in cf.list_subdirectory(dbutils, nivel_1):
      for arquivos_txt in cf.list_subdirectory(dbutils, nivel_2):
        if arquivos_txt.endswith('.txt'):
          print(f'  Explorando subdiretório: {arquivos_txt}')
          df = (spark.read
                        .option("delimiter", ";")
                        .option("header", "true")
                        .option("encoding", "ISO-8859-1")
                        .csv(f"{var_adls_uri}/{nivel_2}")
                        )

          for column in df.columns:
            df = df.withColumnRenamed(column, __normalize_str(column))
          dataframes.append(df)
        else:
          print(f'There is no file txt in {arquivos_txt}') 

# COMMAND ----------

if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------


sheet_name = 'CAGED_TRABALHADOR_RAW'

cf.check_ba_doc(final_df, parse_ba=var_prm_dict, sheet=sheet_name)
final_df = final_df.select(*__transform_columns())

# COMMAND ----------

final_df = final_df.withColumn("ANO", f.substring('COMPETENCIA_DECLARADA', 1, 4))

# COMMAND ----------


final_df = cf.append_control_columns(final_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
final_df = final_df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))


# COMMAND ----------

final_df.write.partitionBy('ANO').mode('overwrite', ).parquet(path=adl_raw, mode='overwrite')