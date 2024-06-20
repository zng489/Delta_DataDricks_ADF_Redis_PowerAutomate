# Databricks notebook source
# from cni_connectors import adls_gen1_connector as adls_conn
# var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/lnd", "error": "/tmp/dev/err", "archive": "/tmp/dev/ach", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "trusted": "/tmp/dev/trs", "business": "/tmp/dev/biz", "prm": "/tmp/dev/prm", "historico": "/tmp/dev/hst" }, "path_prefix": "", "uld": { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }, "systems": { "raw": "usr" } }

dbutils.widgets.text("user_parameters", '{"null": "null"}')

table = {"schema":"mtp_aeat","table":"motivo_class","prm_path":"/usr/mtp_aeat/org_raw_mtp_aeat_motivo_class.py/FIEC_MTP_AEAT_MOTIVO_CLASS_mapeamento_unificado_trusted.xlsx"}

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from unicodedata import normalize
import pyspark.sql.functions as f
import crawler.functions as cf
import datetime
import json
import re

# Databricks notebook source
# Biblioteca cni_connectors, que dá acesso aos dados no datalake
from cni_connectors import adls_connector as adls_conn
# A biblioteca criada para facilitar a declaração dos testes. É necessário importá-la.
from datatest.gx_context_provider import GXContextProvider
# Essa declaração é necessária em todos os notebooks. Somente através dela podemos acessar os dados no datalake.
var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

table = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']
prm = dls['folders']['prm']

# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])
adl_raw = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

# COMMAND ----------

prm_file = table["prm_path"].split('/')[-1]
prm_path = f'{prm}/usr/{table["schema"]}_{table["table"]}/{prm_file}'

# COMMAND ----------

tipo_carga = 'f'
trs_path = "{trs}/{schema}/{table}_{tipo_carga}".format(trs=trs, schema=table["schema"], table=table["table"], tipo_carga=tipo_carga)
adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

# COMMAND ----------

sheet_name='MTP_AEAT_MOTIVO_CLASS'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply transformations and save dataframe

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)
 

# COMMAND ----------

df = spark.read.parquet(adl_raw)
df_2022 = df.filter(f.col("ANO") == "2022")

# COMMAND ----------

df_2021 = df.filter(f.col("ANO") == "2021")

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, StructType, StructField


@udf(returnType=StringType())
#def spliting(x):
#    if x is not None:
#        return x + 1
def split_and_create_columns(s):
    parts = s.split(":")
    return parts
      
# Register UDF with Spark (optional if you want to use it in SQL)
## spark.udf.register("split_and_create_columns_udf", split_and_create_columns)

upperCaseUDF = udf(lambda z:split_and_create_columns(z),StringType())  

df_2021 = df_2021.withColumn("Cureated Name", split_and_create_columns(col("CLASSE_DO_CNAE_2_0")))

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Assuming df_2021 is correctly initialized somewhere above this line

@udf(returnType=StringType())
def creating_columns_0(value):
    x = value[0]
    return x
    
upperCaseUDF = udf(lambda z:split_and_create_columns(z),StringType())
# If you intend to use the UDF directly without lambda, you can do so as follows:
df_2021_ = df_2021.withColumn("Valor_Primeiro", creating_columns_0(col('Cureated Name')))

# Assuming df_2021 is correctly initialized somewhere above this line

#@udf(returnType=StringType())
#def creating_columns_1(value):
#    x = value[-1]
#    return x

@udf(returnType=StringType())
def creating_columns_1(value):
    return value[-1]

upperCaseUDF = udf(lambda z:split_and_create_columns(z),StringType())
# If you intend to use the UDF directly without lambda, you can do so as follows:
df_2021__ = df_2021_.withColumn("Valor_Segundo", creating_columns_1(col('Cureated Name')))

# COMMAND ----------

df_2021__.display()

