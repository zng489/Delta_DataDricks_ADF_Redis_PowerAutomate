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

if not cf.directory_exists(dbutils, uld_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % uld_path)

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

from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BinaryType, BooleanType, ArrayType, MapType

dataframes = []
for path in cf.list_subdirectory(dbutils, uld_path):
  for file_path in cf.list_subdirectory(dbutils, path):
    for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
      if arquivos_txt.endswith('.txt'):
        print(f'  Explorando subdiretório: {arquivos_txt}')
        numbers_list = re.findall(r'\d+', path)
        numbers = ''.join(numbers_list)

        df = (spark.read
                      .option("delimiter", ";")
                      .option("header", "true")
                      .option("encoding", "ISO-8859-1")
                      .csv(f"{var_adls_uri}/{file_path}")
                      )

        headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}

        print("It's set to '2018' as the default for all years.")
        var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path, headers=headers, sheet_names=['2018'])

        for column in df.columns:
          df = df.withColumnRenamed(column, __normalize_str(column))

        def __select(parse_ba_doc, sheet_name):
          for org, dst, _type in parse_ba_doc[sheet_name]:
            if org == 'N/A' and dst not in df.columns:
              yield f.lit(None).cast(_type).alias(dst)
            else:
              _col = f.col(org)
              if _type.lower() == 'double':
                _col = f.regexp_replace(org, ',', '.')
              yield _col.cast(_type).alias(dst)    
              
        print(numbers)
        df = df.select(*__select(var_prm_dict, '2018'))
        df = df.withColumn("ANO", lit(f"{numbers}"))

        dataframes.append(df)
      else:
        print(f'There is no file txt in {path}') 

# COMMAND ----------

if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

df = cf.append_control_columns(final_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

# COMMAND ----------

df.write.partitionBy('ANO').parquet(path=adl_raw, mode='overwrite')