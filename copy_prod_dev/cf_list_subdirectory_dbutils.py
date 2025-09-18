dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from trs_control_field import trs_control_field as tcf
import pyspark.sql.functions as f
import crawler.functions as cf#
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

df = spark.read.format('parquet').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/bases_referencia/cnae/cnae_21')

uld_path = "trs/oni/midr/"
for path in cf.list_subdirectory(dbutils, uld_path):
  print(path)

from pyspark.sql import SparkSession
import os
import datetime

#spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo
file_path = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/lnd/crw/un_comtrade__comer_wld/commodity_annual/2024/part-00215-tid-7617440628921716723-88d2989a-2012-45b4-8312-3dc0ed3a7854-2379-1-c000.snappy.parquet"

# Lê o arquivo parquet
df = spark.read.parquet(file_path)

# Mostra o schema
print("Schema:")
df.printSchema()

# Contagem de registros
row_count = df.count()
print(f"Número de linhas: {row_count}")

# Tipos de dados
print("Tipos de dados:")
print(df.dtypes)

# Número de colunas
print(f"Número de colunas: {len(df.columns)}")

# Lista de colunas
print("Colunas:")
print(df.columns)

# Informações do arquivo (usando dbutils se estiver no Databricks)
try:
    file_info = dbutils.fs.ls(file_path)[0]
    print(f"Tamanho do arquivo: {file_info.size / 1024:.2f} KB")
    print(f"Data de modificação: {datetime.datetime.fromtimestamp(file_info.modificationTime / 1000)}")
except:
    print("Não foi possível obter informações de modificação do arquivo.")

uld_path = "/trs/oni/mte/novo_caged/identificada/CD_ANO=2025"
for path in cf.list_subdirectory(dbutils, uld_path):
  print(path)
  for file_path in cf.list_subdirectory(dbutils, path):
    print(file_path)
    for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
      #print(arquivos_txt)
      pass