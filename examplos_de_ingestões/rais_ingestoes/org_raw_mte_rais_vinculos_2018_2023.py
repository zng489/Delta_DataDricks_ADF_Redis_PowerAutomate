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

# dir(cf.list_subdirectory)

# COMMAND ----------

# Esses são os dicionários de configuração da transformação enviados pelo ADF e acessados via widgets. Os diretórios de origem e destino das tabelas são compostos por valores em 'dls' e 'tables'.
# Parametros necessario para na ingestão
### file = notebook_params.var_file
### dls = notebook_params.var_dls
### adf = notebook_params.var_adf



# COMMAND ----------

uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

# COMMAND ----------

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
print(adl_uld)

# COMMAND ----------

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
print(adl_raw)

# COMMAND ----------

# Path sem {var_adls_uri} e cuidado com /tmp/dev
prm_path = "{path_prefix}{prm_path}".format(path_prefix=dls['path_prefix'], prm_path=file['prm_path'])
prm_path

# COMMAND ----------

cf.list_adl_files(spark, dbutils, uld_path)
# nm_arq_in	dh_arq_in
# AC2014ID.txt	2024-10-18T10:32:39.000+00:00
# AL2014ID.txt	2024-10-18T10:32:32.000+00:00
# AM2014ID.txt	2024-10-18T10:33:56.000+00:00


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import re

# Define a common schema based on the columns you expect to have in all files
# This is an example schema, adjust it according to your actual data
common_schema = StructType([
    StructField("Column1", StringType(), True),
    StructField("Column2", StringType(), True),
    # Add more fields as per your data
])

dataframes = []
for path in cf.list_subdirectory(dbutils, uld_path):
# for file_path in cf.list_subdirectory(dbutils, '/tmp/dev/uld/oni/mte/rais/rais_vinculo_2018_2023/RAIS_2014'):
    # dataframes = []
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

# Union all DataFrames in the list
combined_df = dataframes[0] if dataframes else spark.createDataFrame([],  schema=None)
for df in dataframes[1:]:
    combined_df = combined_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

combined_df = combined_df.withColumnRenamed(combined_df.columns[39], "CNAE 2_0 Classe")
combined_df = combined_df.withColumnRenamed(combined_df.columns[40], "CNAE 2_0 Subclasse")


# COMMAND ----------

# O prm nao esta funcionando
headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, '/tmp/dev/prm/usr/oni/mte/rais_vinculos_2018_2023/prm_rais_vinculo_2018_2023.xlsx', headers=headers, sheet_names='RAW')

# O prm nao esta funcionando
def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)

# O prm nao esta funcionando
df = combined_df.select(*__select(var_prm_dict, 'RAW'))

# COMMAND ----------

# #inserção de coluna de ingestão na camada raw
df = cf.append_control_columns(combined_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = combined_df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

# COMMAND ----------

df.write.partitionBy('ANO').parquet(path=adl_raw, mode='overwrite')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

combined_df.columns[41]

# COMMAND ----------

combined_df = combined_df.withColumnRenamed(combined_df.columns[39], "CNAE 2_0 Classe")
combined_df = combined_df.withColumnRenamed(combined_df.columns[40], "CNAE 2_0 Subclasse")


combined_df = combined_df.withColumnRenamed(combined_df.columns[79], "79")
combined_df = combined_df.withColumnRenamed(combined_df.columns[80], "80")
combined_df = combined_df.withColumnRenamed(combined_df.columns[81], "81")
combined_df = combined_df.withColumnRenamed(combined_df.columns[82], "82")

# COMMAND ----------

# combined_df.columns

# COMMAND ----------

# List of columns to drop
columns_to_drop = ['79',
 '80',
 '81',
 '82']

# Drop the columns
combined_df = combined_df.drop(*columns_to_drop)

# COMMAND ----------

# O prm nao esta funcionando
headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, '/tmp/dev/prm/usr/oni/mte/rais_vinculos_2018_2023/prm_rais_vinculo_2018_2023.xlsx', headers=headers, sheet_names='RAW')

# O prm nao esta funcionando
def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)

# O prm nao esta funcionando
df = combined_df.select(*__select(var_prm_dict, 'RAW'))

# COMMAND ----------

data_fields = [
    "MUNICIPIO",
    "CNAE_95_CLASSE",
    "VINCULO_ATIVO_31/12",
    "TIPO_VINCULO",
    "MOTIVO_DESLIGAMENTO",
    "MES_DESLIGAMENTO",
    "IND_VINCULO_ALVARA",
    "TIPO_ADMISSAO",
    "TIPO_SALARIO",
    "CBO_94_OCUPACAO",
    "ESCOLARIDADE_APOS_2005",
    "SEXO_TRABALHADOR",
    "NACIONALIDADE",
    "RACA_COR",
    "IND_PORTADOR_DEFIC",
    "TAMANHO_ESTABELECIMENTO",
    "NATUREZA_JURIDICA",
    "IND_CEI_VINCULADO",
    "TIPO_ESTAB",
    "IND_ESTAB_PARTICIPA_PAT",
    "IND_SIMPLES",
    "DATA_ADMISSAO_DECLARADA",
    "VL_REMUN_MEDIA_NOM",
    "VL_REMUN_MEDIA_(SM)",
    "VL_REMUN_DEZEMBRO_NOM",
    "VL_REMUN_DEZEMBRO_(SM)",
    "TEMPO_EMPREGO",
    "QTD_HORA_CONTR",
    "VL_ULTIMA_REMUNERACAO_ANO",
    "VL_SALARIO_CONTRATUAL",
    "PIS",
    "DATA_DE_NASCIMENTO",
    "NUMERO_CTPS",
    "CPF",
    "CEI_VINCULADO",
    "CNPJ_/_CEI",
    "CNPJ_RAIZ",
    "NOME_TRABALHADOR",
    "CBO_OCUPACAO_2002",
    "CNAE_2_0_CLASSE",
    "CNAE_2_0_SUBCLASSE",
    "_TIPO_DEFIC",
    "CAUSA_AFASTAMENTO_1",
    "DIA_INI_AF1",
    "MES_INI_AF1",
    "DIA_FIM_AF1",
    "MES_FIM_AF1",
    "CAUSA_AFASTAMENTO_2",
    "DIA_INI_AF2",
    "MES_INI_AF2",
    "DIA_FIM_AF2",
    "MES_FIM_AF2",
    "CAUSA_AFASTAMENTO_3",
    "DIA_INI_AF3",
    "MES_INI_AF3",
    "DIA_FIM_AF3",
    "MES_FIM_AF3",
    "QTD_DIAS_AFASTAMENTO",
    "IDADE",
    "DIA_DE_DESLIGAMENTO",
    "IBGE_SUBSETOR",
    "ANO_CHEGADA_BRASIL",
    "CEP_ESTAB",
    "MUN_TRAB",
    "RAZAO_SOCIAL",
    "VL_REM_JANEIRO_CC",
    "VL_REM_FEVEREIRO_CC",
    "VL_REM_MARCO_CC",
    "VL_REM_ABRIL_CC",
    "VL_REM_MAIO_CC",
    "VL_REM_JUNHO_CC",
    "VL_REM_JULHO_CC",
    "VL_REM_AGOSTO_CC",
    "VL_REM_SETEMBRO_CC",
    "VL_REM_OUTUBRO_CC",
    "VL_REM_NOVEMBRO_CC",
    "IND_TRAB_INTERMITENTE",
    "IND_TRAB_PARCIAL",
    "ANO",
    "VL_REM_JANEIRO_SC",
    "VL_REM_FEVEREIRO_SC",
    "VL_REM_MARCO_SC",
    "VL_REM_ABRIL_SC",
    "VL_REM_MAIO_SC",
    "VL_REM_JUNHO_SC",
    "VL_REM_JULHO_SC",
    "VL_REM_AGOSTO_SC",
    "VL_REM_SETEMBRO_SC",
    "VL_REM_OUTUBRO_SC",
    "VL_REM_NOVEMBRO_SC"
]


# Rename columns based on position
for index, new_name in enumerate(data_fields):
    combined_df = combined_df.withColumnRenamed(combined_df.columns[index], new_name)

# COMMAND ----------

# O prm nao esta funcionando
# headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
# var_prm_dict = cf.parse_ba_doc_spark(dbutils, '/tmp/dev/prm/usr/oni/mte/rais_vinculos_2018_2023/prm_rais_vinculo_2018_2023.xlsx', headers=headers, sheet_names='TRS')

# O prm nao esta funcionando
# def __select(parse_ba_doc, year):
#  for org, dst, _type in parse_ba_doc[year]:
#    if org == 'N/A' and dst not in df.columns:
#      yield f.lit(None).cast(_type).alias(dst)
#    else:
#      _col = f.col(org)
#      if _type.lower() == 'double':
#        _col = f.regexp_replace(org, ',', '.')
#      yield _col.cast(_type).alias(dst)

# O prm nao esta funcionando
# df = combined_df.select(*__select(var_prm_dict, 'RAW'))

# COMMAND ----------

# #inserção de coluna de ingestão na camada raw
df = cf.append_control_columns(combined_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = combined_df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

def clean_backtick_all_columns(df):
    """
    Cleans all columns in the DataFrame by removing backticks.
    
    :param df: Input DataFrame
    :return: Cleaned DataFrame
    """
    for column in df.columns:
        df = df.withColumn(column, regexp_replace(col(column), "`", ""))
    return df

# Clean all columns by removing backticks
cleaned_df = clean_backtick_all_columns(combined_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# Define the UDF to clean backticks
def clean_backticks(value):
    if value is not None:
        return value.replace("`", "")
    return value

clean_backticks_udf = udf(clean_backticks, StringType())

def clean_backtick_all_columns_udf(df):
    """
    Cleans all string columns in the DataFrame by removing backticks using a UDF.
    
    :param df: Input DataFrame
    :return: Cleaned DataFrame
    """
    for column in df.columns:
        df = df.withColumn(column, clean_backticks_udf(column))
    return df


# Clean all columns by removing backticks using the UDF
cleaned_df = clean_backtick_all_columns_udf(combined_df)

# COMMAND ----------

"""
columns_to_select = [
    "Município",
    "CNAE 95 Classe",
    "Vínculo Ativo 31/12",
    "Tipo Vínculo",
]

# Rename columns based on position
for index, new_name in enumerate(columns_to_select):
    combined_df = combined_df.withColumnRenamed(df.columns[index], new_name)
"""

# COMMAND ----------

"""
from pyspark.sql import functions as F

# Assuming combined_df is your initial DataFrame
df = combined_df

# Define a regex pattern for the characters you want to remove
pattern = '[^a-zA-Z0-9\s]'  # This will keep only alphanumeric characters and spaces

# Apply the cleaning function to all string columns in the DataFrame
for column_name in df.columns:
    # Only apply to string columns
    if dict(df.dtypes)[column_name] == 'string':
        # Use backticks for column names to handle special characters/spaces
        df = df.withColumn(column_name, F.regexp_replace(F.col(f"`{column_name}`"), pattern, ''))
"""

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

# __normalize_str(_str) em todas as columns
for column in df.columns:
  df = combined_df.withColumnRenamed(column, __normalize_str(column))

# COMMAND ----------

# O prm nao esta funcionando
headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, '/tmp/dev/prm/usr/oni/mte/rais_vinculos_2018_2023/prm_rais_vinculo_2018_2023.xlsx', headers=headers, sheet_names='RAW')

# COMMAND ----------

# O prm nao esta funcionando
def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)

# COMMAND ----------

# O prm nao esta funcionando
df = combined_df.select(*__select(var_prm_dict, 'RAW'))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Last Year PRM Example") \
    .getOrCreate()

# Sample data
data = [
    (2020, 100),
    (2021, 200),
    (2022, 300),
    (2023, 400)
]

# Create DataFrame
df = spark.createDataFrame(data, ["year", "value"])
df.display()


# COMMAND ----------

filtered_years = df.filter(col("year") < target_value).select("year").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

def replace_invalid_values(df):
    for column in df.columns:
        df = df.withColumn(column, f.when(f.trim(f.col(column)) == "{ñ class}", None).otherwise(f.col(column)))

    return df

# COMMAND ----------

# Example

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Step 1: Initialize a Spark Session
spark = SparkSession.builder \
    .appName("Replace Invalid Values Example") \
    .getOrCreate()

# Step 2: Create a Sample DataFrame
data = [
    (1, "John", "{ñ class}"),
    (2, "Jane", "Doe"),
    (3, "Sam", "{ñ class}"),
    (4, "Alice", "Smith"),
    (5, "Bob", "{ñ class}"),
]

columns = ["id", "first_name", "last_name"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()


# COMMAND ----------


# Step 3: Define the Function to Replace Invalid Values
def replace_invalid_values(df):
    for column in df.columns:
        df = df.withColumn(column, f.when(f.trim(f.col(column)) == "{ñ class}", None).otherwise(f.col(column)))
    return df

# Use the function
df_cleaned = replace_invalid_values(df)

print("Cleaned DataFrame:")
df_cleaned.show()

# Stop the Spark Session
spark.stop()
