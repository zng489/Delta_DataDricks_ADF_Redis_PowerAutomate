# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type truncate/full insert
# MAGIC <pre>
# MAGIC
# MAGIC Processo	org_raw_rais
# MAGIC Tabela/Arquivo Origem	"Camada RAW de USR, assunto MTE, fonte Rais Vinculo"
# MAGIC Tabela/Arquivo Destino	"Camada TRS, assunto MTE, fonte Rais Vinculo"
# MAGIC Particionamento Tabela/Arquivo Destino	Ano selecionado no arquivo de origem (ANO) e as duas primeiras posições do campo Municipio (UF)
# MAGIC Descrição Tabela/Arquivo Destino	Dados da Rais Vínculo dos ano de 2008 a 2018
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atuaização	N/A
# MAGIC Periodicidade/Horario Execução	Anual depois da disponibilização dos dados na landing zone
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

tables = {"path_origin": "/usr/me/rais_vinculo", "rais_ident_new":"/usr/oni/mte/rais/identificada/rais_vinculo/", "path_destination":"/oni/mte/rais/identificada/rais_vinculo/", "prm_path": "/tmp/dev/prm/usr/oni/mte/rais_identificada_vinculos/prm_rais_identificada_vinculo.xlsx",}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}
# COMMAND ----------

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
import os
from core.string_utils import normalize_replace

# COMMAND ----------


# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import datetime
from trs_control_field import trs_control_field as tcf

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

dbutils.widgets.text("tables", "")
dbutils.widgets.text("dls", "")
dbutils.widgets.text("adf", "")

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

rais_ident_new = "{adl_path}{raw}{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["rais_ident_new"])
rais_ident_new

# COMMAND ----------

df_source = spark.read.parquet(source).drop("nr_reg", "nm_arq_in", "dh_arq_in", "kv_process_control", "dh_insercao_raw")
df_source.display()

# COMMAND ----------

df_source.columns

# COMMAND ----------

df_rais_ident_new = spark.read.parquet(rais_ident_new)

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, tables["prm_path"], headers=headers, sheet_names='TRS')

def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df_rais_ident_new.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org) if org in df_rais_ident_new.columns else f.lit(None)
      if _type.lower() == 'double':
        _col = _col.cast("double")  # Assuming org is a column name, cast directly
      yield _col.alias(dst)

# Assuming var_prm_dict is defined correctly as before
df_rais_ident_new = df_rais_ident_new.select(*__select(var_prm_dict, 'TRS')).drop("nr_reg", "nm_arq_in", "dh_arq_in", "kv_process_control", "dh_insercao_raw")

df_rais_ident_new.display()

# COMMAND ----------

# subclasse_cols = ['CD_CNAE20_CLASSE']
# for column in subclasse_cols:
#  df_rais_ident_new = df_rais_ident_new.withColumn(column, regexp_replace(column, '[^1234567890.-]', ''))

# COMMAND ----------

df_rais_ident_new.columns

# COMMAND ----------

# df_rais_ident_new = df_rais_ident_new.withColumnRenamed("CD_CBO_94_OCUPACAO", "CD_CBO94")

# COMMAND ----------

#################
### DF_SOURCE ###
#################

# COMMAND ----------

subclasse_cols = ['CD_CNAE20_CLASSE']
for column in subclasse_cols:
  df_source = df_source.withColumn(column, regexp_replace(column, '[^1234567890.-]', ''))

# COMMAND ----------

df_2008_to_2010 = df_source.filter((col("ANO") >= 2008) & (col("ANO") <= 2010))

# COMMAND ----------

df_2011_onwards = df_source.filter(col("ANO") >= 2011)

# COMMAND ----------

transformation_2008_to_2010 = {"CD_CBO94": when(lower(col("CD_CBO94")) == "ignorado", lit(-1)).otherwise(regexp_replace('CD_CBO94', 'CBO ', '')),
                               "CD_SEXO": when(col('CD_SEXO') == 'MASCULINO', lit(1)).when(col('CD_SEXO') == 'FEMININO', lit(2)).otherwise(col('CD_SEXO')),
                               "CD_CBO": when(lower(col("CD_CBO")) == "ignorado", lit("-1")).otherwise(regexp_replace('CD_CBO', 'CBO ', '')),
                               "CD_CAUSA_AFASTAMENTO1": when(col('CD_CAUSA_AFASTAMENTO1') == '-1', lit('99')).otherwise(col('CD_CAUSA_AFASTAMENTO1')),
                               "CD_CAUSA_AFASTAMENTO2": when(col('CD_CAUSA_AFASTAMENTO2') == '-1', lit('99')).otherwise(col('CD_CAUSA_AFASTAMENTO2')),
                               "CD_CAUSA_AFASTAMENTO3": when(col('CD_CAUSA_AFASTAMENTO3') == '-1', lit('99')).otherwise(col('CD_CAUSA_AFASTAMENTO3')),
                               "NR_DIA_INI_AF1": when(col('NR_DIA_INI_AF1') == '-1', lit('99')).otherwise(col('NR_DIA_INI_AF1')),
                               "NR_DIA_INI_AF2": when(col('NR_DIA_INI_AF2') == '-1', lit('99')).otherwise(col('NR_DIA_INI_AF2')),
                               "NR_DIA_INI_AF3": when(col('NR_DIA_INI_AF3') == '-1', lit('99')).otherwise(col('NR_DIA_INI_AF3')),
                               "NR_MES_INI_AF1": when(lower(col('NR_MES_INI_AF1')).isin('-1','ignorado'), lit('99')).otherwise(col('NR_MES_INI_AF1')),
                               "NR_MES_INI_AF2": when(lower(col('NR_MES_INI_AF2')).isin('-1','ignorado'), lit('99')).otherwise(col('NR_MES_INI_AF2')),
                               "NR_MES_INI_AF3": when(lower(col('NR_MES_INI_AF3')).isin('-1','ignorado'), lit('99')).otherwise(col('NR_MES_INI_AF3')),
                               "NR_DIA_FIM_AF1": when(col('NR_DIA_FIM_AF1') == -1, lit(99)).otherwise(col('NR_DIA_FIM_AF1')),
                               "NR_DIA_FIM_AF2": when(col('NR_DIA_FIM_AF2') == -1, lit(99)).otherwise(col('NR_DIA_FIM_AF2')),
                               "NR_DIA_FIM_AF3": when(col('NR_DIA_FIM_AF3') == -1, lit(99)).otherwise(col('NR_DIA_FIM_AF3')),
                               "NR_MES_FIM_AF1": when(lower(col('NR_MES_FIM_AF1')).isin('-1','ignorado'), lit('99')).otherwise(col('NR_MES_FIM_AF1')),
                               "NR_MES_FIM_AF2": when(lower(col('NR_MES_FIM_AF2')).isin('-1','ignorado'), lit('99')).otherwise(col('NR_MES_FIM_AF2')),
                               "NR_MES_FIM_AF3": when(lower(col('NR_MES_FIM_AF3')).isin('-1','ignorado'), lit('99')).otherwise(col('NR_MES_FIM_AF3')),
                               "VL_IDADE": (months_between(lit(datetime.date(2009, 12, 31)).cast('date'), to_date(lpad('DT_DIA_MES_ANO_DATA_NASCIMENTO',8,'0'), 'ddMMyyyy')) / lit(12)).cast('int')
                              }

for key in transformation_2008_to_2010:
  df_2008_to_2010 = df_2008_to_2010.withColumn(key, transformation_2008_to_2010[key])  

# COMMAND ----------

columns_month = ["NR_MES_INI_AF1",
                 "NR_MES_INI_AF2",
                 "NR_MES_INI_AF3",
                 "NR_MES_FIM_AF1",
                 "NR_MES_FIM_AF2",
                 "NR_MES_FIM_AF3"]

for item in columns_month:
  df_2008_to_2010 = df_2008_to_2010\
  .withColumn(item, when(col("ANO") <= 2010,
                        when(lower(col(item)) == "janeiro", lit('01'))\
                       .when(lower(col(item)) == "fevereiro", lit('02'))\
                       .when(lower(col(item)).isin("marco", "março"), lit('03'))\
                       .when(lower(col(item)) == "abril", lit('04'))\
                       .when(lower(col(item)) == "maio", lit('05'))\
                       .when(lower(col(item)) == "junho", lit('06'))\
                       .when(lower(col(item)) == "julho", lit('07'))\
                       .when(lower(col(item)) == "agosto", lit('08'))\
                       .when(lower(col(item)) == "setembro", lit('09'))\
                       .when(lower(col(item)) == "outubro", lit('10'))\
                       .when(lower(col(item)) == "novembro", lit('11'))\
                       .when(lower(col(item)) == "dezembro", lit('12'))\
                       .otherwise(col(item)))\
                   .otherwise(col(item)))

# COMMAND ----------

df = df_2008_to_2010.union(df_2011_onwards.select(df_2008_to_2010.columns))

# COMMAND ----------

#extended to anothers 'AF' columns to parse all to int
columns_month.extend(['NR_DIA_INI_AF1','NR_DIA_INI_AF2','NR_DIA_INI_AF3'])

for i in columns_month:
  df = df.withColumn(i,f.col(i).cast('int'))

# COMMAND ----------

transformation_all = {"CD_CBO94": when(lower(col("CD_CBO94")) == "{ñ cl", lit(None)).otherwise(col("CD_CBO94")),
                      "CD_CBO": when(lower(col("CD_CBO")) == "0000-1", lit("-1")).otherwise(col("CD_CBO")),
                      "CD_CNAE20_DIVISAO": substring("CD_CNAE20_CLASSE", 0, 2), 
                      "CD_UF": substring("CD_MUNICIPIO", 0, 2),
                      "CD_CBO4": substring("CD_CBO", 0, 4)
                     }

for key in transformation_all:
  df = df.withColumn(key, transformation_all[key])

# COMMAND ----------

df = (df
.withColumn('ID_CPF',f.lpad(f.col('ID_CPF'),11,'0'))
.withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0'))
)

# COMMAND ----------

df = df.drop("nr_reg", "nm_arq_in", "dh_arq_in", "kv_process_control", "dh_insercao_raw")

# COMMAND ----------

df = df.filter((df.ANO >= 2008) & (df.ANO <= 2017))

# COMMAND ----------

##########################
### df_rais_ident_new ####
##########################

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

df_source = broadcast(df_source)

# Perform the union
result_df = df.unionByName(df_source, allowMissingColumns=True)


# COMMAND ----------

#add control fields from trusted_control_field egg
result_df = tcf.add_control_fields(result_df, adf)

# COMMAND ----------

result_df.write.partitionBy('ANO', 'CD_UF').save(path=target, format="parquet", mode='overwrite')

# COMMAND ----------

target

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.partitionBy('ANO', 'CD_UF').save(path=target, format="parquet", mode='overwrite')

# COMMAND ----------

MASK_FIELDS = [
    "FL_IND_VINCULO_ALVARA",
    "CD_TIPO_SALARIO",
    "CD_CBO94",
    "FL_IND_ESTAB_PARTICIPA_PAT",
    "DT_DIA_MES_ANO_DATA_ADMISSAO",
    "VL_REMUN_ULTIMA_ANO_NOM",
    "VL_REMUN_CONTRATUAL_NOM",
    "ID_PIS",
    "DT_DIA_MES_ANO_DATA_NASCIMENTO",
    "ID_CTPS",
    "ID_CPF",
    "ID_CEI_VINCULADO",
    "ID_CNPJ_CEI",
    "ID_CNPJ_RAIZ",
    "ID_NOME_TRABALHADOR",
    "DT_DIA_MES_ANO_DIA_DESLIGAMENTO",
    "NR_DIA_INI_AF1",
    "NR_MES_INI_AF1",
    "NR_DIA_FIM_AF1",
    "NR_MES_FIM_AF1",
    "NR_DIA_INI_AF2",
    "NR_MES_INI_AF2",
    "NR_DIA_FIM_AF2",
    "NR_MES_FIM_AF2",
    "NR_DIA_INI_AF3",
    "NR_MES_INI_AF3",
    "NR_DIA_FIM_AF3",
    "NR_MES_FIM_AF3",
    "ID_CEPAO_ESTAB",
    "ID_RAZAO_SOCIAL",
    "FL_SINDICAL",
    "VL_ANO_CHEGADA_BRASIL2"
 ]
  

# COMMAND ----------

df = df.drop(*MASK_FIELDS)

# COMMAND ----------

df.write.partitionBy('ANO', 'CD_UF').save(path=target+'_publica', format="parquet", mode='overwrite')

# COMMAND ----------

###########################################################
###########################################################

#AnalysisException: [NUM_COLUMNS_MISMATCH] UNION can only be performed on inputs with the same number of columns, but the first input has 90 columns and the second input has 96 columns.;

# Select the common columns from both DataFrames
common_columns = list(set(df.columns).intersection(set(df_source.columns)))

# Select only the common columns from both DataFrames
df = df.select(common_columns)
df_source = df_source.select(common_columns)

# Perform the union
result_df = df.union(broadcast(df_source))

# Show result
display(result_df)

###########################################################
###########################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

df_source = broadcast(df_source)

# Perform the union
result_df = df.unionByName(df_source, allowMissingColumns=True)

# Show result
result_df.display()

###########################################################
###########################################################
