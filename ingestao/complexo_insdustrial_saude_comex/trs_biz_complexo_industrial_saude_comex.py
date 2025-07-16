# Databricks notebook source
# Instalalção do pacote do banco mundial
%pip install wbgapi

# COMMAND ----------

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

tables = {"schema":"", "table":"", 
          "trusted_path_1":"/oni/ceis/produtos_ceis/",
          "trusted_path_2":"/me/comex/ncm_exp_p/",  # /oni TIRAR ONI, PQ NAO TEM NA DEV E PRODUCAO!!!!!!!!!!!!!!!!!!
          "trusted_path_3":"/me/comex/ncm_imp_p/",  # /oni TIRAR ONI, PQ NAO TEM NA DEV E PRODUCAO!!!!!!!!!!!!!!!!!!
          "destination":"/oni/complexo_industrial_saude/comex/", 
          "databricks":{"notebook":"/biz/oni/comex/trs_biz_complexo_industrial_saude"}, 
          "prm_path":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

# COMMAND ----------

# Configurações iniciais
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import (
    col, substring, lpad, when, lit, sum, trim, concat, 
    regexp_replace, round, format_number
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType
from trs_control_field import trs_control_field as tcf
from functools import reduce
import pandas as pd
import re
import os

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

trusted = dls['folders']['trusted']
business = dls['folders']['business']
sink = dls['folders']['business']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'
print(adl_trusted_1)

trusted_path_2 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_2'])
adl_trusted_2 = f'{var_adls_uri}{trusted_path_2}'
print(adl_trusted_2)

trusted_path_3 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_3'])
adl_trusted_3 = f'{var_adls_uri}{trusted_path_3}'
print(adl_trusted_3)

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination'])
adl_destination_path = f'{var_adls_uri}{destination_path}'
print(adl_destination_path)

# COMMAND ----------

# Define livrarias
import pyspark.sql.functions as f
import pandas as pd
import numpy as np
import wbgapi as wb

# COMMAND ----------

# Carrega dados do data lake
#exp_bruto = spark.read.table("datalake__trs.oni.me_comex__ncm_exp_p")
#imp_bruto = spark.read.table("datalake__trs.oni.me_comex__ncm_imp_p")

# COMMAND ----------

# Carrega dados do data lake
ncm_bruto = spark.read.format("delta").load(adl_trusted_1)
exp_bruto = spark.read.parquet(adl_trusted_2)
imp_bruto = spark.read.parquet(adl_trusted_3)


# Seleção da lista de NCMs
lista_ncm = [row['CD_NCM'] for row in ncm_bruto.select('CD_NCM').collect()]

# COMMAND ----------

# Carrega dados da API
ppp_api =  wb.data.DataFrame('PA.NUS.PPPC.RF', 'BRA')

# Limpa dados da API
ppp_pandas = ppp_api.reset_index().melt(id_vars = ['economy'])

ppp_pandas['CD_ANO'] = ppp_pandas['variable'].str.extract('(\d+)')
ppp_pandas['CD_ANO'] = pd.to_numeric(ppp_pandas['CD_ANO'])

ppp_pandas = ppp_pandas[['CD_ANO', 'value']]

ppp = spark.createDataFrame(ppp_pandas)

ppp = ppp.where(f.col('CD_ANO') >= 1997) \
  .withColumnRenamed('value', 'indicador_ppp')

# COMMAND ----------

# Define função para resumir comex
def resume_comex(df, df_ppp, tipo = 'importacao'):
  if tipo == 'importacao':
    nome = 'imp_fob'
    nome_vl_ppp = 'imp_fob_ppp'
  elif tipo == 'exportacao':
    nome = 'exp_fob'
    nome_vl_ppp = 'exp_fob_ppp'

  df_resultado = df \
    .where(f.col('CD_NCM').isin(lista_ncm)) \
    .groupby('CD_ANO', 'CD_NCM', 'CD_PAIS', 'SG_UF_NCM') \
    .agg(f.sum(f.col('VL_FOB')).alias(nome)) \
    .join(df_ppp, on = 'CD_ANO', how = 'left') \
    .withColumn(nome_vl_ppp, f.col(nome) / f.col('indicador_ppp'))

  return(df_resultado)

# COMMAND ----------

# Aplica função aos dados de importação e exportação
imp = resume_comex(imp_bruto, ppp, 'importacao')

# COMMAND ----------

exp = resume_comex(exp_bruto, ppp, 'exportacao')

# COMMAND ----------

# Une dados de importacao e exportacao
comercio = imp \
  .join(exp, on = ['CD_ANO', 'CD_NCM', 'indicador_ppp', 'CD_PAIS', 'SG_UF_NCM'], how = 'full') \
  .fillna(0, ['imp_fob', 'imp_fob_ppp', 'exp_fob', 'exp_fob_ppp']) \
  .withColumn('saldo', f.col('exp_fob') - f.col('imp_fob')) \
  .withColumn('saldo_ppp', f.col('exp_fob_ppp') - f.col('imp_fob_ppp')) \
  .select('CD_ANO', 'CD_NCM', 'SG_UF_NCM', 'CD_PAIS', 'indicador_ppp', 'imp_fob', 'imp_fob_ppp', 'exp_fob', 'exp_fob_ppp', 'saldo', 'saldo_ppp')

# COMMAND ----------

print(adl_destination_path)

# COMMAND ----------

df = comercio.drop('dh_insercao_trs')
final_df = tcf.add_control_fields(df, adf, layer="biz")
final_df.write.mode('overwrite').parquet(adl_destination_path, compression='snappy')

# COMMAND ----------

# COMMAND ----------

