# Databricks notebook source
tables = {'schema':'/oni', 'type_raw':'/crw', 'table':'',
          'trusted_path_1':'/observatorio_nacional/monitor_investimentos/painel_monitor/dim_cnae/',
          'trusted_path_2':'/observatorio_nacional/monitor_investimentos/painel_monitor/empresas_auxiliar/',
          'trusted_path_3':'/observatorio_nacional/monitor_investimentos/painel_monitor/estrutura_setorial/',
          'trusted_path_4':'/observatorio_nacional/monitor_investimentos/painel_monitor/mongodb_monitor__investimento/',
          'trusted_path_5':'/bacen/cotacao/dolar_americano/',
          'trusted_path_6':'/bacen/cotacao/euro/',
          'trusted_path_7':'/ibge/scn/pib_anual/municipios/',
          'business_path_1':'/bases_referencia/municipios/',
          'business_path_2':'/base_unica_cnpjs/cnpjs_rfb_rais/',
          'destination':'/oni/observatorio_nacional/monitor_investimentos/painel_monitor/', 'databricks':{'notebook':'/biz/oni/painel_monitor/trs_biz_e_biz_biz_monitor_de_investimentos'}, 'prm_path':''}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

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
import os
from core.string_utils import normalize_replace

# COMMAND ----------

# Esses são os dicionários de configuração da transformação enviados pelo ADF e acessados via widgets. Os diretórios de origem e destino das tabelas são compostos por valores em 'dls' e 'tables'.
# Parametros necessario para na ingestão
### var_file = notebook_params.var_file
### var_dls = notebook_params.var_dls
### var_adf = notebook_params.var_adf

#tables = notebook_params.var_file
#dls = notebook_params.var_dls
#adf = notebook_params.var_adf

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])
print(prm_path)

# os,path_join nao le 'dls'
# lnd_path = os.path.join(dls['folders']['landing'], tables['type_raw'], tables['lnd_path'])
# print(lnd_path)

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
print(trusted_path_1)
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'
print(adl_trusted_1)



trusted_path_2 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_2'])
print(trusted_path_2)
adl_trusted_2 = f'{var_adls_uri}{trusted_path_2}'
print(adl_trusted_2)



trusted_path_3 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_3'])
print(trusted_path_3)
adl_trusted_3 = f'{var_adls_uri}{trusted_path_3}'
print(adl_trusted_3)




trusted_path_4 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_4'])
print(trusted_path_4)
adl_trusted_4 = f'{var_adls_uri}{trusted_path_4}'
print(adl_trusted_4)




trusted_path_5 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_5'])
print(trusted_path_5)
adl_trusted_5 = f'{var_adls_uri}{trusted_path_5}'
print(adl_trusted_5)




trusted_path_6 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_6'])
print(trusted_path_6)
adl_trusted_6 = f'{var_adls_uri}{trusted_path_6}'
print(adl_trusted_6)


trusted_path_7 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_7'])
print(trusted_path_7)
adl_trusted_7 = f'{var_adls_uri}{trusted_path_7}'
print(adl_trusted_7)


# COMMAND ----------


# os.path_join nao le 'dls'
# raw_path = os.path.join(dls['folders']['raw'], tables['type_raw'], tables['schema'], tables['table'])
# print(raw_path)


business_path_1 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_1'])
print(business_path_1)

adl_business_1 = f'{var_adls_uri}{business_path_1}'
print(adl_business_1)


business_path_2 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_2'])
print(business_path_2)

adl_business_2  = f'{var_adls_uri}{business_path_2}'
print(adl_business_2)



# COMMAND ----------

# Era para funcionar
'''
sparkframes = {}

# List files in the directory
file_paths = cf.list_subdirectory(dbutils, trusted_path)

for file_path in file_paths:
    name = file_path.split('/')[-1]
    full_path = f'{var_adls_uri}{adl_trusted}{name}'
    
    try:
        # Load Parquet file into a DataFrame
        df = spark.read.parquet(full_path)
        sparkframes[name] = df
        print(f'Successfully loaded {name}.')
        
        # Optionally, show the first few rows to confirm
        df.show(5)
        
    except Exception as e:
        print(f'Error loading {name} from {full_path}: {e}')
'''


# COMMAND ----------

df_monitor_completo.display()

# COMMAND ----------

# trs
tabela_cnae_dim = spark.read.parquet(f'{adl_trusted_1}').withColumn("cd_cnae_divisao", f.lpad(f.col("cd_cnae_divisao").cast("string"), 2, "0")) \
 .withColumn("cd_cnae_grupo", f.lpad(f.col("cd_cnae_grupo").cast("string"), 3, "0")) \
 .withColumn("cd_cnae_classe", f.lpad(f.col("cd_cnae_classe").cast("string"), 5, "0")) \
 .withColumn("cd_cnae_subclasse", f.lpad(f.col("cd_cnae_subclasse").cast("string"), 7, "0"))

tabela_auxiliar_empresas = spark.read.parquet(f'{adl_trusted_2}').withColumn("cnpj", f.lpad(f.col("cnpj").cast("string"), 8, "0"))

estrutura_setorial = spark.read.parquet(f'{adl_trusted_3}')

df_monitor_completo = spark.read.parquet(f'{adl_trusted_4}')

df_dolar = spark.read.parquet(f'{adl_trusted_5}')

df_euro = spark.read.parquet(f'{adl_trusted_6}')

df_pib_munic = spark.read.parquet(f'{adl_trusted_7}')


# biz
df_municipio = spark.read.parquet(f'{adl_business_1}')

df_base_unica = spark.read.parquet(f'{adl_business_2}')

# COMMAND ----------

df_monitor_completo2 = df_monitor_completo.withColumn("dt_anuncio", f.to_date(f.col("dt_anuncio"), "yyyy-MM-dd"))

# COMMAND ----------

df_monitor = df_monitor_completo2.filter(f.col("dt_anuncio") > f.lit("2023-05-31"))

# COMMAND ----------

df_monitor = df_monitor.withColumn("setor_investimento", f.expr("transform(setor_investimento, x -> regexp_replace(x, 'Fabricação de produtos de metal, exceto máquinas\\ne equipamentos', 'Fabricação de produtos de metal, exceto máquinas e equipamentos'))")
)

# COMMAND ----------

df_dolar = df_dolar.filter(f.year(f.col("DT_COTACAO")) >= 2020)

df_dolar = df_dolar.withColumn("DT_COTACAO_DOLAR", f.date_format(f.col("DT_COTACAO"), "yyyy-MM"))

df_dolar_avg = df_dolar.groupBy("DT_COTACAO_DOLAR").agg(f.avg("VL_COTACAO_COMPRA").alias("VL_COTACAO_MES_DOLAR"))

# COMMAND ----------

df_euro = df_euro.filter(f.year(f.col("DT_COTACAO")) >= 2020)

df_euro = df_euro.withColumn("DT_COTACAO_EURO", f.date_format(f.col("DT_COTACAO"), "yyyy-MM"))

df_euro_avg = df_euro.groupBy("DT_COTACAO_EURO").agg(f.avg("VL_COTACAO_COMPRA").alias("VL_COTACAO_MES_EURO"))

# COMMAND ----------

df_municipio = df_municipio.select("ds_municipio", "nm_uf", "sg_uf", "cd_ibge_6", "cd_ibge_municipio")

# COMMAND ----------

df_municipio = df_municipio.withColumn("ds_municipio_sg_uf", f.concat_ws("/", f.col("ds_municipio"), f.col("nm_uf")))

# COMMAND ----------

# tabela de id
tabela_id = df_monitor.select("_id").distinct()

# COMMAND ----------

# tabela cnpj
tabela_cnpj1 = df_monitor.select(f.col("_id"), f.explode(f.split(f.regexp_replace(f.col("cnpj_basico"), "[\\[\\]' ]", ""), ",")).alias("cnpj_basico"), f.when(f.col("cnpj_basico") == "nan", f.col("nome_empresa")).otherwise(None).alias("nome_empresa"))

# COMMAND ----------

tabela_cnpj2 = tabela_cnpj1.withColumn("NOME_EMPRESA", f.concat_ws(", ", f.col("NOME_EMPRESA")))

# COMMAND ----------

tabela_cnpj3 = tabela_cnpj2.join(tabela_auxiliar_empresas, tabela_cnpj2.NOME_EMPRESA == tabela_auxiliar_empresas.NOME, "left")

# COMMAND ----------

tabela_cnpj4 = tabela_cnpj3.dropDuplicates(["_ID", "CNPJ_BASICO"])

# COMMAND ----------

tabela_cnpj = tabela_cnpj4.withColumn("cnpj_basico", f.when(f.col("nome").isNotNull(), f.col("cnpj")).otherwise(f.col("cnpj_basico"))).select(tabela_cnpj2["_id"], f.col("cnpj_basico"), tabela_cnpj2["nome_empresa"]).drop("nome_empresa")

# COMMAND ----------

df_base_unica = df_base_unica \
    .filter(f.col("CD_MATRIZ_FILIAL") == 1) \
    .select("CD_CNPJ_BASICO", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA", "CD_NATUREZA_JURIDICA_RFB", "CD_CNAE20_SUBCLASSE_RFB", "CD_CNAE20_SUBCLASSE_SECUNDARIA", "INT_TEC_OCDE", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "PORTE_EURO_ESTAT", "DS_SIT_CADASTRAL", "CD_MUNICIPIO_RFB", f.to_date("DT_INICIO_ATIV", "yyyy-MM-dd").alias("DT_INICIO_ATIV"))

# COMMAND ----------

# final_df.coalesce(1).write.mode("Overwrite").parquet(destination)

# COMMAND ----------


