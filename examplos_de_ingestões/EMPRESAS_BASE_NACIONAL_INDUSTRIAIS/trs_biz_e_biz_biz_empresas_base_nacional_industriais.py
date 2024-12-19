# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


tables = {"schema":"", "table":"", 
          "trusted_path_1":"/oni/empresas_base_nacional_industriais/",
          "trusted_path_2":"/oni/observatorio_nacional/dimensionamento_sesi/base_industrias/",
          "business_path_1":"/oni/bases_referencia/municipios/", 
          "business_path_2":"/oni/base_unica_cnpjs/cnpjs_rfb_rais/", 
          "business_path_3":"/oni/bases_referencia/cnae_20/cnae_divisao/",          
          "destination":"/oni/empresas_base_nacional_industriais/", 
          "databricks":{"notebook":"/biz/oni/empresas_base_nacional_industriais/trs_biz_e_biz_biz_empresas_base_nacional_industriais"}, 
          "prm_path":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

import re
import os
import pandas as pd
from functools import reduce
import pyspark.sql.functions as f
from trs_control_field import trs_control_field as tcf
from pyspark.sql.functions import col, substring, lpad, when, lit

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

# COMMAND ----------

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'
print(adl_trusted_1)

trusted_path_2 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_2'])
adl_trusted_2 = f'{var_adls_uri}{trusted_path_2}'
print(adl_trusted_2)

business_path_1 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_1'])
adl_business_1 = f'{var_adls_uri}{business_path_1}'
print(adl_business_1)

business_path_2 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_2'])
adl_business_2 = f'{var_adls_uri}{business_path_2}'
print(adl_business_2)

business_path_3 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_3'])
adl_business_3 = f'{var_adls_uri}{business_path_3}'
print(adl_business_3)

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination'])
adl_destination_path = f'{var_adls_uri}{destination_path}'
print(adl_destination_path)

# COMMAND ----------

df_base_nacional_grupos = spark.read.parquet(adl_trusted_1)

# EMPRESAS - LATITUDE E LONGGITUDE
df_lat_long_empresas = spark.read.parquet(adl_trusted_2)

# LISTA DE MUNICIPIOS
df_municipios = spark.read.parquet(adl_business_1)

# LISTA DE BASE ÚNICA (CRITÉRIOS: SOMENTE ATIVOS)
df_base_unica = spark.read.parquet(adl_business_2)

cnaes20_divisao = spark.read.parquet(adl_business_3)

# COMMAND ----------

df_lat_long_empresas = df_lat_long_empresas \
  .select("CD_CNPJ","VL_LATITUDE","VL_LONGITUDE") \
  .withColumnRenamed("CD_CNPJ", "CD_CNPJ_14")

# COMMAND ----------

df_municipios = df_municipios \
  .drop("SG_UF")

# COMMAND ----------

# enriquecer com municipios

df_base_unica = df_base_unica \
  .join(df_municipios, df_municipios["cd_ibge_municipio"] == df_base_unica["CD_MUNICIPIO_RFB"],"left")

# COMMAND ----------

# Renomeando o COD_CNAE e COD_CNAE_DIVISAO após trazer da base única e exclusão dos campos CNAE após renomeados

df_base_unica = df_base_unica \
  .withColumnRenamed("CD_CNAE20_SUBCLASSE_RFB", "CD_CNAE_SUBCLASSE_BU") \
  .withColumnRenamed("CD_CNAE20_DIVISAO", "COD_CNAE_DIVISAO_BU") \
  .drop("CD_CNAE20_SUBCLASSE_RFB","CD_CNAE20_DIVISAO")

# COMMAND ----------

# Criação de flag INDUSTRIAL

df_base_unica = df_base_unica.withColumn(
    "CNAE_INDUSTRIAL",
    f.when(f.col("COD_CNAE_DIVISAO_BU").between('05', '43'), "Sim")
     .otherwise("Não")
)

# COMMAND ----------

# Criação de MEI após cruzamento com base única

df_base_unica = df_base_unica.withColumn(
    "MEI",
    f.when((f.col("CD_OPCAO_MEI") == ' ') | f.col("CD_OPCAO_MEI").isNull() | (f.col("CD_OPCAO_MEI") == 0), 'Não')
     .when(f.col("CD_OPCAO_MEI") == 1, 'Sim')
     .otherwise(None)
)

# COMMAND ----------

# DBTITLE 1,BASE INDUSTRIA
# Realização de filtros para construção da Base Indústria
# Filtros:
#   CNAE Industrial diferente da Baixada
#   MEI igual a Não
#   Que possua CNAE Industrial (05 a 43)
#   Que possua mais de um vínculo empregatício


base_Industria = df_base_unica \
    .filter(f.col("CD_SIT_CADASTRAL") != "08") \
    .filter(f.col("MEI") == "Não") \
    .filter((f.col("CNAE_INDUSTRIAL") == "Sim")) \
    .filter((f.col("QT_VINC_ATIV") > 1 ) & f.col("QT_VINC_ATIV").isNotNull())

# COMMAND ----------

# DBTITLE 1,FILTRAGEM BASE INDUSTRIA POR UF
# Construção da Base Indústria por UF

base_Industria_AC = base_Industria \
    .filter(f.col("SG_UF") == "AC")

base_Industria_AL = base_Industria \
    .filter(f.col("SG_UF") == "AL")

base_Industria_AM = base_Industria \
    .filter(f.col("SG_UF") == "AM")

base_Industria_AP = base_Industria \
    .filter(f.col("SG_UF") == "AP")

base_Industria_BA = base_Industria \
    .filter(f.col("SG_UF") == "BA")

base_Industria_CE = base_Industria \
    .filter(f.col("SG_UF") == "CE")

base_Industria_DF = base_Industria \
    .filter(f.col("SG_UF") == "DF")

base_Industria_ES = base_Industria \
    .filter(f.col("SG_UF") == "ES")

base_Industria_GO = base_Industria \
    .filter(f.col("SG_UF") == "GO")

base_Industria_MA = base_Industria \
    .filter(f.col("SG_UF") == "MA")

base_Industria_MG = base_Industria \
    .filter(f.col("SG_UF") == "MG")

base_Industria_MS = base_Industria \
    .filter(f.col("SG_UF") == "MS")

base_Industria_MT = base_Industria \
    .filter(f.col("SG_UF") == "MT")

base_Industria_PA = base_Industria \
    .filter(f.col("SG_UF") == "PA")

base_Industria_PB = base_Industria \
    .filter(f.col("SG_UF") == "PB")

base_Industria_PE = base_Industria \
    .filter(f.col("SG_UF") == "PE")

base_Industria_PI = base_Industria \
    .filter(f.col("SG_UF") == "PI")

base_Industria_PR = base_Industria \
    .filter(f.col("SG_UF") == "PR")

base_Industria_RJ = base_Industria \
    .filter(f.col("SG_UF") == "RJ")

base_Industria_RN = base_Industria \
    .filter(f.col("SG_UF") == "RN")

base_Industria_RO = base_Industria \
    .filter(f.col("SG_UF") == "RO")

base_Industria_RR = base_Industria \
    .filter(f.col("SG_UF") == "RR")

base_Industria_RS = base_Industria \
    .filter(f.col("SG_UF") == "RS")

base_Industria_SC = base_Industria \
    .filter(f.col("SG_UF") == "SC")

base_Industria_SE = base_Industria \
    .filter(f.col("SG_UF") == "SE")

base_Industria_SP = base_Industria \
    .filter(f.col("SG_UF") == "SP")

base_Industria_TO = base_Industria \
    .filter(f.col("SG_UF") == "TO")

# COMMAND ----------

# DBTITLE 1,BASE INDUSTRIA - UFs
colunas_desejadas = ["SG_UF","CD_CNPJ_BASICO", "DS_MUNICIPIO", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA", "PORTE_SEBRAE", "DS_MATRIZ_FILIAL","QT_VINC_ATIV", "COD_CNAE_DIVISAO_BU", "DS_NAT_JURIDICA_RFB", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "INT_TEC_OCDE"]

base_Industria_AC = base_Industria_AC.select(*colunas_desejadas)
base_Industria_AL = base_Industria_AL.select(*colunas_desejadas)
base_Industria_AM = base_Industria_AM.select(*colunas_desejadas)
base_Industria_AP = base_Industria_AP.select(*colunas_desejadas)
base_Industria_BA = base_Industria_BA.select(*colunas_desejadas)
base_Industria_CE = base_Industria_CE.select(*colunas_desejadas)
base_Industria_DF = base_Industria_DF.select(*colunas_desejadas)
base_Industria_ES = base_Industria_ES.select(*colunas_desejadas)
base_Industria_GO = base_Industria_GO.select(*colunas_desejadas)
base_Industria_MA = base_Industria_MA.select(*colunas_desejadas)
base_Industria_MG = base_Industria_MG.select(*colunas_desejadas)
base_Industria_MS = base_Industria_MS.select(*colunas_desejadas)
base_Industria_MT = base_Industria_MT.select(*colunas_desejadas)
base_Industria_PA = base_Industria_PA.select(*colunas_desejadas)
base_Industria_PB = base_Industria_PB.select(*colunas_desejadas)
base_Industria_PE = base_Industria_PE.select(*colunas_desejadas)
base_Industria_PI = base_Industria_PI.select(*colunas_desejadas)
base_Industria_PR = base_Industria_PR.select(*colunas_desejadas)
base_Industria_RJ = base_Industria_RJ.select(*colunas_desejadas)
base_Industria_RN = base_Industria_RN.select(*colunas_desejadas)
base_Industria_RO = base_Industria_RO.select(*colunas_desejadas)
base_Industria_RR = base_Industria_RR.select(*colunas_desejadas)
base_Industria_RS = base_Industria_RS.select(*colunas_desejadas)
base_Industria_SC = base_Industria_SC.select(*colunas_desejadas)
base_Industria_SE = base_Industria_SE.select(*colunas_desejadas)
base_Industria_SP = base_Industria_SP.select(*colunas_desejadas)
base_Industria_TO = base_Industria_TO.select(*colunas_desejadas)

# COMMAND ----------

# DBTITLE 1,BASE ESTABELECIMENTOS POR UF
# Atendimentos nacional

base_estabelecimentos_por_empresa_em_ufs = base_Industria \
  .withColumnRenamed("CD_CNPJ_BASICO","CD_CNPJ_BASICO_ATEND_NAC") \
  .drop("CD_CNPJ_BASICO") \
  .filter((f.col("SG_UF") != "EX") & (f.col("SG_UF") != "BR")) \
  .groupBy("CD_CNPJ_BASICO_ATEND_NAC") \
  .pivot("SG_UF") \
  .count() \
  .fillna(0)

# COMMAND ----------

# DBTITLE 1,GERAÇÃO VINCULOS EMPRESA - CONFORME PORTE SEBRAE
df_qtd_vinculos_empresa = df_base_unica \
  .withColumnRenamed("CD_CNPJ_BASICO", "CD_CNPJ_BASICO_EMPRESA") \
  .filter(f.col("CD_SIT_CADASTRAL") != "08") \
  .filter((f.col("SG_UF") != "EX") & (f.col("SG_UF") != "BR")) \
  .filter(f.col("MEI") == "Não") \
  .withColumn(
    "QT_VINC_ATIV",
    f.when(f.col("QT_VINC_ATIV").isNull(), f.lit(0))  # substitui nulos por 0
     .otherwise(f.col("QT_VINC_ATIV"))
  ) \
  .groupBy("CD_CNPJ_BASICO_EMPRESA") \
  .agg(f.sum("QT_VINC_ATIV").cast("int").alias("QT_VINC_EMPRESA")) \
  .withColumn("PORTE_SEBRAE_EMPRESA", 
              f.when(f.col("QT_VINC_EMPRESA") <= 19, "Micro")
               .when((f.col("QT_VINC_EMPRESA") >= 20) & (f.col("QT_VINC_EMPRESA") <= 99), "Pequeno")
               .when((f.col("QT_VINC_EMPRESA") >= 100) & (f.col("QT_VINC_EMPRESA") <= 499), "Médio")
               .when(f.col("QT_VINC_EMPRESA") >= 500, "Grande")
               .otherwise("Não declarado")
              ) \
  .orderBy("CD_CNPJ_BASICO_EMPRESA")

# COMMAND ----------

matrizes_empresas_brasil = df_base_unica \
  .withColumnRenamed("DS_MATRIZ_FILIAL", "DS_MATRIZ_FILIAL_MATRIZES_BRASIL") \
  .withColumnRenamed("CD_CNPJ_BASICO", "CD_CNPJ_BASICO_MATRIZES_BRASIL") \
  .filter(f.col("DS_MATRIZ_FILIAL_MATRIZES_BRASIL") == "Matriz") \
  .filter(f.col("CD_SIT_CADASTRAL") != "08") \
  .filter(f.col("MEI") == "Não") \
  .select("CD_CNPJ_BASICO_MATRIZES_BRASIL", "DS_MATRIZ_FILIAL_MATRIZES_BRASIL", "SG_UF") \
  .withColumnRenamed("SG_UF", "UF_MATRIZ")

# COMMAND ----------

# Disponibilização da base nacional com os atendimentos por UF
base_Nacional_AC = base_Industria_AC \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_AC["CD_CNPJ_BASICO"],"left")

base_Nacional_AL = base_Industria_AL \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_AL["CD_CNPJ_BASICO"],"left")

base_Nacional_AM = base_Industria_AM \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_AM["CD_CNPJ_BASICO"],"left")

base_Nacional_AP = base_Industria_AP \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_AP["CD_CNPJ_BASICO"],"left")

base_Nacional_BA = base_Industria_BA \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_BA["CD_CNPJ_BASICO"],"left")

base_Nacional_CE = base_Industria_CE \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_CE["CD_CNPJ_BASICO"],"left")

base_Nacional_DF = base_Industria_DF \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_DF["CD_CNPJ_BASICO"],"left")

base_Nacional_ES = base_Industria_ES \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_ES["CD_CNPJ_BASICO"],"left")

base_Nacional_GO = base_Industria_GO \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_GO["CD_CNPJ_BASICO"],"left")

base_Nacional_MA = base_Industria_MA \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_MA["CD_CNPJ_BASICO"],"left")

base_Nacional_MG = base_Industria_MG \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_MG["CD_CNPJ_BASICO"],"left")

base_Nacional_MS = base_Industria_MS \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_MS["CD_CNPJ_BASICO"],"left")

base_Nacional_MT = base_Industria_MT \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_MT["CD_CNPJ_BASICO"],"left")

base_Nacional_PA = base_Industria_PA \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_PA["CD_CNPJ_BASICO"],"left")

base_Nacional_PB = base_Industria_PB \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_PB["CD_CNPJ_BASICO"],"left")

base_Nacional_PE = base_Industria_PE \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_PE["CD_CNPJ_BASICO"],"left")

base_Nacional_PI = base_Industria_PI \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_PI["CD_CNPJ_BASICO"],"left")

base_Nacional_PR = base_Industria_PR \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_PR["CD_CNPJ_BASICO"],"left")

base_Nacional_RJ = base_Industria_RJ \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_RJ["CD_CNPJ_BASICO"],"left")

base_Nacional_RN = base_Industria_RN \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_RN["CD_CNPJ_BASICO"],"left")

base_Nacional_RO = base_Industria_RO \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_RO["CD_CNPJ_BASICO"],"left")

base_Nacional_RR = base_Industria_RR \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_RR["CD_CNPJ_BASICO"],"left")

base_Nacional_RS = base_Industria_RS \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_RS["CD_CNPJ_BASICO"],"left")

base_Nacional_SC = base_Industria_SC \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_SC["CD_CNPJ_BASICO"],"left")

base_Nacional_SE = base_Industria_SE \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_SE["CD_CNPJ_BASICO"],"left")

base_Nacional_SP = base_Industria_SP \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_SP["CD_CNPJ_BASICO"],"left")

base_Nacional_TO = base_Industria_TO \
  .join(base_estabelecimentos_por_empresa_em_ufs, base_estabelecimentos_por_empresa_em_ufs["CD_CNPJ_BASICO_ATEND_NAC"] == base_Industria_TO["CD_CNPJ_BASICO"],"left")

# COMMAND ----------

# Disponibilização da base nacional com os atendimentos por UF
base_Nacional_AC_com_matriz = base_Nacional_AC \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_AC["CD_CNPJ_BASICO"],"left")

base_Nacional_AL_com_matriz = base_Nacional_AL \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_AL["CD_CNPJ_BASICO"],"left")

base_Nacional_AM_com_matriz = base_Nacional_AM \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_AM["CD_CNPJ_BASICO"],"left")

base_Nacional_AP_com_matriz = base_Nacional_AP \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_AP["CD_CNPJ_BASICO"],"left")

base_Nacional_BA_com_matriz = base_Nacional_BA \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_BA["CD_CNPJ_BASICO"],"left")

base_Nacional_CE_com_matriz = base_Nacional_CE \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_CE["CD_CNPJ_BASICO"],"left")

base_Nacional_DF_com_matriz = base_Nacional_DF \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_DF["CD_CNPJ_BASICO"],"left")

base_Nacional_ES_com_matriz = base_Nacional_ES \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_ES["CD_CNPJ_BASICO"],"left")

base_Nacional_GO_com_matriz = base_Nacional_GO \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_GO["CD_CNPJ_BASICO"],"left")

base_Nacional_MA_com_matriz = base_Nacional_MA \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_MA["CD_CNPJ_BASICO"],"left")

base_Nacional_MG_com_matriz = base_Nacional_MG \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_MG["CD_CNPJ_BASICO"],"left")

base_Nacional_MS_com_matriz = base_Nacional_MS \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_MS["CD_CNPJ_BASICO"],"left")

base_Nacional_MT_com_matriz = base_Nacional_MT \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_MT["CD_CNPJ_BASICO"],"left")

base_Nacional_PA_com_matriz = base_Nacional_PA \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_PA["CD_CNPJ_BASICO"],"left")

base_Nacional_PB_com_matriz = base_Nacional_PB \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_PB["CD_CNPJ_BASICO"],"left")

base_Nacional_PE_com_matriz = base_Nacional_PE \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_PE["CD_CNPJ_BASICO"],"left")

base_Nacional_PI_com_matriz = base_Nacional_PI \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_PI["CD_CNPJ_BASICO"],"left")

base_Nacional_PR_com_matriz = base_Nacional_PR \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_PR["CD_CNPJ_BASICO"],"left")

base_Nacional_RJ_com_matriz = base_Nacional_RJ \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_RJ["CD_CNPJ_BASICO"],"left")

base_Nacional_RN_com_matriz = base_Nacional_RN \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_RN["CD_CNPJ_BASICO"],"left")

base_Nacional_RO_com_matriz = base_Nacional_RO \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_RO["CD_CNPJ_BASICO"],"left")

base_Nacional_RR_com_matriz = base_Nacional_RR \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_RR["CD_CNPJ_BASICO"],"left")

base_Nacional_RS_com_matriz = base_Nacional_RS \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_RS["CD_CNPJ_BASICO"],"left")

base_Nacional_SC_com_matriz = base_Nacional_SC \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_SC["CD_CNPJ_BASICO"],"left")

base_Nacional_SE_com_matriz = base_Nacional_SE \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_SE["CD_CNPJ_BASICO"],"left")

base_Nacional_SP_com_matriz = base_Nacional_SP \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_SP["CD_CNPJ_BASICO"],"left")

base_Nacional_TO_com_matriz = base_Nacional_TO \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_Industria_TO["CD_CNPJ_BASICO"],"left")

# COMMAND ----------

# DBTITLE 1,DEFINIÇÃO DE COLUNAS - EXIBIÇÃO
colunas_desejadas = ['SG_UF','CD_CNPJ_BASICO', 'DS_MUNICIPIO','NM_RAZAO_SOCIAL_RECEITA_EMPRESA','PORTE_SEBRAE','DS_MATRIZ_FILIAL','QT_VINC_ATIV','AC','AL','AM','AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO', 'UF_MATRIZ','COD_CNAE_DIVISAO_BU','DS_NAT_JURIDICA_RFB', 'DS_PORTE_EMPRESA', 'PORTE_SEBRAE', 'INT_TEC_OCDE']

base_Nacional_AC_com_matriz = base_Nacional_AC_com_matriz.select(*colunas_desejadas)
base_Nacional_AL_com_matriz = base_Nacional_AL_com_matriz.select(*colunas_desejadas)
base_Nacional_AM_com_matriz = base_Nacional_AM_com_matriz.select(*colunas_desejadas)
base_Nacional_AP_com_matriz = base_Nacional_AP_com_matriz.select(*colunas_desejadas)
base_Nacional_BA_com_matriz = base_Nacional_BA_com_matriz.select(*colunas_desejadas)
base_Nacional_CE_com_matriz = base_Nacional_CE_com_matriz.select(*colunas_desejadas)
base_Nacional_DF_com_matriz = base_Nacional_DF_com_matriz.select(*colunas_desejadas)
base_Nacional_ES_com_matriz = base_Nacional_ES_com_matriz.select(*colunas_desejadas)
base_Nacional_GO_com_matriz = base_Nacional_GO_com_matriz.select(*colunas_desejadas)
base_Nacional_MA_com_matriz = base_Nacional_MA_com_matriz.select(*colunas_desejadas)
base_Nacional_MG_com_matriz = base_Nacional_MG_com_matriz.select(*colunas_desejadas)
base_Nacional_MS_com_matriz = base_Nacional_MS_com_matriz.select(*colunas_desejadas)
base_Nacional_MT_com_matriz = base_Nacional_MT_com_matriz.select(*colunas_desejadas)
base_Nacional_PA_com_matriz = base_Nacional_PA_com_matriz.select(*colunas_desejadas)
base_Nacional_PB_com_matriz = base_Nacional_PB_com_matriz.select(*colunas_desejadas)
base_Nacional_PE_com_matriz = base_Nacional_PE_com_matriz.select(*colunas_desejadas)
base_Nacional_PI_com_matriz = base_Nacional_PI_com_matriz.select(*colunas_desejadas)
base_Nacional_PR_com_matriz = base_Nacional_PR_com_matriz.select(*colunas_desejadas)
base_Nacional_RJ_com_matriz = base_Nacional_RJ_com_matriz.select(*colunas_desejadas)
base_Nacional_RN_com_matriz = base_Nacional_RN_com_matriz.select(*colunas_desejadas)
base_Nacional_RO_com_matriz = base_Nacional_RO_com_matriz.select(*colunas_desejadas)
base_Nacional_RR_com_matriz = base_Nacional_RR_com_matriz.select(*colunas_desejadas)
base_Nacional_RS_com_matriz = base_Nacional_RS_com_matriz.select(*colunas_desejadas)
base_Nacional_SC_com_matriz = base_Nacional_SC_com_matriz.select(*colunas_desejadas)
base_Nacional_SE_com_matriz = base_Nacional_SE_com_matriz.select(*colunas_desejadas)
base_Nacional_SP_com_matriz = base_Nacional_SP_com_matriz.select(*colunas_desejadas)
base_Nacional_TO_com_matriz = base_Nacional_TO_com_matriz.select(*colunas_desejadas)

# COMMAND ----------

# DBTITLE 1,LISTA PARA CONTAGEM BASE NACIONAL
colunas_AC = ["AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
colunas_AL =["AC"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_AM = ["AC"	,"AL"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_AP = ["AC"	,"AL"	,"AM"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_BA = ["AC"	,"AL"	,"AM"	,"AP"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_CE = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_DF = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_ES = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_GO = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	, "MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_MA = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	, "MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_MG = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_MS = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_MT = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_PA = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_PB = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_PE = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_PI = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_PR = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_RJ = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_RN = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_RO = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	, "RR"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_RR = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RS"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_RS = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"SC"	,"SE"	,"SP"	,"TO"]
colunas_SC = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SE"	,"SP"	,"TO"]
colunas_SE = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SP"	,"TO"]
colunas_SP = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"TO"]
colunas_TO = ["AC"	,"AL"	,"AM"	,"AP"	,"BA"	,"CE"	,"DF"	,"ES"	,"GO"	,"MA"	,"MG"	,"MS"	,"MT"	,"PA"	,"PB"	,"PE"	,"PI"	,"PR"	,"RJ"	,"RN"	,"RO"	,"RR"	,"RS"	,"SC"	,"SE"	,"SP"]


# COMMAND ----------

base_Nacional_AC_com_matriz = base_Nacional_AC_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_AC])) \
  .filter(f.col("Total") > 0)

base_Nacional_AL_com_matriz = base_Nacional_AL_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_AL])) \
  .filter(f.col("Total") > 0)

base_Nacional_AM_com_matriz = base_Nacional_AM_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_AM])) \
  .filter(f.col("Total") > 0)

base_Nacional_AP_com_matriz = base_Nacional_AP_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_AP])) \
  .filter(f.col("Total") > 0)

base_Nacional_BA_com_matriz = base_Nacional_BA_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_BA])) \
  .filter(f.col("Total") > 0)

base_Nacional_CE_com_matriz = base_Nacional_CE_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_CE])) \
  .filter(f.col("Total") > 0)

base_Nacional_DF_com_matriz = base_Nacional_DF_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_DF])) \
  .filter(f.col("Total") > 0)

base_Nacional_ES_com_matriz = base_Nacional_ES_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_ES])) \
  .filter(f.col("Total") > 0)

base_Nacional_GO_com_matriz = base_Nacional_GO_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_GO])) \
  .filter(f.col("Total") > 0)

base_Nacional_MA_com_matriz = base_Nacional_MA_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_MA])) \
  .filter(f.col("Total") > 0)

base_Nacional_MG_com_matriz = base_Nacional_MG_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_MG])) \
  .filter(f.col("Total") > 0)

base_Nacional_MS_com_matriz = base_Nacional_MS_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_MS])) \
  .filter(f.col("Total") > 0)

base_Nacional_MT_com_matriz = base_Nacional_MT_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_MT])) \
  .filter(f.col("Total") > 0)

base_Nacional_PA_com_matriz = base_Nacional_PA_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_PA])) \
  .filter(f.col("Total") > 0)

base_Nacional_PB_com_matriz = base_Nacional_PB_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_PB])) \
  .filter(f.col("Total") > 0)

base_Nacional_PE_com_matriz = base_Nacional_PE_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_PE])) \
  .filter(f.col("Total") > 0)

base_Nacional_PI_com_matriz = base_Nacional_PI_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_PI])) \
  .filter(f.col("Total") > 0)

base_Nacional_PR_com_matriz = base_Nacional_PR_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_PR])) \
  .filter(f.col("Total") > 0)

base_Nacional_RJ_com_matriz = base_Nacional_RJ_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_RJ])) \
  .filter(f.col("Total") > 0)

base_Nacional_RN_com_matriz = base_Nacional_RN_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_RN])) \
  .filter(f.col("Total") > 0)

base_Nacional_RO_com_matriz = base_Nacional_RO_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_RO])) \
  .filter(f.col("Total") > 0)

base_Nacional_RR_com_matriz = base_Nacional_RR_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_RR])) \
  .filter(f.col("Total") > 0)

base_Nacional_RS_com_matriz = base_Nacional_RS_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_RS])) \
  .filter(f.col("Total") > 0)

base_Nacional_SC_com_matriz = base_Nacional_SC_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_SC])) \
  .filter(f.col("Total") > 0)

base_Nacional_SE_com_matriz = base_Nacional_SE_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_SE])) \
  .filter(f.col("Total") > 0)

base_Nacional_SP_com_matriz = base_Nacional_SP_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_SP])) \
  .filter(f.col("Total") > 0)

base_Nacional_TO_com_matriz = base_Nacional_TO_com_matriz \
  .withColumn("Total", reduce(lambda x, y: x + y, [f.col(c) for c in colunas_TO])) \
  .filter(f.col("Total") > 0)

# COMMAND ----------

base_nacional_final = base_Nacional_AC_com_matriz.union(base_Nacional_AL_com_matriz).union(base_Nacional_AP_com_matriz).union(base_Nacional_BA_com_matriz).union(base_Nacional_AM_com_matriz).union(base_Nacional_CE_com_matriz).union(base_Nacional_DF_com_matriz).union(base_Nacional_ES_com_matriz).union(base_Nacional_GO_com_matriz).union(base_Nacional_MA_com_matriz).union(base_Nacional_MG_com_matriz).union(base_Nacional_MS_com_matriz).union(base_Nacional_MT_com_matriz).union(base_Nacional_PA_com_matriz).union(base_Nacional_PB_com_matriz).union(base_Nacional_PE_com_matriz).union(base_Nacional_PI_com_matriz).union(base_Nacional_PR_com_matriz).union(base_Nacional_RJ_com_matriz).union(base_Nacional_RN_com_matriz).union(base_Nacional_RO_com_matriz).union(base_Nacional_RR_com_matriz).union(base_Nacional_RS_com_matriz).union(base_Nacional_SC_com_matriz).union(base_Nacional_SE_com_matriz).union(base_Nacional_SP_com_matriz).union(base_Nacional_TO_com_matriz)

# COMMAND ----------

base_nacional_final_impressao = base_nacional_final \
  .select("SG_UF","CD_CNPJ_BASICO", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA","DS_MATRIZ_FILIAL", 'AC','AL','AM','AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO','COD_CNAE_DIVISAO_BU', 'UF_MATRIZ', 'DS_NAT_JURIDICA_RFB', 'DS_PORTE_EMPRESA', 'PORTE_SEBRAE', 'INT_TEC_OCDE')

# COMMAND ----------

base_nacional_final_impressao_completo = base_nacional_final_impressao \
  .select("CD_CNPJ_BASICO") \
  .withColumnRenamed("CD_CNPJ_BASICO","CNPJ8") \
  .distinct()

# COMMAND ----------

base_entrega = base_nacional_final_impressao_completo \
  .join(df_base_unica, df_base_unica["CD_CNPJ_BASICO"] == base_nacional_final_impressao_completo["CNPJ8"],"left") \
  .join(matrizes_empresas_brasil, matrizes_empresas_brasil["CD_CNPJ_BASICO_MATRIZES_BRASIL"] == base_nacional_final_impressao_completo["CNPJ8"],"left")

# COMMAND ----------

base_entrega_final = base_entrega \
  .select("CD_CNPJ","CD_CNPJ_BASICO","NM_RAZAO_SOCIAL_RECEITA_EMPRESA","DS_MATRIZ_FILIAL", "SG_UF","DS_SIT_CADASTRAL","COD_CNAE_DIVISAO_BU", "UF_MATRIZ","QT_VINC_ATIV", "DS_NAT_JURIDICA_RFB", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "INT_TEC_OCDE")

# COMMAND ----------

base_entrega_final_grupos_apresentacao = base_entrega \
  .select("CD_CNPJ","CD_CNPJ_BASICO","NM_RAZAO_SOCIAL_RECEITA_EMPRESA","DS_MATRIZ_FILIAL", "SG_UF","DS_SIT_CADASTRAL","COD_CNAE_DIVISAO_BU", "UF_MATRIZ", "QT_VINC_ATIV", "DS_NAT_JURIDICA_RFB", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "INT_TEC_OCDE")
  
  # ,"Grupo"

# COMMAND ----------

base_entrega_final_grupos_apresentacao_cnae = base_entrega_final_grupos_apresentacao \
  .join(cnaes20_divisao, cnaes20_divisao["cd_cnae_divisao"] == base_entrega_final_grupos_apresentacao["COD_CNAE_DIVISAO_BU"],"left")

# COMMAND ----------

listagem_base_nacional = base_entrega_final_grupos_apresentacao_cnae \
  .select("CD_CNPJ","CD_CNPJ_BASICO","NM_RAZAO_SOCIAL_RECEITA_EMPRESA","DS_MATRIZ_FILIAL", "SG_UF","DS_SIT_CADASTRAL","COD_CNAE_DIVISAO_BU","nm_cnae_divisao", "UF_MATRIZ","QT_VINC_ATIV", "DS_NAT_JURIDICA_RFB", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "INT_TEC_OCDE")

  # "Grupo",

# COMMAND ----------

listagem_base_nacional_final = listagem_base_nacional \
  .filter(f.col("DS_SIT_CADASTRAL") != "Baixada") \
  .filter(f.col("SG_UF") != "EX") \
  .withColumn(
    "QT_VINC_ATIV",
    f.when(f.col("QT_VINC_ATIV").isNull(), "0")
     .otherwise(f.col("QT_VINC_ATIV"))
  )
  # .filter(f.col(1"DS_SIT_CADASTRAL") == "Ativa") \

# COMMAND ----------

listagem_base_nacional_final_painel = listagem_base_nacional_final \
  .withColumn("PORTE_SEBRAE",
              f.when(f.col("PORTE_SEBRAE").isNull(), "Não declarado")
              .otherwise(f.col("PORTE_SEBRAE"))
              )

# COMMAND ----------

listagem_base_nacional_final_painel_vincEmpresa = listagem_base_nacional_final_painel \
  .join(df_qtd_vinculos_empresa, listagem_base_nacional_final_painel["CD_CNPJ_BASICO"] == df_qtd_vinculos_empresa["CD_CNPJ_BASICO_EMPRESA"],"left")
  # .join(df_senai_ep_2023, listagem_base_nacional_final_painel["CD_CNPJ"] == df_senai_ep_2023["CNPJ_ESTAB_14_EP"],"left") \
  # .join(df_senai_sti_2023, listagem_base_nacional_final_painel["CD_CNPJ"] == df_senai_sti_2023["CNPJ_ESTAB_14_STI"],"left") \
  # .join(df_sesi_ssi_2023, listagem_base_nacional_final_painel["CD_CNPJ"] == df_sesi_ssi_2023["CNPJ_ESTAB_14_SSI"],"left") \
  # .join(df_sesi_ebc_2023, listagem_base_nacional_final_painel["CD_CNPJ"] == df_sesi_ebc_2023["CNPJ_ESTAB_14_EBC"],"left") \
  # .join(df_sesi_ssi_2023_servico_sst, listagem_base_nacional_final_painel["CD_CNPJ"] == df_sesi_ssi_2023_servico_sst["CNPJ_ESTAB_14_SST"],"left") \
  # .join(df_sesi_ssi_2023_servico_promocaoSaude, listagem_base_nacional_final_painel["CD_CNPJ"] == df_sesi_ssi_2023_servico_promocaoSaude["CNPJ_ESTAB_14_PROMOCAO_SAUDE"],"left")  

# COMMAND ----------

listagem_base_nacional_final_painel_vincEmpresa_fechamentoProducao_impressao = listagem_base_nacional_final_painel_vincEmpresa \
  .select("CD_CNPJ","CD_CNPJ_BASICO","NM_RAZAO_SOCIAL_RECEITA_EMPRESA","DS_MATRIZ_FILIAL", "SG_UF","DS_SIT_CADASTRAL","COD_CNAE_DIVISAO_BU","nm_cnae_divisao", "UF_MATRIZ","QT_VINC_ATIV", "DS_NAT_JURIDICA_RFB", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "PORTE_SEBRAE_EMPRESA", "INT_TEC_OCDE")

  # , "SENAI EP?", "SENAI STI?","SESI SSI?","SESI EBC?", "SESI SST?","SESI PROMOCAO SAUDE?""Grupo"

# COMMAND ----------

# enriquecer com latitude e longitude

listagem_base_nacional_final_painel_vincEmpresa_fechamentoProducao_impressao_lat_long = listagem_base_nacional_final_painel_vincEmpresa \
  .join(df_lat_long_empresas, listagem_base_nacional_final_painel_vincEmpresa["CD_CNPJ"] == df_lat_long_empresas["CD_CNPJ_14"],"left")

# COMMAND ----------

listagem_base_nacional_final_painel_vincEmpresa_fechamentoProducao_impressao_lat_long_impressao = listagem_base_nacional_final_painel_vincEmpresa_fechamentoProducao_impressao_lat_long \
  .select("CD_CNPJ","CD_CNPJ_BASICO","NM_RAZAO_SOCIAL_RECEITA_EMPRESA","DS_MATRIZ_FILIAL", "SG_UF","DS_SIT_CADASTRAL","COD_CNAE_DIVISAO_BU","nm_cnae_divisao", "UF_MATRIZ","QT_VINC_ATIV", "DS_NAT_JURIDICA_RFB", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "PORTE_SEBRAE_EMPRESA", "INT_TEC_OCDE","VL_LATITUDE","VL_LONGITUDE")

  # , "SENAI EP?", "SENAI STI?","SESI SSI?","SESI EBC?", "SESI SST?","SESI PROMOCAO SAUDE?""Grupo",

# COMMAND ----------

final_df = tcf.add_control_fields(listagem_base_nacional_final_painel_vincEmpresa_fechamentoProducao_impressao_lat_long_impressao, adf, layer="biz")
final_df.write.mode('overwrite').parquet(adl_destination_path, compression='snappy')