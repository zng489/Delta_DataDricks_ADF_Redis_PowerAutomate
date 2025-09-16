# Databricks notebook source
tables =  {"schema":"","table":"","raw_path":"/usr/oni/midr/","trusted_path":"/oni/midr/","prm_path":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------


dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

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
from pyspark.sql.functions import col, to_date, year, month, concat_ws
from pyspark.sql.types import DoubleType, IntegerType
import re
from core.string_utils import normalize_replace
from pyspark.sql.functions import concat, lit, col, substring
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

from pyspark.sql.functions import coalesce, col, trim
from pyspark.sql.functions import col, to_date, year
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, IntegerType

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
print(raw)

trusted = dls['folders']['trusted']
print(trusted)

prm_path = tables['prm_path']

usr = dls['systems']['raw']
print(usr)

raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
print(adl_raw)

trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
print(adl_trusted)

# COMMAND ----------

import unicodedata
import re

def save(dataframe, path):
      df = cf.append_control_columns(dataframe, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
      fechamento = "comercio_comtrade_tab_auxiliar_tabela_continentes"
      print(path.lower())
      print(f'{adl_trusted}{path.lower()}')
      df.write.format("parquet").mode("overwrite").save(f'{adl_trusted}{path.lower()}')

# COMMAND ----------


raw_path
parameters = ['fco/carteira', 'fco/contratacoes', 'fco/desembolsos', 'fne/carteira', 'fne/contratacoes', 'fne/desembolsos', 'fno/carteira', 'fno/contratacoes', 'fno/desembolsos']

dados = {}
for parameter in parameters:
  dados[f"{parameter}"] = spark.read.format("parquet").load(f"{adl_raw}{parameter}")

# COMMAND ----------

# MAGIC %md
# MAGIC # FCO

# COMMAND ----------

# MAGIC %md
# MAGIC ### fco/carteira

# COMMAND ----------

col_rename_map_fco_carteira = {
    "UF": "UF",
    "CODIGO_DO_MUNICIPIO": "COD_MUN",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "PESSOA_FISICA_OU_JURIDICA": "TP_PESSOA",
    "DATA_DA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "TIPOLOGIA_DO_MUNICIPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "TAXA_DE_JUROS": "VL_TAXA_JUROS",
    "FORMA_DA_TAXAS_DE_JUROS": "TP_TAXA_JUROS_FORMA",
    "SITUACAO_DA_OPERACAO": "TP_SITUACAO",
    "RATING": "TP_RATING",
    "QUANTIDADE_DE_CONTRATOS": "QTDE_CONTRATOS",
    "SALDO_DA_CARTEIRA": "VL_SALDO_CARTEIRA",
    "SALDO_EM_ATRASO": "VL_SALDO_ATRASO",
    "INTITUICAO_OPERADORA": "NM_OPERADORA"
}
for valor_da_chave_dicionario in list(dados.keys()):

  if valor_da_chave_dicionario == 'fco/carteira':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fco_carteira .items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #print(df.columns)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fco/contratacoes

# COMMAND ----------

col_rename_map_fco_contratacoes = {
    "UF": "UF",
    "CODIGO_DO_MUNICIPIO": "COD_MUN",
    "SETOR": "NM_SETOR",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "PORTE": "TP_PORTE",
    "NM_ARQ_IN": "NM_ARQ_IN",
    "RIDES": "RIDES",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "SETOR_2": "SETOR_2",
    "FONTE": "FONTE",
    "ANO_DA_CONTRATACAO": "ANO_DA_CONTRATACAO",
    "NUM_MES_ANO": "NUM_MES_ANO",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "TIPOLOGIA_DO_MUNICIPIO": "TP_MUN",
    "PESSOA_FISICA_OU_JURIDICA": "TP_PESSOA",
    "DATA_DA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "CNAE": "COD_CNAE",
    "PROGRAMA": "NM_PROGRAMA",
    "ATIVIDADE": "NM_ATIVIDADE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "TAXA_DE_JUROS": "VL_TAXA_JUROS",
    "FORMA_TAXA_JUROS": "TP_TAXA_JUROS_FORMA",
    "RATING": "TP_RATING",
    "QUANTIDADE_CONTRATOS": "QTDE_CONTRATOS",
    "VALOR_CONTRATADO": "VL_CONTRATADO",
    "INTITUICAO_OPERADORA": "NM_OPERADORA",
    "FCO_MULHERES_EMPREENDEDORAS": "TP_FCO_MULHERES",
    "QUANTIDADE_BENEFICIARIOS": "QTDE_BENEFICIARIOS",
    "MES_ANO": "MES_ANO",
    "NUM_MES": "NUM_MES"
}

for valor_da_chave_dicionario in list(dados.keys()):

  if valor_da_chave_dicionario == 'fco/contratacoes':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fco_contratacoes.items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    print(df.columns)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fco/desembolsos

# COMMAND ----------

col_rename_map_fco_desembolsos = {
    "UF": "UF",
    "CODIGO_DO_MUNICIPIO": "COD_MUN",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "TIPOLOGIA_DO_MUNICIPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "PESSOA_FISICA_OU_JURIDICA": "TP_PESSOA",
    "DATA_DA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "DATA_DO_DESEMBOLSO": "DT_DESEMBOLSO",
    "CNAE": "COD_CNAE",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "RATING": "TP_RATING",
    "QUANTIDADE_CONTRATOS": "QTDE_CONTRATOS",
    "VALOR_DESEMBOLSADO": "VL_DESEMBOLSADO",
    "INTITUICAO_OPERADORA": "NM_OPERADORA",
    "ANO_DO_DESEMBOLSO": "ANO_DESEMBOLSO"
}

for valor_da_chave_dicionario in list(dados.keys()):
  
  if valor_da_chave_dicionario == 'fco/desembolsos':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')


    for origem, destino in col_rename_map_fco_desembolsos.items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #print(df.columns)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # FNE

# COMMAND ----------

# MAGIC %md
# MAGIC ### fne/carteira

# COMMAND ----------

col_rename_map_fne_carteira = {
    "UF": "UF",
    "CODIGO_DE_MUNICIPIO": "COD_MUN",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "PESSOA_FISICA_JURIDICA": "TP_PESSOA",
    "DATA_DA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "TIPOLOGIA_MUNICIPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "CNAE": "COD_CNAE",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "TAXA_DE_JUROS": "VL_TAXA_JUROS",
    "TAXA_DE_JUROS_FORMA": "TP_TAXA_JUROS_FORMA",
    "SITUACAO": "TP_SITUACAO",
    "RATING": "TP_RATING",
    "QUANTIDADE_DE_CONTRATOS": "QTDE_CONTRATOS",
    "SALDO_DA_CARTEIRA_PORTARIA_MI_MF": "VL_SALDO_CARTEIRA_MI_MF",
    "SALDO_EM_ATRASO_PORTARIA_MI_MF": "VL_SALDO_ATRASO_MI_MF",
    "SALDO_DA_CARTEIRA_RES__2_682_99": "VL_SALDO_CARTEIRA",
    "SALDO_EM_ATRASO_RES__CMN_2682_99": "VL_SALDO_ATRASO",
    "SETOR_2": "NM_SETOR_2"
}


for valor_da_chave_dicionario in list(dados.keys()):

  if valor_da_chave_dicionario == 'fne/carteira':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fne_carteira.items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #print(df.columns)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fne/contratacoes

# COMMAND ----------

col_rename_map_fne_contratacoes = {
    "UF": "UF",
    "CODIGO_DE_MUNICIPIO": "COD_MUN",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "PESSOA_FISICA_JURIDICA": "TP_PESSOA",
    "DATA_DA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "TIPOLOGIA_MUNICIPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "CNAE": "COD_CNAE",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "TAXA_DE_JUROS": "VL_TAXA_JUROS",
    "TAXA_DE_JUROS_FORMA": "TP_TAXA_JUROS_FORMA",
    "SITUACAO": "TP_SITUACAO",
    "RATING": "TP_RATING",
    "QUANTIDADE_DE_CONTRATOS": "QTDE_CONTRATOS",
    "SALDO_DA_CARTEIRA_PORTARIA_MI_MF": "VL_SALDO_CARTEIRA_MI_MF",
    "SALDO_EM_ATRASO_PORTARIA_MI_MF": "VL_SALDO_ATRASO_MI_MF",
    "SALDO_DA_CARTEIRA_RES__2_682_99": "VL_SALDO_CARTEIRA",
    "SALDO_EM_ATRASO_RES__CMN_2682_99": "VL_SALDO_ATRASO",
    "SETOR_2": "NM_SETOR_2"
}

for valor_da_chave_dicionario in list(dados.keys()):

  if valor_da_chave_dicionario == 'fne/contratacoes':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fne_contratacoes.items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #print(df.columns)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fne/desembolsos

# COMMAND ----------

col_rename_map_fne_desembolsos = {
    "UF": "UF",
    "CÓDIGO_DE_MUNICÍPIO": "COD_MUN",
    "NOME_DO_MUNICÍPIO": "NM_MUN",
    "TIPOLOGIA_MUNICÍPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIÁRIDO": "TP_SEMIARIDO",
    "PESSOA_FISICA_JURÍDICA": "TP_PESSOA",
    "DATA_DA_CONTRATAÇÃO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "DATA_DO_DESEMBOLSO": "DT_DESEMBOLSO",
    "CNAE": "COD_CNAE",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERAÇÃO": "DSC_FINALID",
    "RISCO_DA_OPERAÇÃO": "TP_RISCO",
    "RATING": "TP_RATING",
    "QTDE_DE_BENEFICIÁRIOS": "QTDE_BENEFICIARIOS",
    "QUANTIDADE_DE_CONTRATOS": "QTDE_CONTRATOS",
    "VALOR_DESEMBOLSADO": "VL_DESEMBOLSADO",
    "INSTITUIÇÃO_OPERADORA": "NM_OPERADORA",
    "SEXO": "TP_SEXO",
    "SETOR_2": "NM_SETOR_2"
}


for valor_da_chave_dicionario in list(dados.keys()):

  if valor_da_chave_dicionario == 'fne/desembolsos':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fne_desembolsos.items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #print(df.columns)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # FNO

# COMMAND ----------

# MAGIC %md
# MAGIC ### fno/carteira

# COMMAND ----------

col_rename_map_fno_carteira = {
    "UF": "UF",
    "CODIGO_DE_MUNICIPIO": "COD_MUN",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "PESSOA_FISICA_OU_JURIDICA": "TP_PESSOA",
    "DATA_DA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "TIPOLOGIA_DO_MUNICIPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "TAXA_DE_JUROS": "VL_TAXA_JUROS",
    "TAXA_DE_JUROS_FORMA": "TP_TAXA_JUROS_FORMA",
    "SITUACAO_DA_OPERACAO": "TP_SITUACAO",
    "RATING": "TP_RATING",
    "QUANTIDADE_CONTRATOS": "QTDE_CONTRATOS",
    "SALDO_DA_CARTEIRA": "VL_SALDO_CARTEIRA",
    "SALDO_EM_ATRASO_PORTARIA_MI_MF": "VL_SALDO_ATRASO_MI_MF",
    "SALDO_DA_CARTEIRA_REGRA_DE_MERCADO": "VL_SALDO_CARTEIRA_MERCADO",
    "SALDO_EM_ATRASO_RESOLUCAO_CMN": "VL_SALDO_ATRASO_CMN"
}


for valor_da_chave_dicionario in list(dados.keys()):
 
  if valor_da_chave_dicionario == 'fno/carteira':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fno_carteira.items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fno/contratacoes

# COMMAND ----------

col_rename_map_fno_contratacoes = {
    "UF": "UF",
    "CODIGO_DE_MUNICIPIO": "COD_MUN",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "TIPOLOGIA_DO_MUNICIPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "PESSOA_FISICA_OU_JURIDICA": "TP_PESSOA",
    "DATA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "CNAE": "COD_CNAE",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "TAXA_DE_JUROS": "VL_TAXA_JUROS",
    "TAXA_DE_JUROS_FORMA": "TP_TAXA_JUROS_FORMA",
    "RATING": "TP_RATING",
    "VALOR_CONTRATADO": "VL_CONTRATADO",
    "INSTITUICAO_OPERADORA": "NM_OPERADORA",
    "ATENDIDO_PELAS_CONDICOES_DO_FNO_AMAZONIA_PARA_ELAS": "TP_FCO_MULHERES",
    "QTDE_DE_BENEFICIARIOS": "QTDE_BENEFICIARIOS",
    "RIDES": "RIDES",
    "DSC_SETOR_2": "NM_SETOR_2",
    "DSC_FONTE": "NM_FONTE",
    "QUANTIDADE_DE_CONTRATOS": "QTDE_CONTRATOS"
}


for valor_da_chave_dicionario in list(dados.keys()):

  if valor_da_chave_dicionario == 'fno/contratacoes':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fno_contratacoes.items():
        df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fno/desembolsos

# COMMAND ----------

col_rename_map_fno_desembolsos = {
    "UF": "UF",
    "CODIGO_DE_MUNICIPIO": "COD_MUN",
    "NOME_DO_MUNICIPIO": "NM_MUN",
    "TIPOLOGIA_DO_MUNICIPIO": "TP_MUN",
    "FAIXA_DE_FRONTEIRA": "TP_FRONTEIRA",
    "SEMIARIDO": "TP_SEMIARIDO",
    "PESSOA_FISICA_OU_JURIDICA": "TP_PESSOA",
    "DATA_CONTRATACAO": "DT_CONTR",
    "VENCIMENTO_FINAL": "DT_VENC",
    "DATA_DO_DESEMBOLSO": "DT_DESEMBOLSO",
    "CNAE": "COD_CNAE",
    "SETOR": "NM_SETOR",
    "PROGRAMA": "NM_PROGRAMA",
    "LINHA_DE_FINANCIAMENTO": "NM_LINHA_FIN",
    "ATIVIDADE": "NM_ATIVIDADE",
    "PORTE": "TP_PORTE",
    "FINALIDADE_DA_OPERACAO": "DSC_FINALID",
    "RISCO_DA_OPERACAO": "TP_RISCO",
    "RATING": "TP_RATING",
    "QUANTIDADE_DE_CONTRATOS": "QTDE_CONTRATOS",
    "VALOR_DESEMBOLSADO": "VL_DESEMBOLSADO",
    "INSTITUICAO_OPERADORA": "NM_OPERADORA",
    "ATENDIDO_PELAS_CONDICOES_DO_FNO_AMAZONIA_PARA_ELAS": "TP_FCO_MULHERES",
    "TAXA_DE_JUROS_FORMA": "TP_TAXA_JUROS_FORMA",
    "QTDE_DE_BENEFICIARIOS": "QTDE_BENEFICIARIOS"
}


for valor_da_chave_dicionario in list(dados.keys()):
  if valor_da_chave_dicionario == 'fno/desembolsos':
    df = dados[valor_da_chave_dicionario].drop('dh_insercao_raw','nr_reg')

    for origem, destino in col_rename_map_fno_desembolsos.items():
      df = df.withColumnRenamed(origem, destino)

    print(valor_da_chave_dicionario)
    df = tcf.add_control_fields(df, adf)
    save(df, valor_da_chave_dicionario)
    #df.display()
    #df