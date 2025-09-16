# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

tables = {"schema":"", "table":"", "trusted_path_1":"/oni/observatorio_nacional/monitor_investimentos/painel_monitor/dim_cnae/", "trusted_path_2":"/oni/observatorio_nacional/monitor_investimentos/painel_monitor/empresas_auxiliar/", "trusted_path_3":"/oni/observatorio_nacional/monitor_investimentos/painel_monitor/estrutura_setorial/", "trusted_path_4":"/oni/observatorio_nacional/monitor_investimentos/painel_monitor/mongodb_monitor__investimento/", "trusted_path_5":"/oni/bacen/cotacao/dolar_americano/", "trusted_path_6":"/oni/bacen/cotacao/euro/", "trusted_path_7":"/oni/ibge/scn/pib_anual/municipios/", "business_path_1":"/oni/bases_referencia/municipios/", "business_path_2":"/oni/base_unica_cnpjs/cnpjs_rfb_rais/", "destination":"/oni/observatorio_nacional/monitor_investimentos/painel_monitor/", "databricks":{"notebook":"/biz/oni/painel_monitor/trs_biz_e_biz_biz_monitor_de_investimentos"}, "prm_path":""}

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

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

trusted = dls['folders']['trusted']
business = dls['folders']['business']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'

trusted_path_2 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_2'])
adl_trusted_2 = f'{var_adls_uri}{trusted_path_2}'


trusted_path_3 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_3'])
adl_trusted_3 = f'{var_adls_uri}{trusted_path_3}'


trusted_path_4 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_4'])
adl_trusted_4 = f'{var_adls_uri}{trusted_path_4}'


trusted_path_5 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_5'])
adl_trusted_5 = f'{var_adls_uri}{trusted_path_5}'


trusted_path_6 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_6'])
adl_trusted_6 = f'{var_adls_uri}{trusted_path_6}'


trusted_path_7 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_7'])
adl_trusted_7 = f'{var_adls_uri}{trusted_path_7}'

# COMMAND ----------

business_path_1 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_1'])
adl_business_1 = f'{var_adls_uri}{business_path_1}'


business_path_2 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_2'])
adl_business_2  = f'{var_adls_uri}{business_path_2}'

# COMMAND ----------

# trs
tabela_cnae_dim = spark.read.parquet(f'{adl_trusted_1}') \
    .withColumn("CD_CNAE_DIVISAO", f.lpad(f.col("CD_CNAE_DIVISAO").cast("string"), 2, "0")) \
    .withColumn("CD_CNAE_GRUPO", f.lpad(f.col("CD_CNAE_GRUPO").cast("string"), 3, "0")) \
    .withColumn("CD_CNAE_CLASSE", f.lpad(f.col("CD_CNAE_CLASSE").cast("string"), 5, "0")) \
    .withColumn("CD_CNAE_SUBCLASSE", f.lpad(f.col("CD_CNAE_SUBCLASSE").cast("string"), 7, "0"))


tabela_auxiliar_empresas = spark.read.parquet(f'{adl_trusted_2}').withColumn("CNPJ", f.lpad(f.col("CNPJ").cast("string"), 8, "0"))

estrutura_setorial = spark.read.parquet(f'{adl_trusted_3}')

df_monitor_completo = spark.read.parquet(f'{adl_trusted_4}')

df_dolar = spark.read.parquet(f'{adl_trusted_5}')

df_euro = spark.read.parquet(f'{adl_trusted_6}')

df_pib_munic = spark.read.parquet(f'{adl_trusted_7}')


# biz
df_municipio = spark.read.parquet(f'{adl_business_1}')

df_base_unica = spark.read.parquet(f'{adl_business_2}')

# COMMAND ----------

df_monitor_completo2 = df_monitor_completo.withColumn("DT_ANUNCIO", f.to_date(f.col("DT_ANUNCIO"), "yyyy-MM-dd"))

# COMMAND ----------

df_monitor = df_monitor_completo2.filter(f.col("DT_ANUNCIO") > f.lit("2023-05-31"))

# COMMAND ----------

df_monitor = df_monitor.withColumn("SETOR_INVESTIMENTO", f.expr("transform(SETOR_INVESTIMENTO, x -> regexp_replace(x, 'Fabricação de produtos de metal, exceto máquinas\\ne equipamentos', 'Fabricação de produtos de metal, exceto máquinas e equipamentos'))")
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
tabela_id = df_monitor.select("_ID").distinct()

# COMMAND ----------

# tabela cnpj
tabela_cnpj1 = df_monitor.select(f.col("_ID"), f.explode(f.split(f.regexp_replace(f.col("CNPJ_BASICO"), "[\\[\\]' ]", ""), ",")).alias("CNPJ_BASICO"), f.when(f.col("CNPJ_BASICO") == "nan", f.col("NOME_EMPRESA")).otherwise(None).alias("NOME_EMPRESA"))

# COMMAND ----------

tabela_cnpj2 = tabela_cnpj1.withColumn("NOME_EMPRESA", f.concat_ws(", ", f.col("NOME_EMPRESA")))

# COMMAND ----------

tabela_cnpj3 = tabela_cnpj2.join(tabela_auxiliar_empresas, tabela_cnpj2.NOME_EMPRESA == tabela_auxiliar_empresas.NOME, "left")

# COMMAND ----------

tabela_cnpj4 = tabela_cnpj3.dropDuplicates(["_ID", "CNPJ_BASICO"])

# COMMAND ----------

tabela_cnpj = tabela_cnpj4.withColumn("CNPJ_BASICO", f.when(f.col("NOME").isNotNull(), f.col("CNPJ")).otherwise(f.col("CNPJ_BASICO"))).select(tabela_cnpj2["_ID"], f.col("CNPJ_BASICO"), tabela_cnpj2["NOME_EMPRESA"]).drop("NOME_EMPRESA")

# COMMAND ----------

df_base_unica = df_base_unica \
    .filter(f.col("CD_MATRIZ_FILIAL") == 1) \
    .select("CD_CNPJ_BASICO", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA", "CD_NATUREZA_JURIDICA_RFB", "CD_CNAE20_SUBCLASSE_RFB", "CD_CNAE20_SUBCLASSE_SECUNDARIA", "INT_TEC_OCDE", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "PORTE_EURO_ESTAT", "DS_SIT_CADASTRAL", "CD_MUNICIPIO_RFB", f.to_date("DT_INICIO_ATIV", "yyyy-MM-dd").alias("DT_INICIO_ATIV"))

# COMMAND ----------

# Realizar o join entre tabela_cnpj e df_base_unica_filtered
tabela_cnpj_completa = tabela_cnpj.join(df_base_unica, tabela_cnpj["CNPJ_BASICO"] == df_base_unica["CD_CNPJ_BASICO"], "left").drop(df_base_unica["CD_CNPJ_BASICO"])

# COMMAND ----------

# tabela de uf com municipios
tabela_local = df_monitor.select("_ID", f.explode(f.col("UF")).alias("UF"), "MUNICIPIOS")

# COMMAND ----------

# Remover caracteres indesejados e converter a coluna 'municipios' para um array
tabela_local2 = tabela_local.withColumn("MUNICIPIOS_ARRAY", f.split(f.regexp_replace(f.col("MUNICIPIOS"), "[\\[\\]']", ""), ","))

# COMMAND ----------

# Explodir a coluna 'municipios_array'
tabela_local3 = tabela_local2.select("_ID", "UF", f.explode(f.col("MUNICIPIOS_ARRAY")).alias("MUNICIPIOS"))

# COMMAND ----------

# Adicionar a coluna 'uf_munic' concatenando 'uf' e 'municipios'
tabela_local4 = tabela_local3.withColumn("UF_MUNIC", f.concat_ws("/", f.col("MUNICIPIOS"), f.col("UF")))

# COMMAND ----------

# Remover espaços em branco no início dos valores da coluna 'uf_munic'
tabela_local4 = tabela_local4.withColumn("UF_MUNIC", f.trim(f.col("UF_MUNIC")))

# COMMAND ----------

# Realiza o join entre tabela_local4 e df_municipio
joined_df = tabela_local4.join(df_municipio, tabela_local4["UF_MUNIC"] == df_municipio["ds_municipio_sg_uf"], "left")

# COMMAND ----------

# Cria a flag indicando se o valor está presente nas duas tabelas
result_df = joined_df.withColumn("FLAG", f.when(f.col("ds_municipio_sg_uf").isNotNull(), f.lit(1)).otherwise(f.lit(0)))

# COMMAND ----------

# Seleciona as colunas de tabela_local4 e adiciona a coluna flag, ds_municipio_sg_uf e cd_ibge_municipio
final_uf = result_df.select(tabela_local4["_ID"], tabela_local4["UF"], tabela_local4["MUNICIPIOS"], tabela_local4["UF_MUNIC"], f.col("FLAG"), f.col("ds_municipio_sg_uf"), f.col("CD_IBGE_MUNICIPIO"))

# COMMAND ----------

final_uf2 = final_uf.withColumn("MUNICIPIOS", f.when(f.col("FLAG") == 0, f.lit("nan")).otherwise(f.col("MUNICIPIOS")))

# COMMAND ----------

final_uf3 = final_uf2.withColumn("UF_MUNIC", f.when(f.col("FLAG") == 0, f.regexp_replace(f.col("UF_MUNIC"), r"^[^/]+", "nan")).otherwise(f.col("UF_MUNIC")))

# COMMAND ----------

# Remove linhas duplicadas de final_uf3
final_uf3 = final_uf3.dropDuplicates()

# COMMAND ----------

# Filtra as linhas onde flag = 1 ou uf_munic começa com "nan"
final_filtered_df = final_uf3.filter((f.col("flag") == 1) | (f.col("UF_MUNIC").startswith("nan")))

# COMMAND ----------

# Apaga as colunas 'uf_munic' e 'flag'
tabela_uf = final_filtered_df.drop("UF_MUNIC", "FLAG")

# COMMAND ----------

# tabela investimento
tabela_invest = df_monitor.select("_ID", "DT_ANUNCIO", "OBJETIVO", "DESC", "NOVO_PAC", "MOEDA", "VALOR", "UNIDADE_VALOR", "ORIGEM_RECURSO", "NOME_RESPONSAVEL", "DIA_HORA_PREENCHIMENTO", "ANO_CONCLUSAO", "ANO_INICIO")


# COMMAND ----------

# Adiciona a coluna unidade_valor2 com os valores correspondentes
tabela_invest2 = tabela_invest.withColumn("UNIDADE_EXTENSO",
    f.when(f.col("UNIDADE_VALOR") == "Milhões", 1000000)
    .when(f.col("UNIDADE_VALOR") == "Bilhões", 1000000000)
    .when(f.col("UNIDADE_VALOR") == "Trilhões", 1000000000000))

# COMMAND ----------

# Adiciona a coluna valor_expandido que multiplica valor por unidade_valor2
tabela_invest3 = tabela_invest2.withColumn("VALOR_EXPANDIDO", f.col("VALOR") * f.col("UNIDADE_EXTENSO"))

# COMMAND ----------

tabela_invest4 = tabela_invest3.drop("UNIDADE_EXTENSO")

# COMMAND ----------

# extrai mês e ano da coluna dt_anuncio
tabela_invest4 = tabela_invest4.withColumn("YEAR_MONTH", f.date_format(f.col("DT_ANUNCIO"), "yyyy-MM"))

# COMMAND ----------

# join entre a tabela de investimento com df_dolar e df_euro
tabela_invest5 = tabela_invest4 \
    .join(df_dolar_avg, tabela_invest4.YEAR_MONTH == df_dolar_avg.DT_COTACAO_DOLAR, "left") \
    .join(df_euro_avg, tabela_invest4.YEAR_MONTH == df_euro_avg.DT_COTACAO_EURO, "left")

# COMMAND ----------

# adiciona a coluna valor_real com os valores correspondentes
tabela_invest6 = tabela_invest5.withColumn("VALOR_REAL",
    f.when(f.col("MOEDA") == "R$ - Brazilian real", f.col("VALOR_EXPANDIDO"))
    .when(f.col("MOEDA") == "$ - United States dollar", f.col("VALOR_EXPANDIDO") * f.col("VL_COTACAO_MES_DOLAR"))
    .when(f.col("MOEDA") == "€ - European Euro", f.col("VALOR_EXPANDIDO") * f.col("VL_COTACAO_MES_EURO")))

# COMMAND ----------

# arredonda valor_real para 2 casas decimais
tabela_invest6 = tabela_invest6.withColumn("VALOR_REAL", f.round(f.col("VALOR_REAL"), 2))

# COMMAND ----------

# Seleciona as colunas finais desejadas
tabela_investimento = tabela_invest6.select("_ID", "DT_ANUNCIO", "OBJETIVO", "DESC", "NOVO_PAC", "MOEDA", "VALOR", "UNIDADE_VALOR", "VALOR_EXPANDIDO", "VALOR_REAL", "ORIGEM_RECURSO", "NOME_RESPONSAVEL", "DIA_HORA_PREENCHIMENTO", "ANO_CONCLUSAO", "ANO_INICIO")

# COMMAND ----------

# Tabela de setor_investimento
tabela_setor1 = df_monitor.select("_ID", f.explode("SETOR_INVESTIMENTO").alias("SETOR_INVESTIMENTO"))

# COMMAND ----------

# Realiza o join entre tabela_setor1 e tabela_cnae_dim
tabela_setor2 = tabela_setor1.join(tabela_cnae_dim, tabela_setor1["SETOR_INVESTIMENTO"] == tabela_cnae_dim["NM_CNAE_DIVISAO"], how="left").select(tabela_setor1["_ID"], tabela_setor1["SETOR_INVESTIMENTO"], tabela_cnae_dim["NM_CNAE_DIVISAO"], tabela_cnae_dim["CD_CNAE_DIVISAO"])

# COMMAND ----------

# Remove linhas duplicadas
tabela_setor3 = tabela_setor2.dropDuplicates().drop("NM_CNAE_DIVISAO").withColumnRenamed("CD_CNAE_DIVISAO", "CD_CNAE_DIV_SETOR_INVEST")

# COMMAND ----------

tabela_setor = tabela_setor3.withColumn("CD_CNAE_DIV_SETOR_INVEST", f.lpad(f.col("CD_CNAE_DIV_SETOR_INVEST").cast("string"), 2, "0"))

# COMMAND ----------

# explode a coluna fonte e selecionar os campos necessários
tabela_source = df_monitor_completo.select("_ID", f.explode("FONTE").alias("FONTE")).select("_ID", f.col("FONTE.portal").alias("PORTAL"), f.col("FONTE.titulo").alias("TITULO"), f.col("FONTE.url").alias("URL"))

# COMMAND ----------

df_pib_munic = df_pib_munic \
                         .select("NR_ANO", "SG_UF", "VL_PIB_RS1000") \
                         .filter(f.col("NR_ANO") == 2021) \
                         .withColumn("VL_PIB_RS1000", f.col("VL_PIB_RS1000") * 1000)

# COMMAND ----------

tabela_pib = df_pib_munic.groupBy("NR_ANO", "SG_UF").agg(f.sum("VL_PIB_RS1000").alias("total_pib"))

# COMMAND ----------

# Realiza o join entre as tabelas e cria uma só
joined_df = tabela_investimento.join(tabela_uf, on="_ID", how="inner") \
                               .join(tabela_setor, on="_ID", how="inner") \
                               .join(tabela_source, on="_ID", how="inner") \
                               .join(tabela_cnpj_completa, on="_ID", how="inner")

# COMMAND ----------

# Substitui valores null por "nan"
replacement_values = {
    "DT_ANUNCIO": "1111-11-11", "DT_INICIO_ATIV": "1111-11-11",  # Colunas no formato de data
    "OBJETIVO": "nan", "DESC": "nan", "MOEDA": "nan", "UNIDADE_VALOR": "nan", "ORIGEM_RECURSO": "nan", 
    "NOME_RESPONSAVEL": "nan", "DIA_HORA_PREENCHIMENTO": "nan", "ANO_CONCLUSAO": "nan", "ANO_INICIO": "nan", 
    "UF": "nan", "SETOR_INVESTIMENTO": "nan", "TITULO": "nan", "PORTAL": "nan", "URL": "nan", "CNPJ_BASICO": "nan", 
    "NM_RAZAO_SOCIAL_RECEITA_EMPRESA": "nan", "CD_CNAE20_SUBCLASSE_RFB": "nan", "CD_NATUREZA_JURIDICA_RFB": "nan", 
    "CD_CNAE20_SUBCLASSE_SECUNDARIA": "nan", "INT_TEC_OCDE": "nan", "DS_PORTE_EMPRESA": "nan", "PORTE_SEBRAE": "nan", 
    "PORTE_EURO_ESTAT": "nan", "CD_CNAE_DIV_SETOR_INVEST": "nan",  # Colunas no formato de string
    "VALOR": 99999999, "VALOR_EXPANDIDO": 99999999, "VALOR_REAL": 99999999, "CD_MUNICIPIO_RFB": 9999999  # Colunas no formato de integer
}


# COMMAND ----------

joined_df = joined_df.na.fill(replacement_values)

# COMMAND ----------

# Conta o número de combinações únicas de uf, setor_investimento e outras colunas para cada _id
count_combinations_df = joined_df.groupBy("_ID").agg(f.countDistinct("UF", "SETOR_INVESTIMENTO", "TITULO", "PORTAL", "URL", "CNPJ_BASICO", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA", "CD_CNAE20_SUBCLASSE_RFB", "CD_NATUREZA_JURIDICA_RFB", "CD_CNAE20_SUBCLASSE_SECUNDARIA", "INT_TEC_OCDE", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "PORTE_EURO_ESTAT", "CD_MUNICIPIO_RFB").alias("NUM_COMBINATIONS"))

# COMMAND ----------

# Realiza o join com a contagem de combinações únicas
joined_with_count_df = joined_df.join(count_combinations_df, on="_ID", how="inner")

# COMMAND ----------

# Divide os valores conforme o número de combinações únicas para cada _id
final_df1 = joined_with_count_df.withColumn("VALOR", f.col("VALOR") / f.col("NUM_COMBINATIONS")) \
                               .withColumn("VALOR_EXPANDIDO", f.col("VALOR_EXPANDIDO") / f.col("NUM_COMBINATIONS")) \
                               .withColumn("VALOR_REAL", f.col("VALOR_REAL") / f.col("NUM_COMBINATIONS"))

# COMMAND ----------

# Seleciona as colunas desejadas
final_df2 = final_df1.select("_ID", "DT_ANUNCIO", "OBJETIVO", "DESC", "NOVO_PAC", "MOEDA", "VALOR", "UNIDADE_VALOR", "VALOR_EXPANDIDO", "VALOR_REAL", "ORIGEM_RECURSO", "NOME_RESPONSAVEL", "DIA_HORA_PREENCHIMENTO", "ANO_CONCLUSAO", "ANO_INICIO", "UF", "SETOR_INVESTIMENTO", "CD_CNAE_DIV_SETOR_INVEST", "TITULO", "PORTAL", "URL", "CNPJ_BASICO", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA", "CD_CNAE20_SUBCLASSE_RFB", "CD_NATUREZA_JURIDICA_RFB", "CD_CNAE20_SUBCLASSE_SECUNDARIA", "INT_TEC_OCDE", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "PORTE_EURO_ESTAT", "CD_MUNICIPIO_RFB", "DT_INICIO_ATIV")

# COMMAND ----------

# Remove linhas duplicadas
final_df3 = final_df2.dropDuplicates()

# COMMAND ----------

# Aplicar a transformação na coluna CD_CNAE20_SUBCLASSE_SECUNDARIA
final_df4 = final_df3.withColumn("CD_CNAE20_SUBCLASSE_SECUNDARIA", f.when(   f.col("CD_CNAE20_SUBCLASSE_SECUNDARIA").isNotNull(), f.coalesce(f.when(f.concat_ws(',', f.array_distinct(f.expr("filter(transform(split(CD_CNAE20_SUBCLASSE_SECUNDARIA, ','), x -> case when substring(x, 1, 2) = substring(CD_CNAE_DIV_SETOR_INVEST, 1, 2) then substring(x, 1, 2) else null end), x -> x is not null)"))) == "", f.lit("nan")).otherwise( f.concat_ws(',', f.array_distinct( f.expr("filter(transform(split(CD_CNAE20_SUBCLASSE_SECUNDARIA, ','), x -> case when substring(x, 1, 2) = substring(CD_CNAE_DIV_SETOR_INVEST, 1, 2) then substring(x, 1, 2) else null end), x -> x is not null)")))), f.lit("nan"))).otherwise("nan"))

# COMMAND ----------

final_df = final_df4.withColumnRenamed("_id", "ID_NOTICIA") \
                     .withColumnRenamed("dt_anuncio", "DT_ANUNCIO") \
                     .withColumnRenamed("objetivo", "DS_OBJETIVO") \
                     .withColumnRenamed("desc", "DS_DESC") \
                     .withColumnRenamed("novo_pac", "FL_NOVO_PAC") \
                     .withColumnRenamed("moeda", "DS_MOEDA") \
                     .withColumnRenamed("valor", "VL_VALOR") \
                     .withColumnRenamed("unidade_valor", "VL_UNIDADE") \
                     .withColumnRenamed("valor_expandido", "VL_EXPANDIDO") \
                     .withColumnRenamed("valor_real", "VL_REAL") \
                     .withColumnRenamed("origem_recurso", "DS_ORIGEM_RECURSO") \
                     .withColumnRenamed("nome_responsavel", "NM_NOME_RESPONSAVEL") \
                     .withColumnRenamed("dia_hora_preenchimento", "DH_DIA_HORA_PREENCHIMENTO") \
                     .withColumnRenamed("ano_conclusao", "ANO_CONCLUSAO") \
                     .withColumnRenamed("ano_inicio", "ANO_INICIO") \
                     .withColumnRenamed("uf", "SG_UF") \
                     .withColumnRenamed("setor_investimento", "DS_SETOR_INVESTIMENTO") \
                     .withColumnRenamed("cd_cnae_div_setor_invest", "CD_CNAE_DIV_SETOR_INVEST") \
                     .withColumnRenamed("titulo", "DS_TITULO") \
                     .withColumnRenamed("portal", "DS_PORTAL") \
                     .withColumnRenamed("url", "DS_URL") \
                     .withColumnRenamed("cnpj_basico", "CD_CNPJ_BASICO") \
                     .withColumnRenamed("NM_RAZAO_SOCIAL_RECEITA_EMPRESA", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA") \
                     .withColumnRenamed("CD_CNAE20_SUBCLASSE_RFB", "CD_CNAE20_SUBCLASSE_RFB") \
                     .withColumnRenamed("CD_NATUREZA_JURIDICA_RFB", "CD_NATUREZA_JURIDICA_RFB") \
                     .withColumnRenamed("CD_CNAE20_SUBCLASSE_SECUNDARIA", "CD_CNAE_DIV_SECUND_REL_SETOR_INVEST") \
                     .withColumnRenamed("INT_TEC_OCDE", "DS_INT_TEC_OCDE") \
                     .withColumnRenamed("DS_PORTE_EMPRESA", "DS_PORTE_EMPRESA") \
                     .withColumnRenamed("PORTE_SEBRAE", "DS_PORTE_SEBRAE") \
                     .withColumnRenamed("PORTE_EURO_ESTAT", "DS_PORTE_EURO_STAT") \
                     .withColumnRenamed("CD_MUNICIPIO_RFB", "CD_MUNICIPIO_RFB") \
                     .withColumnRenamed("DT_INICIO_ATIV", "DT_INICIO_ATIV")

# COMMAND ----------

final_df = tcf.add_control_fields(final_df, adf, layer="biz")

# COMMAND ----------

biz_path = "{business}{destination}".format(business=business, destination=tables['destination'])
adl_biz = f'{var_adls_uri}{biz_path}'

# COMMAND ----------

print(adl_biz)

# COMMAND ----------

final_df.write.mode("Overwrite").parquet(adl_biz)
