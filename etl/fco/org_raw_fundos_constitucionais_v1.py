# Databricks notebook source
file = {"namespace":"/oni", "file_folder":'/midr', 
        "file_subfolder":'/dados_fundos_constitucionais/',
        "raw_path":"/usr/oni/midr/",
        "prm_path": "", 
        "extension":"txt","column_delimiter":"","encoding":"iso-8859-1","null_value":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }

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
import re
from core.string_utils import normalize_replace
from pyspark.sql.functions import concat, lit, col
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import to_date
from pyspark.sql.types import DoubleType, IntegerType, DateType


# COMMAND ----------

#file = notebook_params.var_file
#dls = notebook_params.var_dls
#adf = notebook_params.var_adf

# COMMAND ----------


uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
adl_uld

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
adl_raw

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
prm_path

adl_raw

if not cf.directory_exists(dbutils, uld_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % uld_path)

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
import re
from pyspark.sql import DataFrame

def load_csv_to_dataframe(file_path: str) -> DataFrame:
    return (spark.read
                .option("delimiter", ";")
                .option("header", "true")
                .option("encoding", "utf-8")
                .csv(file_path))

# COMMAND ----------

lista_sub_nomes = []
for path in cf.list_subdirectory(dbutils, uld_path):
  for file_path in cf.list_subdirectory(dbutils, path):
    lista_sub_nomes.append(file_path)

# COMMAND ----------

lista_sub_nomes

# COMMAND ----------

def spark_frame(nome):
  lista = []
  for path in cf.list_subdirectory(dbutils, uld_path):
    for file_path in cf.list_subdirectory(dbutils, path):
      for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
        if arquivos_txt.endswith('.csv') and nome in file_path:
          lista.append(arquivos_txt)
          #print(arquivos_txt)

  dataframes = [] 
  for caminho in lista:
      df = load_csv_to_dataframe(f"{var_adls_uri}/{caminho}")
      print(f"{var_adls_uri}/{caminho}")
      df = cf.append_control_columns(df, dh_insercao_raw=None).drop('dh_insercao_raw','nr_reg')
      #df.display()
      print(df.count())
      dataframes.append(df)
      if dataframes:
          final_df = dataframes[0]
          for df in dataframes[1:]:
              final_df = final_df.unionByName(df, allowMissingColumns=True)
  return final_df


# COMMAND ----------

import unicodedata
import re

def normalizar_texto(texto):
    # Converte para minúsculas
    texto = texto.lower()
    # Remove acentos e cedilhas
    texto_sem_acentos = ''.join(
        c for c in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(c)
    )
    return texto_sem_acentos


# COMMAND ----------

import unicodedata
import re

'''
def save_delta(dataframe, path):

  # Ensure the path is empty or already a Delta table
  dbutils.fs.rm(path, recurse=True)

  df = cf.append_control_columns(dataframe, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
  dh_insercao_raw = datetime.datetime.now()
  df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))
  
  caminho = normalizar_texto( path)
  print(caminho)

  fechamento="Dados Fundos Constitucionais"
  df.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("userMetadata", fechamento) \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "auto") \
        .format("delta") \
        .save(caminho)
  return
'''


def save(dataframe, path):
      df = cf.append_control_columns(dataframe, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
      dh_insercao_raw = datetime.datetime.now()
      df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))
      fechamento = "comercio_comtrade_tab_auxiliar_tabela_continentes"
      print(path.lower())
      df.write.format("parquet").mode("overwrite").save(f'{path.lower()}')

# COMMAND ----------

# MAGIC %md
# MAGIC # FCO_CARTEIRA

# COMMAND ----------

nome_do_fundo = "/".join(lista_sub_nomes[0].split("/")[-2:])
FCO_CARTEIRA = spark_frame(nome_do_fundo)

for column in FCO_CARTEIRA.columns:
  FCO_CARTEIRA = FCO_CARTEIRA.withColumnRenamed(column, __normalize_str(column))

path=f'{adl_raw}{nome_do_fundo}'
print(path)
save(FCO_CARTEIRA, path)

# COMMAND ----------


'''
FCO_CARTEIRA = FCO_CARTEIRA.withColumn("TAXA_DE_JUROS", col("TAXA_DE_JUROS").cast(DoubleType())) \
    .withColumn("QUANTIDADE_DE_CONTRATOS", col("QUANTIDADE_DE_CONTRATOS").cast(IntegerType())) \
    .withColumn("SALDO_DA_CARTEIRA", col("SALDO_DA_CARTEIRA").cast(DoubleType())) \
    .withColumn("SALDO_EM_ATRASO", col("SALDO_EM_ATRASO").cast(DoubleType())) \
    .withColumn("DATA_DA_CONTRATACAO", to_date("DATA_DA_CONTRATACAO", "MM/yyyy")) \
    .withColumn("VENCIMENTO_FINAL", to_date("VENCIMENTO_FINAL", "MM/yyyy"))
'''
#FCO_CARTEIRA.select('DATA_DA_CONTRATACAO').distinct().display()

#print(FCO_CARTEIRA.select('DATA_DA_CONTRATACAO').count())
#nulos = FCO_CARTEIRA.filter(F.col("DATA_DA_CONTRATACAO").isNull()).count()#
#print(nulos)

#FCO_CARTEIRA.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # FCO_CONTRATACOES

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

nome_do_fundo = "/".join(lista_sub_nomes[1].split("/")[-2:])
print(nome_do_fundo)

FCO_CONTRATACOES = spark_frame(nome_do_fundo)

for column in FCO_CONTRATACOES.columns:
  FCO_CONTRATACOES = FCO_CONTRATACOES.withColumnRenamed(column, __normalize_str(column))


path=f'{adl_raw}{nome_do_fundo}'
print(path)
duplicate_columns = set()
seen = set()
for column in FCO_CONTRATACOES.columns:
  if column.lower() in seen:
    duplicate_columns.add(column)
  else:
    seen.add(column.lower())
print(duplicate_columns)
print(seen)

columns = FCO_CONTRATACOES.columns
new_columns = []
seen = set()
for col in columns:
  if col in seen:
      i = 1
      while col + f"_{i}" in seen:
        i += 1
      new_col = col + f"_REP_{i}"
      new_columns.append(new_col)
      seen.add(new_col)
  else:
      new_columns.append(col)
      seen.add(col)

FCO_CONTRATACOES = FCO_CONTRATACOES.toDF(*new_columns) 

FCO_CONTRATACOES = FCO_CONTRATACOES.withColumn(
    "DATA",
    F.when(
        ~F.col("DATA_DA_CONTRATRACAO").contains("/"),
        F.concat(
            F.substring(F.col("DATA_DA_CONTRATRACAO"), 5, 2),  # mês
            F.lit("/"),
            F.substring(F.col("DATA_DA_CONTRATRACAO"), 1, 4)   # ano
        )
    ).otherwise(F.col("DATA_DA_CONTRATRACAO"))  # mantém valor original se não tiver "/"
)


from pyspark.sql import functions as F

# dicionário de meses em português para número
mapa_meses = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
}

# cria expressão CASE WHEN (mapa)
expr_meses = F.create_map([F.lit(x) for x in sum(mapa_meses.items(), ())])

FCO_CONTRATACOES = FCO_CONTRATACOES.withColumn(
    "MES_NUM",
    F.when(F.col("NUM_MES_ANO").rlike("^[0-9]{6}$"),   # formato AAAAMM
           F.col("NUM_MES_ANO").substr(5, 2))
     .when(F.lower(F.col("NUM_MES_ANO")).isin(list(mapa_meses.keys())),   # nomes
           F.lpad(expr_meses[F.lower(F.col("NUM_MES_ANO"))].cast("string"), 2, "0"))
     .otherwise(F.lit(None))   # nulos, traços etc
)


FCO_CONTRATACOES = FCO_CONTRATACOES.withColumn(
    "MES_ANO",
    F.when(
        (F.col("MES_NUM").isNotNull()) & (F.col("NUM_ANO").isNotNull()),
        F.concat_ws("/", F.col("MES_NUM"), F.col("NUM_ANO"))
    ).otherwise(F.lit(None))
)

FCO_CONTRATACOES = (
    FCO_CONTRATACOES

    # UF
    .withColumn("UF", F.coalesce(F.col("UF"), F.col("DSC_UF")))
    .drop("DSC_UF")

    # CODIGO_DO_MUNICIPIO
    .withColumn("CODIGO_DO_MUNICIPIO", F.coalesce(F.col("CODIGO_DO_MUNICIPIO"), F.col("COD_MUNICIPIO"), F.col("CODIGO_DO_MUNICIPIO_REP_1")))
    .drop("COD_MUNICIPIO", "CODIGO_DO_MUNICIPIO_REP_1")

    # NOME_DO_MUNICIPIO
    .withColumn("NOME_DO_MUNICIPIO", F.coalesce(F.col("NOME_DO_MUNICIPIO"), F.col("MUNICIPIO"), F.col("DSC_MUNICIPIO")))
    .drop("MUNICIPIO", "DSC_MUNICIPIO")

    # LINHA_DE_FINANCIAMENTO
    .withColumn("LINHA_DE_FINANCIAMENTO", F.coalesce(F.col("LINHA_DE_FINANCIAMENTO"), F.col("DSC_LINHA_FINANCIAMENTO"), F.col("LINHA_DE_FINANCIAMENTO_REP_1")))
    .drop("DSC_LINHA_FINANCIAMENTO", "LINHA_DE_FINANCIAMENTO_REP_1")

    # FINALIDADE_DA_OPERACAO
    .withColumn("FINALIDADE_DA_OPERACAO", F.coalesce(F.col("FINALIDADE_DA_OPERACAO"), F.col("FINALIDADE"), F.col("DSC_FINALIDADE")))
    .drop("FINALIDADE", "DSC_FINALIDADE")

    # SETOR
    .withColumn("SETOR", F.coalesce(F.col("SETOR"), F.col("DSC_SETOR_1")))
    .drop("DSC_SETOR_1")

    # QUANTIDADE_CONTRATOS
    .withColumn("QUANTIDADE_CONTRATOS", F.coalesce(F.col("QUANTIDADE_CONTRATOS"), F.col("NR_OP_CONTRATADAS"), F.col("NUM_OPERACAO_CONTRATADA")))
    .drop("NR_OP_CONTRATADAS", "NUM_OPERACAO_CONTRATADA")

    # VALOR_CONTRATADO
    .withColumn("VALOR_CONTRATADO", F.coalesce(F.col("VALOR_CONTRATADO"), F.col("VALOR_CONTRATADO_RS_100"), F.col("VLR_CONTRATO")))
    .drop("VALOR_CONTRATADO_RS_100", "VLR_CONTRATO")

    # PORTE
    .withColumn("PORTE", F.coalesce(F.col("PORTE"), F.col("DSC_PORTE")))
    .drop("DSC_PORTE")

    # TIPOLOGIA_DO_MUNICIPIO
    .withColumn("TIPOLOGIA_DO_MUNICIPIO", F.coalesce(F.col("TIPOLOGIA_DO_MUNICIPIO"), F.col("TIPOLOGIA"), F.col("TIPOLOGIA_1")))
    .drop("TIPOLOGIA", "TIPOLOGIA_1")

    # RIDES
    .withColumn("RIDES", F.coalesce(F.col("RIDES"), F.col("RIDE")))
    .drop("RIDE")

    # FAIXA_DE_FRONTEIRA
    .withColumn("FAIXA_DE_FRONTEIRA", F.coalesce(F.col("FAIXA_DE_FRONTEIRA"), F.col("FAIXA_DE_FRONTEIRA_REP_1")))
    .drop("FAIXA_DE_FRONTEIRA_REP_1")

    # SEMIARIDO
    .withColumn("SEMIARIDO", F.coalesce(F.col("SEMIARIDO"), F.col("SEMIARIDO_REP_1")))
    .drop("SEMIARIDO_REP_1")

    # CNAE
    .withColumn("CNAE", F.coalesce(F.col("CNAE"), F.col("NUM_CNAE")))
    .drop("NUM_CNAE")

    # PROGRAMA
    .withColumn("PROGRAMA", F.coalesce(F.col("PROGRAMA"), F.col("DSC_PROGRAMA")))
    .drop("DSC_PROGRAMA")

    # ATIVIDADE
    .withColumn("ATIVIDADE", F.coalesce(F.col("ATIVIDADE"), F.col("DSC_ATIVIDADE")))
    .drop("DSC_ATIVIDADE")
)

FCO_CONTRATACOES = FCO_CONTRATACOES.drop("UNNAMED:_20", "UNNAMED:_21", "UNNAMED:_22","MES_NUM","DATA")

from pyspark.sql import functions as F

# dicionário mês em português -> número
mapa_meses = {
    "Janeiro": "01",
    "Fevereiro": "02",
    "Março": "03",
    "Abril": "04",
    "Maio": "05",
    "Junho": "06",
    "Julho": "07",
    "Agosto": "08",
    "Setembro": "09",
    "Outubro": "10",
    "Novembro": "11",
    "Dezembro": "12"
}

# cria expressão CASE WHEN baseada no dicionário
expr_case = F.when(F.col("NUM_MES_ANO").rlike("^[0-9]{6}$"), 
                  F.substring("NUM_MES_ANO", 5, 2))  # pega os 2 últimos dígitos do formato yyyymm

for nome, num in mapa_meses.items():
    expr_case = expr_case.when(F.col("NUM_MES_ANO") == nome, F.lit(num))

# trata valores inválidos (null, "-")
expr_case = expr_case.otherwise(F.lit(None))

FCO_CONTRATACOES = FCO_CONTRATACOES.withColumn("NUM_MES", expr_case)

FCO_CONTRATACOES = FCO_CONTRATACOES.withColumnRenamed("NUM_ANO", "ANO_DA_CONTRATAÇÃO")
from pyspark.sql.functions import col, split

FCO_CONTRATACOES = FCO_CONTRATACOES.withColumn("ANO_2018_2024", split(col("DATA_DA_CONTRATRACAO"), "/").getItem(1))


#FCO_CONTRATACOES = (
#    FCO_CONTRATACOES.withColumn("ANO_DA_CONTRATACAO", F.coalesce(F.col("ANO_2018_2024"), F.col("ANO"), F.col("ANO_DA_CONTRATACAO"))))

for column in FCO_CONTRATACOES.columns:
  FCO_CONTRATACOES = FCO_CONTRATACOES.withColumnRenamed(column, __normalize_str(column))

# COMMAND ----------

FCO_CONTRATACOES = (FCO_CONTRATACOES.withColumn("ANO_DA_CONTRATACAO", F.coalesce(F.col("ANO_2018_2024"), F.col("ANO"), F.col("ANO_DA_CONTRATACAO"))))

FCO_CONTRATACOES = FCO_CONTRATACOES.drop("ANO_2018_2024","ANO", )

FCO_CONTRATACOES = cf.append_control_columns(FCO_CONTRATACOES, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
path=f'{adl_raw}{nome_do_fundo}'
print(path)

save(FCO_CONTRATACOES, path)

# COMMAND ----------

#from pyspark.sql.functions import sum as _sum

# COMMAND ----------

#df_soma = FCO_CONTRATACOES.agg(_sum("VALOR_CONTRATADO").alias("soma_valor"))
#df_soma.display()

# COMMAND ----------


'''
FCO_CONTRATACOES = FCO_CONTRATACOES.withColumn("ANO", F.col("ANO").cast(IntegerType())) \
    .withColumn("NR_OP_CONTRATADAS", F.col("NR_OP_CONTRATADAS").cast(IntegerType())) \
    .withColumn("VALOR_CONTRATADO_RS_100", F.col("VALOR_CONTRATADO_RS_100").cast(DoubleType())) \
    .withColumn("NUM_OPERACAO_CONTRATADA", F.col("NUM_OPERACAO_CONTRATADA").cast(IntegerType())) \
    .withColumn("VLR_CONTRATO", F.col("VLR_CONTRATO").cast(DoubleType())) \
    .withColumn("NUM_ANO", F.col("NUM_ANO").cast(IntegerType())) \
    .withColumn("NUM_CNAE", F.col("NUM_CNAE").cast(IntegerType())) \
    .withColumn("NUM_MES_ANO", F.col("NUM_MES_ANO").cast(IntegerType())) \
    .withColumn("TAXA_DE_JUROS", F.col("TAXA_DE_JUROS").cast(DoubleType())) \
    .withColumn("QUANTIDADE_CONTRATOS", F.col("QUANTIDADE_CONTRATOS").cast(IntegerType())) \
    .withColumn("VALOR_CONTRATADO", F.col("VALOR_CONTRATADO").cast(DoubleType())) \
    .withColumn("QUANTIDADE_BENEFICIARIOS", F.col("QUANTIDADE_BENEFICIARIOS").cast(IntegerType()))
'''

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # FCO_DESEMBOLSOS

# COMMAND ----------

from pyspark.sql.functions import coalesce, col

nome_do_fundo = "/".join(lista_sub_nomes[2].split("/")[-2:])
FCO_DESEMBOLSOS = spark_frame(nome_do_fundo)

for column in FCO_DESEMBOLSOS.columns:
  FCO_DESEMBOLSOS = FCO_DESEMBOLSOS.withColumnRenamed(column, __normalize_str(column))

FCO_DESEMBOLSOS = FCO_DESEMBOLSOS.withColumnRenamed("ANO", "ANO_DO_DESEMBOLSO")
FCO_DESEMBOLSOS = cf.append_control_columns(FCO_DESEMBOLSOS, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
path=f'{adl_raw}{nome_do_fundo}'
print(path)


save(FCO_DESEMBOLSOS, path)

# COMMAND ----------

# MAGIC %md
# MAGIC # FNE_CARTEIRA

# COMMAND ----------

print(lista_sub_nomes[3])

nome_do_fundo = "/".join(lista_sub_nomes[3].split("/")[-2:])
FNE_CARTEIRA = spark_frame(nome_do_fundo)

for column in FNE_CARTEIRA.columns:
  FNE_CARTEIRA = FNE_CARTEIRA.withColumnRenamed(column, __normalize_str(column))

FNE_CARTEIRA = (
    FNE_CARTEIRA
    .withColumn(
        "SETOR",
        coalesce(col("SETOR"), col("SETOR_1"))
    )
    .drop("SETOR_1")
)

path=f'{adl_raw}{nome_do_fundo}'
print(path)

save(FNE_CARTEIRA, path)

# COMMAND ----------

# MAGIC %md
# MAGIC # FNE_CONTRATACOES

# COMMAND ----------

lista_sub_nomes[4]

# COMMAND ----------

from pyspark.sql.functions import coalesce, col

nome_do_fundo = "/".join(lista_sub_nomes[4].split("/")[-2:])
FNE_CONTRATACOES = spark_frame(nome_do_fundo)

for column in FNE_CONTRATACOES.columns:
  FNE_CONTRATACOES = FNE_CONTRATACOES.withColumnRenamed(column, __normalize_str(column))


FNE_CONTRATACOES = FNE_CONTRATACOES.withColumn(
    "UF",
    coalesce(col("UF_MUN"), col("DSC_UF"), col("UF"))
).drop('UF_MUN','DSC_UF')

FNE_CONTRATACOES = FNE_CONTRATACOES.withColumn(
    'CODIGO_DE_MUNICIPIO',
    coalesce(col('CD_MUN_IBGE'), col('COD_MUNICIPIO'), col('CODIGO_DE_MUNICIPIO'))
).drop('CD_MUN_IBGE','COD_MUNICIPIO')


FNE_CONTRATACOES = (
    FNE_CONTRATACOES
    # Município
    .withColumn(
        "NOME_DO_MUNICIPIO",
        coalesce(col("NOME_DO_MUNICIPIO"), col("MUNICIPIO"), col("DSC_MUNICIPIO"))
    ).drop("MUNICIPIO", "DSC_MUNICIPIO")

    # Tipologia
    .withColumn(
        "TIPOLOGIA_MUNICIPIO",
        coalesce(col("TIPOLOGIA_MUNICIPIO"), col("TIPOLOGIA_1"), col("TIPOLOGIA"))
    ).drop("TIPOLOGIA_1", "TIPOLOGIA")

    # Data da contratação
    .withColumn(
        "DATA_DA_CONTRATACAO",
        coalesce(col("DATA_DA_CONTRATACAO"), col("ANO"), col("NUM_ANO"), col("NUM_MES_ANO"))
    ).drop("ANO", "NUM_ANO", "NUM_MES_ANO")

    # CNAE
    .withColumn(
        "CNAE",
        coalesce(col("CNAE"), col("NUM_CNAE"))
    ).drop("NUM_CNAE")

    # Setor
    .withColumn(
        "SETOR",
        coalesce(col("SETOR"), col("DSC_SETOR_1"))
    ).drop("DSC_SETOR_1")

    # Programa
    .withColumn(
        "PROGRAMA",
        coalesce(col("PROGRAMA"), col("DSC_PROGRAMA"))
    ).drop("DSC_PROGRAMA")

    # Linha de financiamento
    .withColumn(
        "LINHA_DE_FINANCIAMENTO",
        coalesce(col("LINHA_DE_FINANCIAMENTO"), col("DSC_LINHA_FINANCIAMENTO"))
    ).drop("DSC_LINHA_FINANCIAMENTO")

    # Atividade
    .withColumn(
        "ATIVIDADE",
        coalesce(col("ATIVIDADE"), col("DSC_ATIVIDADE"))
    ).drop("DSC_ATIVIDADE")

    # Porte
    .withColumn(
        "PORTE",
        coalesce(col("PORTE"), col("DSC_PORTE"))
    ).drop("DSC_PORTE")

    # Quantidade de contratos
    .withColumn(
        "QUANTIDADE_DE_CONTRATOS",
        coalesce(col("QUANTIDADE_DE_CONTRATOS"), col("QTDE"), col("NUM_OPERACAO_CONTRATADA"))
    ).drop("QTDE", "NUM_OPERACAO_CONTRATADA")

    # Valor contratado
    .withColumn(
        "VALOR_CONTRATADO",
        coalesce(col("VALOR_CONTRATADO"), col("VLR_CONTRATO"))
    ).drop("VLR_CONTRATO")

    # Finalidade da operação
    .withColumn(
        "FINALIDADE_DA_OPERACAO",
        coalesce(col("FINALIDADE_DA_OPERACAO"), col("FINALIDADE"), col("DSC_FINALIDADE"))
    ).drop("FINALIDADE", "DSC_FINALIDADE")
)

path=f'{adl_raw}{nome_do_fundo}'
print(path)

save(FNE_CONTRATACOES, path)

# COMMAND ----------

'''
duplicate_columns = set()
seen = set()
for column in FNE_CONTRATACOES.columns:
  if column.lower() in seen:
    duplicate_columns.add(column)
  else:
    seen.add(column.lower())
print(duplicate_columns)
print(seen)

columns = FNE_CONTRATACOES.columns
new_columns = []
seen = set()
for col in columns:
  if col in seen:
      i = 1
      while col + f"_{i}" in seen:
        i += 1
      new_col = col + f"_REP_{i}"
      new_columns.append(new_col)
      seen.add(new_col)
  else:
      new_columns.append(col)
      seen.add(col)
FNE_CONTRATACOES = FNE_CONTRATACOES.toDF(*new_columns)
'''

# COMMAND ----------

# MAGIC %md
# MAGIC # FNE_DESEMBOLSOS

# COMMAND ----------

lista_sub_nomes[5]

# COMMAND ----------

from pyspark.sql.functions import coalesce, col, when

nome_do_fundo = "/".join(lista_sub_nomes[5].split("/")[-2:])
FNE_DESEMBOLSOS = spark_frame(nome_do_fundo)


FNE_DESEMBOLSOS = FNE_DESEMBOLSOS.withColumn(
    "SETOR",
    when(col("Setor 1").isNotNull(), col("Setor 1")).otherwise(col("SETOR"))
).drop('Setor 1')

path=f'{adl_raw}{nome_do_fundo}'
save(FNE_DESEMBOLSOS, path)

# COMMAND ----------

# MAGIC %md
# MAGIC # FNO_CARTEIRA

# COMMAND ----------

nome_do_fundo = "/".join(lista_sub_nomes[6].split("/")[-2:])
print(nome_do_fundo)
FNO_CARTEIRA = spark_frame(nome_do_fundo)

for column in FNO_CARTEIRA.columns:
  FNO_CARTEIRA = FNO_CARTEIRA.withColumnRenamed(column, __normalize_str(column))


# Exemplo de unificação de várias colunas duplicadas
FNO_CARTEIRA = (
    FNO_CARTEIRA
    # Unificar CODIGO_DE_MUNICIPIO e CODIBGE
    .withColumn(
        "CODIGO_DE_MUNICIPIO",
        coalesce(col("CODIGO_DE_MUNICIPIO"), col("CODIBGE"))
    )
    .drop("CODIBGE")
    
    # Unificar SALDO_EM_ATRASO_PORTARIA_MI_MF e SALDO_EM_ATRASOPORTARIA_MI_MF
    .withColumn(
        "SALDO_EM_ATRASO_PORTARIA_MI_MF",
        coalesce(col("SALDO_EM_ATRASO_PORTARIA_MI_MF"), col("SALDO_EM_ATRASOPORTARIA_MI_MF"))
    )
    .drop("SALDO_EM_ATRASOPORTARIA_MI_MF")

    # Unificar SALDO_DA_CARTEIRA_REGRA_DE_MERCADO e SALDO_DA_CARTEIRAREGRA_DE_MERCADO
    .withColumn(
        "SALDO_DA_CARTEIRA_REGRA_DE_MERCADO",
        coalesce(col("SALDO_DA_CARTEIRA_REGRA_DE_MERCADO"), col("SALDO_DA_CARTEIRAREGRA_DE_MERCADO"))
    )
    .drop("SALDO_DA_CARTEIRAREGRA_DE_MERCADO")

    # Unificar SALDO_EM_ATRASO_RESOLUCAO_CMN e SALDO_EM_ATRASORESOLUCAO_CMN
    .withColumn(
        "SALDO_EM_ATRASO_RESOLUCAO_CMN",
        coalesce(col("SALDO_EM_ATRASO_RESOLUCAO_CMN"), col("SALDO_EM_ATRASORESOLUCAO_CMN"))
    )
    .drop("SALDO_EM_ATRASORESOLUCAO_CMN")
)


FNO_CARTEIRA = cf.append_control_columns(FNO_CARTEIRA, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
path=f'{adl_raw}{nome_do_fundo}'
print(path)

save(FNO_CARTEIRA, path)

# COMMAND ----------

# MAGIC %md
# MAGIC # FNO_CONTRATACOES

# COMMAND ----------

import re
from pyspark.sql.functions import coalesce, col


nome_do_fundo = "/".join(lista_sub_nomes[7].split("/")[-2:])
print(nome_do_fundo)
FNO_CONTRATACOES = spark_frame(nome_do_fundo)

for column in FNO_CONTRATACOES.columns:
  FNO_CONTRATACOES = FNO_CONTRATACOES.withColumnRenamed(column, __normalize_str(column))
  

def clean_col_name(name):
    # Remove aspas duplas, ponto de interrogação, e substitui espaços por _
    name = re.sub(r'["?]', '', name)
    name = name.replace(' ', '_')
    return name

for col_name in FNO_CONTRATACOES.columns:
    new_name = clean_col_name(col_name)
    if new_name != col_name:
        FNO_CONTRATACOES = FNO_CONTRATACOES.withColumnRenamed(col_name, new_name)


from pyspark.sql.functions import col, coalesce

FNO_CONTRATACOES = (
    FNO_CONTRATACOES
    # Código do município
    .withColumn(
        "CODIGO_DE_MUNICIPIO",
        coalesce(col("CODIGO_DE_MUNICIPIO"), col("CODIGO_DO_MUNICIPIO"), col("COD_MUNICIPIO"))
    ).drop("CODIGO_DO_MUNICIPIO", "COD_MUNICIPIO")
    
    # Nome do município
    .withColumn(
        "NOME_DO_MUNICIPIO",
        coalesce(col("NOME_DO_MUNICIPIO"), col("MUNICIPIO"), col("DSC_MUNICIPIO"))
    ).drop("MUNICIPIO", "DSC_MUNICIPIO")
    
    # UF
    .withColumn(
        "UF",
        coalesce(col("UF"), col("DSC_UF"))
    ).drop("DSC_UF")
    
    # Finalidade da operação
    .withColumn(
        "FINALIDADE_DA_OPERACAO",
        coalesce(col("FINALIDADE_DA_OPERACAO"), col("DSC_FINALIDADE"), col("FINALIDADE"))
    ).drop("DSC_FINALIDADE", "FINALIDADE")
    
    # Data da contratação
    .withColumn(
        "DATA_CONTRATACAO",
        coalesce(col("DATA_CONTRATACAO"), col("ANO"), col("NUM_ANO"), col("NUM_MES_ANO"))
    ).drop("ANO", "NUM_ANO", "NUM_MES_ANO")
    
    # Valor contratado
    .withColumn(
        "VALOR_CONTRATADO",
        coalesce(col("VALOR_CONTRATADO"), col("VALOR_CONTRATADO_RS_100"), col("VLR_CONTRATO"))
    ).drop("VALOR_CONTRATADO_RS_100", "VLR_CONTRATO")
    
    # Tipologia
    .withColumn(
        "TIPOLOGIA_DO_MUNICIPIO",
        coalesce(col("TIPOLOGIA_DO_MUNICIPIO"), col("TIPOLOGIA"), col("TIPOLOGIA_1"))
    ).drop("TIPOLOGIA", "TIPOLOGIA_1")
    
    # Linha de financiamento
    .withColumn(
        "LINHA_DE_FINANCIAMENTO",
        coalesce(col("LINHA_DE_FINANCIAMENTO"), col("DSC_LINHA_FINANCIAMENTO"))
    ).drop("DSC_LINHA_FINANCIAMENTO")
    
    # Programa
    .withColumn(
        "PROGRAMA",
        coalesce(col("PROGRAMA"), col("DSC_PROGRAMA"))
    ).drop("DSC_PROGRAMA")
    
    # Setor
    .withColumn(
        "SETOR",
        coalesce(col("SETOR"), col("DSC_SETOR_1"))
    ).drop("DSC_SETOR_1")
    
    # CNAE
    .withColumn(
        "CNAE",
        coalesce(col("CNAE"), col("NUM_CNAE"))
    ).drop("NUM_CNAE")
    
    # Porte
    .withColumn(
        "PORTE",
        coalesce(col("PORTE"), col("DSC_PORTE"))
    ).drop("DSC_PORTE")
    
    # RIDES
    .withColumn(
        "RIDES",
        coalesce(col("RIDES"), col("RIDE"))
    ).drop("RIDE")
    
    # Renomear beneficiários
    .withColumnRenamed("QUANTIDADE_DE_BENEFICIARIOS_ATENDIDOS", "QTDE_DE_BENEFICIARIOS")
)

FNO_CONTRATACOES = (
    FNO_CONTRATACOES.withColumn(
        "QUANTIDADE_DE_CONTRATOS",
        coalesce(col("QUANTIDADE_CONTRATOS"), col("NUM_OPERACAO_CONTRATADA"))
    ).drop("NO_DE_OPERACOES_CONTRATADA", "NUM_OPERACAO_CONTRATADA"))

FNO_CONTRATACOES = (
    FNO_CONTRATACOES.withColumn(
        "ATIVIDADE",
        coalesce(col("ATIVIDADE"), col("DSC_ATIVIDADE"))
    ).drop("DSC_ATIVIDADE"))

FNO_CONTRATACOES = (
    FNO_CONTRATACOES.withColumn(
        "QUANTIDADE_DE_CONTRATOS",
        coalesce(col("QUANTIDADE_DE_CONTRATOS"), col("NO_DE_OPERACOES_CONTRATADAS"))
    ).drop("NO_DE_OPERACOES_CONTRATADAS"))

FNO_CONTRATACOES = (
    FNO_CONTRATACOES.withColumn(
        "QUANTIDADE_DE_CONTRATOS",
        coalesce(col("QUANTIDADE_DE_CONTRATOS"), col("QUANTIDADE_CONTRATOS"))
    ).drop("QUANTIDADE_CONTRATOS")
)

FNO_CONTRATACOES.display()

# Now assign path and save
path = f'{adl_raw}{nome_do_fundo}'
print(path)
save(FNO_CONTRATACOES, path)

# COMMAND ----------

# MAGIC %md
# MAGIC # FNO_DESEMBOLSOS

# COMMAND ----------

from pyspark.sql.functions import coalesce, col

nome_do_fundo = "/".join(lista_sub_nomes[8].split("/")[-2:])
print(nome_do_fundo)
FNO_DESEMBOLSOS = spark_frame(nome_do_fundo)

for column in FNO_DESEMBOLSOS.columns:
  FNO_DESEMBOLSOS = FNO_DESEMBOLSOS.withColumnRenamed(column, __normalize_str(column))

FNO_DESEMBOLSOS = (
    FNO_DESEMBOLSOS
    .withColumnRenamed("QUANTIDADE_DE_OPERACOES_QUE_TIVERAM_DESEMBOLSO", "QUANTIDADE_DE_CONTRATOS")
    .withColumnRenamed("QUANTIDADE_DE_BENEFICIARIOS_ATENDIDOS", "QTDE_DE_BENEFICIARIOS")
    .withColumnRenamed("VALOR_DESEMBOLSADO_ATE_O_MES_DE_REFERENCIA", "VALOR_DESEMBOLSADO")
)

path=f'{adl_raw}{nome_do_fundo}'
print(path)

save(FNO_DESEMBOLSOS, path)

# COMMAND ----------

