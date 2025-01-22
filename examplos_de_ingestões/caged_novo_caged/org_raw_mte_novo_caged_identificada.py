# Databricks notebook source
# Databricks notebook source
file = {"namespace":"/oni", "file_folder":"/mte/novo_caged/", 
        "file_subfolder":"/identificada/", 
        "raw_path":"/usr/oni/mte/novo_caged/identificada/",
        "prm_path": "/tmp/dev/prm/usr/oni/mte/novo_caged_identificada/FIEC_me_novo_caged_exc_for_mov_mapeamento_unificado_raw.xlsx",
        #"prm_file_name":["NOVO_CAGED_EXC", "NOVO_CAGED_FOR", "NOVO_CAGED_MOV"], 
        "extension":"txt","column_delimiter":"","encoding":"utf-8","null_value":""}
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

adl_raw

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
  for nivel_1 in cf.list_subdirectory(dbutils, path):
    for nivel_2 in cf.list_subdirectory(dbutils, nivel_1):
      #print(nivel_2)
      for arquivos_txt in cf.list_subdirectory(dbutils, nivel_2):
        #print(arquivos_txt)
        if arquivos_txt.endswith('.txt'):
          #print(f'  Explorando subdiretório: {arquivos_txt}')
          #numbers_list = re.findall(r'\d+', path)
          match = re.search(r'/(\d{4})/', nivel_2)
          #numbers = ''.join(numbers_list)
          #if match:
          numbers = match.group(1)  # Captura o número

          df = (spark.read
                        .option("delimiter", ";")
                        .option("header", "true")
                        .option("encoding", "utf-8")
                        .csv(f"{var_adls_uri}/{nivel_2}")
                        )
          """
          headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
          var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path, headers=headers, sheet_names=numbers)
          """
          for column in df.columns:
            df = df.withColumnRenamed(column, __normalize_str(column))
          """
          def __select(parse_ba_doc, year):
            for org, dst, _type in parse_ba_doc[year]:
              if org == 'N/A' and dst not in df.columns:
                yield f.lit(None).cast(_type).alias(dst)
              else:
                _col = f.col(org)
                if _type.lower() == 'double':
                  _col = f.regexp_replace(org, ',', '.')
                yield _col.cast(_type).alias(dst)    
          print(numbers)
          df = df.select(*__select(var_prm_dict, numbers))
          """
          df = df.withColumn("ANO", lit(f"{numbers}"))

          dataframes.append(df)
        else:
          print(f'There is no file txt in {arquivos_txt}') 

# COMMAND ----------

if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

#final_df.display()

# COMMAND ----------

# d = final_df.alias("d")

# COMMAND ----------

# NOVO_CAGED_EXC = NOVO_CAGED_EXC_FOR_MOV
# Problemas e falta de lógica na função parse_ba_doc Logo foi obrigado a trocar os valores

sheet_name = 'NOVO_CAGED_EXC'
columns_name = ['HORASCONTRATUAIS','SALARIO']
wrong_name_of_columns = ['INDICADORDEFORADOPRAZOCODIGO','ORIGEMDAINFORMACAOCODIGO','INDICADORDEEXCLUSAOCODIGO']

name_of_columns = df.columns

for name in wrong_name_of_columns:
  if name in name_of_columns:
    final_df = final_df.withColumnRenamed(name, name[:-6])

cf.check_ba_doc(final_df, parse_ba=var_prm_dict, sheet=sheet_name)

for col in columns_name:
  final_df = final_df.withColumn(col, f.translate(f.col(col), ',' ,'.'))

final_df = final_df.select(*__transform_columns())
final_df = final_df.withColumn("SALARIO", f.round(final_df["SALARIO"], 2))

#df = df.withColumn('ANO_MES', f.lit(year_month).cast('string'))
# A estruturação do script faz perder a coluna de ANOS

final_df = final_df.withColumn("ANO", F.substring("COMPETENCIAMOV", 1, 4).cast("string"))

"""
final_df = final_df.withColumn("ANO_MES", F.concat(F.substring("COMPETENCIAMOV", 1, 4), 
        F.lit("_"), 
        F.substring("COMPETENCIAMOV", 5, 2)
    ).cast("string"))
"""



# COMMAND ----------

df = cf.append_control_columns(final_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

# COMMAND ----------

"""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import re

# Função para transformar a string de acordo com o formato genérico
def gerar_valor(nm_arq_in):
    # Usando expressão regular para capturar qualquer prefixo seguido de ano e mês
    match = re.match(r'(\w+)(\d{4})(\d{2})ID.txt', nm_arq_in)
    if match:
        prefixo = match.group(1)  # Prefixo (qualquer sequência de letras ou palavras)
        ano = match.group(2)       # Ano
        mes = match.group(3)       # Mês
        # Retornar a string formatada
        return f"{prefixo}_{ano}_{mes}"
    return None  # Retorna None se o formato não for encontrado


# Testando a função com diferentes valores
nm_arq_in1 = "CAGEDMOV202001ID.txt"
nm_arq_in2 = "OUTROPREFIXO202005ID.txt"
nm_arq_in3 = "EXEMPLO202312ID.txt"

# Testando para diferentes entradas
resultado1 = transformar_string(nm_arq_in1)
resultado2 = transformar_string(nm_arq_in2)
resultado3 = transformar_string(nm_arq_in3)

# Exibindo os resultados
print(resultado1)  # Saída: CAGEDMOV_2020_01
print(resultado2)  # Saída: OUTROPREFIXO_2020_05
print(resultado3)  # Saída: EXEMPLO_2023_12


# Registrar a função UDF (User Defined Function) com PySpark
gerar_valor_udf = F.udf(gerar_valor, StringType())

# Supondo que 'df' seja o seu DataFrame original
df = df.withColumn('CAGED_CATEGORIA', gerar_valor_udf(df['nm_arq_in']))
"""


# COMMAND ----------

# df.display()

# COMMAND ----------

df.write.partitionBy('ANO','COMPETENCIAMOV').mode('overwrite', ).parquet(path=adl_raw, mode='overwrite')

# COMMAND ----------

