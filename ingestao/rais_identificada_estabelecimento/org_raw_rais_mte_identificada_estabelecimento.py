# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

file = {'namespace':'/oni', 'file_folder':'/mte/rais/identificada',          'file_subfolder':'/rais_estabelecimento/',          'raw_path':'/usr/oni/mte/rais/identificada/rais_estabelecimento/',         'prm_path': '/tmp/dev/prm/usr/oni/mte/rais_identificada_estabelecimento/KC2332_ME_RAIS_ESTABELECIMENTO_mapeamento_raw.xlsx',          'extension':'txt','column_delimiter':'','encoding':'iso-8859-1','null_value':''}
        
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }



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

# Lendo arquivos antes de 2011

from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, LongType

# Mapeamento: Campo Origem -> Campo Destino
col_rename_map = {
    "CEI_VINC": "ID_CEI_VINCULADO",
    "CEP": "ID_CEPAO_ESTAB",
    "CLAS_CNAE_95": "CD_CNAE10_CLASSE",
    "IDENTIFICAD": "ID_CNPJ_CEI",
    "RADIC_CNPJ": "ID_CNPJ_RAIZ",
    "DT_ABERT_COM": "DT_ABERTURA",
    "DT_BAIXA_COM": "DT_BAIXA",
    "DT_ENCER_OR": "DT_ENCERRAMENTO",
    "EMAIL": "NM_EMAIL",
    "IND_CEI_VINC": "FL_IND_CEI_VINCULADO",
    "IND_PAT": "FL_IND_ESTAB_PARTICIPA_PAT",
    "IND_RAIS_NEG": "FL_IND_RAIS_NEGAT",
    "IND_SIMPLES": "FL_IND_SIMPLES",
    "MUNICIPIO": "CD_MUNICIPIO",
    "NAT_JURIDICA": "CD_NATUREZA_JURIDICA",
    "ENDERECO": "NM_LOGRADOURO",
    "TELEF_CONT": "NR_TELEFONE_EMPRE",
    "ESTOQUE": "QT_VINC_ATIV",
    "EST_CLT_OUT": "QT_VINC_CLT",
    "ESTOQUE_ESTA": "QT_VINC_ESTAT",
    "RAZAO_SOCIAL": "ID_RAZAO_SOCIAL",
    "TAMESTAB": "CD_TAMANHO_ESTABELECIMENTO",
    "TIPO_ESTBL": "CD_TIPO_ESTAB_ID",
    "SUBS_IBGE": "CD_IBGE_SUBSETOR",
    "IND_ATIV_ANO": "FL_IND_ATIV_ANO",
    "CLAS_CNAE_20": "CD_CNAE20_CLASSE",
    "SB_CLAS_20": "CD_CNAE20_SUBCLASSE"
}


col_dtype_map = {
    "ID_CEI_VINCULADO": LongType(),
    "ID_CEPAO_ESTAB": DoubleType(),
    "CD_CNAE10_CLASSE": IntegerType(),
    "ID_CNPJ_CEI": StringType(),
    "ID_CNPJ_RAIZ": StringType(),
    "DT_ABERTURA": IntegerType(),
    "DT_BAIXA": IntegerType(),
    "DT_ENCERRAMENTO": IntegerType(),
    "NM_EMAIL": StringType(),
    "FL_IND_CEI_VINCULADO": IntegerType(),
    "FL_IND_ESTAB_PARTICIPA_PAT": IntegerType(),
    "FL_IND_RAIS_NEGAT": IntegerType(),
    "FL_IND_SIMPLES": IntegerType(),
    "CD_MUNICIPIO": IntegerType(),
    "CD_NATUREZA_JURIDICA": IntegerType(),
    "NM_LOGRADOURO": StringType(),
    "NR_LOGRADOURO": DoubleType(),
    "NM_BAIRRO": StringType(),
    "DISTRITOS_SP": IntegerType(),
    "BAIRROS_SP": IntegerType(),
    "BAIRROS_FORTALEZA": IntegerType(),
    "BAIRROS_RJ": IntegerType(),
    "NR_TELEFONE_EMPRE": LongType(),
    "QT_VINC_ATIV": IntegerType(),
    "QT_VINC_CLT": IntegerType(),
    "QT_VINC_ESTAT": IntegerType(),
    "ID_RAZAO_SOCIAL": StringType(),
    "CD_TAMANHO_ESTABELECIMENTO": IntegerType(),
    "CD_TIPO_ESTAB_ID": IntegerType(),
    "TIPO_ESTAB19": IntegerType(),
    "TIPO_ESTAB20": StringType(),
    "CD_IBGE_SUBSETOR": IntegerType(),
    "FL_IND_ATIV_ANO": IntegerType(),
    "CD_CNAE20_CLASSE": IntegerType(),
    "CD_CNAE20_SUBCLASSE": IntegerType(),
    "CD_UF": IntegerType(),
    "REGIOES_ADM_DF": StringType(),
    "ANO": StringType()
}


# COMMAND ----------

import re
import csv
from io import StringIO
from pyspark.sql.types import *
from pyspark.sql.functions import *

dataframes_a = []

def detectar_delimitador(dbutils, path, tamanho_amostra=4096):
    """Detecta delimitador usando csv.Sniffer"""
    try:
        amostra = dbutils.fs.head(path, tamanho_amostra)
        return csv.Sniffer().sniff(amostra).delimiter
    except Exception:
        return "|"  # fallback padrão

for path in cf.list_subdirectory(dbutils, uld_path):
    for file_path in cf.list_subdirectory(dbutils, path):
        for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
            if arquivos_txt.endswith('.txt'):
                numbers_list = re.findall(r'\d+', path)
                numbers = int(''.join(numbers_list))
                if numbers < 2011:
                    print(f"Ano detectado: {numbers}")
                    
                    full_path = f"{var_adls_uri}/{file_path}"

                    # Detecta delimitador
                    delimitador = detectar_delimitador(dbutils, full_path)
                    print(f"Delimitador detectado: '{delimitador}' para {arquivos_txt}")

                    # Lê com o delimitador detectado
                    df_a = (
                        spark.read
                            .option("delimiter", delimitador)
                            .option("header", "true")
                            .option("encoding", "ISO-8859-1")
                            .csv(full_path)
                    )

                    for column in df_a.columns:
                      df_a = df_a.withColumnRenamed(column, __normalize_str(column))

                    df_a = df_a.withColumn("ANO", lit(f"{numbers}"))

                    # Suponha que seu DataFrame PySpark se chame "df"
                    for origem, destino in col_rename_map.items():
                        df_a = df_a.withColumnRenamed(origem, destino)


                    for col_name, dtype in col_dtype_map.items():
                        if col_name in df_a.columns:
                            df_a = df_a.withColumn(col_name, col(col_name).cast(dtype))
                    dataframes_a.append(df_a)
                else:
                    print(f'Não há arquivi do tipo txt no caminho {path} ou o ano é menor que 2011') 

                    #df.display()

#if dataframes_a:
#    final_df_a = dataframes_a[0]
#    for df_a in dataframes_a[1:]:
#        final_df_a = final_df_a.unionByName(df_a, allowMissingColumns=True)


# COMMAND ----------

# Lendo arquivos depois de 2011

from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BinaryType, BooleanType, ArrayType, MapType

dataframes = []
for path in cf.list_subdirectory(dbutils, uld_path):
  for file_path in cf.list_subdirectory(dbutils, path):
    for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
      if arquivos_txt.endswith('.txt'):
        print(f'  Explorando subdiretório: {arquivos_txt}')
        numbers_list = re.findall(r'\d+', path)
        numbers = int(''.join(numbers_list))
        if numbers >= 2011:
          df = (spark.read
                        .option("delimiter", ";")
                        .option("header", "true")
                        .option("encoding", "ISO-8859-1")
                        .csv(f"{var_adls_uri}/{file_path}")
                        )
          headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}

          print("It's set to '2018' as the default for all years.")
          var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path, headers=headers, sheet_names=['2018'])

          for column in df.columns:
            df = df.withColumnRenamed(column, __normalize_str(column))

          def __select(parse_ba_doc, sheet_name):
            for org, dst, _type in parse_ba_doc[sheet_name]:
              if org == 'N/A' and dst not in df.columns:
                yield f.lit(None).cast(_type).alias(dst)
              else:
                _col = f.col(org)
                if _type.lower() == 'double':
                  _col = f.regexp_replace(org, ',', '.')
                yield _col.cast(_type).alias(dst)    
                
          print(numbers)
          df = df.select(*__select(var_prm_dict, '2018'))
          df = df.withColumn("ANO", lit(f"{numbers}"))

          dataframes.append(df)
        else:
          print(f'Não há arquivi do tipo txt no caminho {path} ou o ano não é menor que 2011') 

#if dataframes:
#    final_df = dataframes[0]
#    for df in dataframes[1:]:
#        final_df = final_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# Juntar as duas listas de dataframes
all_dfs = dataframes + dataframes_a

# COMMAND ----------

if all_dfs:
    df_union = all_dfs[0]
    for df_ in all_dfs[1:]:
        df_union = df_union.unionByName(df_, allowMissingColumns=True)

# COMMAND ----------

df = cf.append_control_columns(df_union, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

# COMMAND ----------

for path in cf.list_subdirectory(dbutils, raw_usr_path):
  #print(f"{var_adls_uri}/{path}")
  for item in dbutils.fs.ls(f"{var_adls_uri}/{path}"):
    if item.name == "_SUCCESS" and not item.isDir():
        print(f"Deletando arquivo: {item.path}")
        dbutils.fs.rm(item.path, recurse=False)

# COMMAND ----------

df.write.partitionBy('ANO').parquet(path=adl_raw, mode='overwrite')