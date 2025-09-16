# Databricks notebook source
file = {"namespace":"/oni", "file_folder":"/ibge/pintec/", 
        "file_subfolder":"", "file_name":"", "sheet_name":"",
        "raw_path":"/usr/oni/ibge/pintec_semestral/",
        "prm_path": "", 
        "extension":"csv","column_delimiter":",","encoding":"iso-8859-1","null_value":""}
        
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

import os
import re
import datetime
from unicodedata import normalize

from pyspark.sql.functions import count, col
import pandas as pd
import pyspark.pandas as ps
import crawler.functions as cf
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index, col, when, explode, concat

from core.string_utils import normalize_replace

# COMMAND ----------

file = notebook_params.var_file
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

file_name = file['file_name']
sheet_name = file['sheet_name']

uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

print(file_name)
print(sheet_name)

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
print(adl_uld)

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
print(adl_raw)

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
print(prm_path)

# COMMAND ----------

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

from pyspark.sql.functions import col, count, isnan, when

def drop_null_columns(df, coluna):
    df = df.filter(col(f"{coluna}").isNotNull())
    return df


# COMMAND ----------

def load_csv_to_dataframe(file_path: str):
    return (spark.read
                .option("delimiter", ",")
                .option("header", "true")
                .option("encoding", "utf-8")
                .csv(file_path))

# COMMAND ----------

paths_filtrados = []

for path in cf.list_subdirectory(dbutils, uld_path):
    try:
        # pegar o trecho depois de /pintec/
        parte = path.split("/pintec/")[1].split("/")[0]
        if parte.isdigit():
            paths_filtrados.append(path)
    except IndexError:
        continue  # ignora paths que não tenham /pintec/

# Exibir resultado
lista_pintec_cnae_po = []
for p in paths_filtrados:
  for path in cf.list_subdirectory(dbutils, p):
    lista_pintec_cnae_po.append(path)

# COMMAND ----------

lista_dos_caminhos_arquivos = []
for caminhos_dos_arquivos in lista_pintec_cnae_po:
  for path in cf.list_subdirectory(dbutils, caminhos_dos_arquivos):
    if path.endswith('.csv'):
      lista_dos_caminhos_arquivos.append(path)
      #print(path)

# COMMAND ----------

#lista_dos_caminhos_arquivos

# COMMAND ----------

dataframes = [] 
for caminho in lista_dos_caminhos_arquivos:
  try:
    df = load_csv_to_dataframe(f"{var_adls_uri}/{caminho}")
    dataframes.append(df)

  except Exception as e:
    print(f"Erro ao processar arquivo {caminho}: {e}")

if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.display()

# COMMAND ----------

tabelas = final_df.select("NR_NOME_BASE").distinct().collect()
lista_strings = [row["NR_NOME_BASE"] for row in tabelas]
lista_tabelas = [item for item in lista_strings if item is not None]
lista_tabelas

'''
['proc_neg_gr_obst_cnae',
'grau_nov_prod_cnae',
'prod_proc_disp_pd_cnae',
'prod_proc_apoio_pub_cnae',
'inov_prod_proces_po',
'n_proc_neg_gr_obst_po',
'inov_prod_proces_cnae',
'prod_proc_disp_pd_po',
'prod_proc_evolu_disp_cnae',
'proc_neg_po',
'n_proc_neg_gr_obst_cnae',
'ef_pand_po',
'prod_proc_n_apoio_pub_po',
'prod_proc_apoio_pub_instr_cnae',
'rel_sust_po',
'proc_neg_gr_obst_po',
'prod_proc_apoio_pub_instr_po',
'prod_proc_evolu_disp_po',
'prod_proc_rel_coop_cnae',
'ef_pand_cnae',
'prod_proc_apoio_pub_po',
'proc_neg_cnae',
'prod_proc_rel_coop_po',
'prod_proc_n_apoio_pub_cnae',
'grau_nov_prod_po',
'rel_sust_cnae']
'''

# COMMAND ----------

from pyspark.sql.types import StringType, DoubleType, IntegerType

col_cast_map = {
    "NR_ANO": IntegerType(),
    "NM_ATIVIDADES_DA_INDUSTRIA": StringType(),
    "NR_EMPRESAS_TOTAL": DoubleType(),
    "NR_REL_SUSTENT_TOTAL": DoubleType(),
    "NR_IMPLEMENT_INOV_PROD_PROC_TOTAL": DoubleType(),
    "NR_NOME_BASE": StringType(),
    "NR_ATIVAS_IMPLEMENT_INOV": DoubleType(),
    "NR_INOV_PROD_PROC_NEG_TOTAL": DoubleType(),
    "NR_INOV_PROD": DoubleType(),
    "NR_INOV_PROC_NEG": DoubleType(),
    "NR_APENAS_PJT_INCOMP_ABAND_TOTAL": DoubleType(),
    "NR_APENAS_PJT_INCOMP": DoubleType(),
    "NR_APENAS_PJT_ABAND": DoubleType(),
    "NR_IMPLEMENT_INOV_PROD_TOTAL": DoubleType(),
    "NR_GRAU_NOV_NOVO_EMP": DoubleType(),
    "NR_GRAU_NOV_NOVO_MERC_NAC": DoubleType(),
    "NR_GRAU_NOV_NOVO_MERC_MUND": DoubleType(),
    "NR_RESULT_INOV_MELHOR_ESP": DoubleType(),
    "NR_RESULT_INOV_ACORDO_ESP": DoubleType(),
    "NR_RESULT_INOV_ABAIXO_ESP": DoubleType(),
    "NR_RESULT_INOV_CEDO_AVAL": DoubleType(),
    "NR_IMPLEMENT_INOV_PROC_NEG_TOTAL": DoubleType(),
    "NR_FUNC_EMP_PROD_BENS_SERV": DoubleType(),
    "NR_FUNC_EMP_LOGIST": DoubleType(),
    "NR_FUNC_EMP_PROCES_INFO": DoubleType(),
    "NR_FUNC_EMP_CONTAB": DoubleType(),
    "NR_FUNC_EMP_PRAT_GEST": DoubleType(),
    "NR_FUNC_EMP_GEST_RH": DoubleType(),
    "NR_FUNC_EMP_MKT": DoubleType(),
    "NR_IMPLEMENT_INOV_PROD_PROC_NEG_TOTAL": DoubleType(),
    "NR_DIFICUL_OBST_TOTAL": DoubleType(),
    # Continua o mesmo padrão para as demais colunas NR_
}

# COMMAND ----------

def save_data(lista_tabelas):
  dataframes = []
  for caminho in lista_dos_caminhos_arquivos:
    df = load_csv_to_dataframe(f"{var_adls_uri}/{caminho}")
      
    if df.select("NR_NOME_BASE").first()[0] == lista_tabelas:
      print(df.select("NR_NOME_BASE").first()[0], lista_tabelas)
      dataframes.append(df)

  # Junta todos os dataframes encontrados
  if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
      final_df = final_df.unionByName(df, allowMissingColumns=True)
      
    df_ = final_df

    for column in df_.columns:
      df_ = df_.withColumnRenamed(column, __normalize_str(column))
      valor_str = df_.select("NR_NOME_BASE").first()[0]

    df_ = cf.append_control_columns(
        df_, 
        dh_insercao_raw=adf["adf_trigger_time"].split(".")[0]
    )
    dh_insercao_raw = datetime.datetime.now()
    df_ = df_.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

    valor_str = df_.select("NR_NOME_BASE").first()[0]
    valor_tabela = "_".join(valor_str.split("_")[:-1])
    print(valor_tabela)  # Saída: proc_neg_gr_obst

    valor_str = df.select("NR_NOME_BASE").first()[0]
    ultimo_valor = valor_str.split("_")[-1]
    print(ultimo_valor)  # Saída: cnae
    
    fechamento = "camada raw - nova serie historica pintec 2021 2022 2023"
    df_.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(f'{adl_raw}{ultimo_valor}/{valor_tabela}')

    print("==" * 20)  

  else:
      print("Nenhum DataFrame correspondente encontrado.")

  return 

# COMMAND ----------



# COMMAND ----------

for valor in lista_tabelas:
  save_data(valor)

# COMMAND ----------

#df = cf.append_control_columns(final_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
#dh_insercao_raw = datetime.datetime.now()
#df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

#fechamento = "camada trs - nova serie historica pintec 2021 2022 2023"




#fechamento="nova serie historica pintec 2021 2022 2023"
df.write.option("userMetadata",  fechamento) \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "auto") \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("TIPO","NR_NOME_BASE","NR_ANO") \
        .save(path=adl_raw)

#.option("delta.logRetentionDuration", "365 days") \        
#df.write.partitionBy('NR_ANO').mode('overwrite', ).parquet(path=adl_raw, mode='overwrite')
