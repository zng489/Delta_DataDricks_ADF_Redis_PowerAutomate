# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from unicodedata import normalize

import datetime
import crawler.functions as cf
import json
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import re

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

dbutils.widgets.text("file", "")
dbutils.widgets.text("dls", "")
dbutils.widgets.text("adf", "")

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']

# COMMAND ----------

var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
var_source

# COMMAND ----------

var_sink = "{adl_path}{raw}/usr/{namespace}/{file_folder}/".format(adl_path=var_adls_uri, raw=raw, 
                                                                   namespace=var_file['namespace'], file_folder=var_file['file_folder'].split('/')[0])
var_sink

# COMMAND ----------

var_year = var_file['file_folder'].split('/')[-1]
var_year

# COMMAND ----------

var_source_txt_files = '{source}/*.txt'.format(source=var_adls_uri + var_source)
var_source_txt_files

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, var_file['prm_path'], headers=headers, sheet_names=[var_year])

# COMMAND ----------

if not cf.directory_exists(dbutils, var_source):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % var_source)

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

def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)

# COMMAND ----------

#extração arquivos .txt da uld
df = spark.read.csv(path=var_source_txt_files, sep=';', encoding='iso-8859-1', header=True)

#normalização nome de colunas
for column in df.columns:
  df = df.withColumnRenamed(column, __normalize_str(column))

#verificação com prm
cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=var_year)
df = df.select(*__select(parse_ba_doc=var_prm_dict, year=var_year))

#coluna ano para particionamento no salvamento
df = df.withColumn('ANO', f.lit(var_year).cast('Int'))

#inserção de coluna de ingestão na camada raw
df = cf.append_control_columns(df, dh_insercao_raw=var_adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

df_adl_files = cf.list_adl_files(spark, dbutils, var_source)
df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

# #escrita de dados na raw
df.repartition(40).write.partitionBy('ANO').parquet(path=var_sink, mode='overwrite')
