# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os, uuid, sys

# COMMAND ----------

dbutils.widgets.text("env","dev")
dbutils.widgets.text("path","uds")

# COMMAND ----------

env = dbutils.widgets.get("env")
var_path = dbutils.widgets.get("path")

# COMMAND ----------

default_dir = '/'.join(['','tmp','dev'])
if env == 'prod':
  default_dir = ""  

# COMMAND ----------

path = "{adl_path}{default_dir}/{var_path}".format(adl_path=var_adls_uri,default_dir=default_dir,var_path=var_path)
path_size = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir)
path_size += '/' + 'gov' + '/' + 'tables' + '/' + 'size'

# COMMAND ----------

env, var_path, path, path_size

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# COMMAND ----------

# deixa somente o path sem o caminho do data lake
def normaliza_path(path):
  i = (3 if (path[0:6] == 'abfss:') else 0)
  return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')

# COMMAND ----------

# captura o tamanho dos arquivos a partir do path informado, varrendo todas as
# subpastas e retornando uma lista com o path do arquivo e o tamanho em bytes
def get_size (path): 
  size = 0
  conjunto = []
  try: lista = dbutils.fs.ls(path)
  except: dbutils.notebook.exit("Path não encontrado: " + path)
  for item in lista:
    if item.name[-1:] == '/':         # se for pasta
      print(item)
      conjunto += get_size(item.path)
    else:                             # se for arquivo
      conjunto.append(dict([('path',normaliza_path(item.path)),('size',item.size)]))
  return conjunto

# COMMAND ----------

conjunto_size = get_size(path)

# COMMAND ----------

if conjunto_size == []:
  dbutils.notebook.exit("Path vazia: " + path)

# COMMAND ----------

df_size = spark.createDataFrame(conjunto_size).withColumn("created_at",current_date())

# COMMAND ----------

df_size.show()

# COMMAND ----------

df_delta = DeltaTable.forPath(spark, path_size)

# COMMAND ----------

size_set = {
  "path"       : "upsert.path",
  "size"       : "upsert.size",
  "created_at" : "upsert.created_at" 
}

# COMMAND ----------

df_delta.alias("target") \
  .merge(df_size.alias("upsert"), "target.path       = upsert.path       and  \
                                   target.created_at = upsert.created_at    ") \
  .whenNotMatchedInsert(values=size_set) \
  .execute()

# COMMAND ----------


