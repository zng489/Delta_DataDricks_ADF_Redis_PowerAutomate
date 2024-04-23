# Databricks notebook source
pip install azure-storage-file-datalake

# COMMAND ----------

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
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

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
path_acl = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir)
path_prop = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir)
path_acl += '/' + 'gov' + '/' + 'tables' + '/' + 'acl'
path_prop += '/' + 'gov' + '/' + 'tables' + '/' + 'prop'

# COMMAND ----------

env, var_path, path, path_acl, path_prop

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# COMMAND ----------

# deixa somente o path sem o caminho do data lake
def normaliza_path(path):
  i = (3 if (path[0:6] == 'abfss:') else 0)
  return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')

# COMMAND ----------

# Cria o objeto adls a partir do file system do blob storage
try:  
  service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
    "https", 'cnibigdatadlsgen2'), credential='dpPvBydrRGcRUKZa5Ag/MJnLZ4Tg4jWORgw3sALbsPQgSuFkbteB19JPXxlTvDr8AATQ7b4S548alNkQj4R5Og==')
  adls = service_client.get_file_system_client("datalake")    
except Exception as e: 
  dbutils.notebook.exit(e)

# COMMAND ----------

# captura as pastas e subpastas do datalake a partir da variável path informada
def get_paths (path):
  conjunto = []
  try: lista = dbutils.fs.ls(path)
  except: dbutils.notebook.exit("Path não encontrado: " + path)
  for item in lista:
    if item.name[-1:] == '/':
      if item.name.find("=") == -1:
        conjunto.append(item.path)
        conjunto += get_paths(item.path)
    else:
      continue
  return conjunto

# COMMAND ----------

# varre todos os diretórios de forma recorrente a partir do path informado
def get_acl (path):
  conjunto = []
  path_normalizada = normaliza_path(path)
  pasta = adls.get_directory_client(path_normalizada)
  acl = pasta.get_access_control()['acl'].split(',')
  for registro in acl:
    reg = registro.split(':')
    if reg[0] in ['user', 'group'] and reg[1] != '':
      conjunto.append(dict([('path',path_normalizada),
                            ('id', reg[1]),
                            ('tipo', reg[0]),
                            ('ler', 0 if reg[2][0] == '-' else 1),
                            ('gravar', 0 if reg[2][1] == '-' else 1),
                            ('executar', 0 if reg[2][2] == '-' else 1)
                           ]))
  return conjunto

# COMMAND ----------

# varre todos os diretórios de forma recorrente a partir do path informado
def get_prop (path):
  conjunto = []
  path_normalizada = normaliza_path(path)
  pasta = adls.get_directory_client(path_normalizada)
  propriedades = pasta.get_directory_properties()
  conjunto.append(dict([('path',path_normalizada),
                        ('path_creation_time' ,propriedades.creation_time),
                        ('path_last_modified' ,propriedades.last_modified),
                        ('path_etag', propriedades.etag),
                        ('path_deleted', propriedades.deleted)                      
                       ]))
  return conjunto

# COMMAND ----------

# pega acl e propriedades de todas as pastas listadas
conjunto_acl = []
conjunto_prop = []
pastas = get_paths(path)
for pasta in pastas:
  print(pasta)
  conjunto_acl += get_acl(pasta)
  conjunto_prop += get_prop(pasta)

# COMMAND ----------

df_acl = spark.createDataFrame(conjunto_acl).withColumn("created_at",current_date())

# COMMAND ----------

df_acl.show()

# COMMAND ----------

df_prop = spark.createDataFrame(conjunto_prop) \
  .withColumn("created_at",current_date()) \
  .withColumn("updated_at",current_date())

# COMMAND ----------

df_delta = DeltaTable.forPath(spark, path_acl)

# COMMAND ----------

df_delta2 = DeltaTable.forPath(spark, path_prop)

# COMMAND ----------

acl_set = {
  "path"       : "upsert.path",
  "id"         : "upsert.id",
  "tipo"       : "upsert.tipo",
  "ler"        : "upsert.ler",
  "gravar"     : "upsert.gravar",
  "executar"   : "upsert.executar",
  "created_at" : "upsert.created_at" 
}

# COMMAND ----------

df_delta.alias("target") \
  .merge(df_acl.alias("upsert"), "target.path       = upsert.path       and  \
                                  target.created_at = upsert.created_at    ") \
  .whenNotMatchedInsert(values=acl_set) \
  .execute()

# COMMAND ----------

prop_set = {
  "path"              : "upsert.path",
  "path_creation_time": "upsert.path_creation_time",
  "path_last_modified": "upsert.path_last_modified",
  "path_etag"         : "upsert.path_etag",
  "path_deleted"      : "upsert.path_deleted",
  "created_at"        : "upsert.created_at",
  "updated_at"        : "upsert.updated_at" 
}

# COMMAND ----------

df_delta2.alias("target") \
  .merge(df_prop.alias("upsert"), "target.path       = upsert.path ") \
  .whenNotMatchedInsert(values=prop_set) \
  .execute()

# COMMAND ----------


