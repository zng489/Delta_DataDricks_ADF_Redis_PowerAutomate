# Databricks notebook source
pip install azure-storage-file-datalake

# COMMAND ----------

dbutils.widgets.text("env","dev") #tmp ou prod
dbutils.widgets.text("path","uds")
env = dbutils.widgets.get("env")
var_path = dbutils.widgets.get("path")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, var_path, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

# COMMAND ----------

# Cria o objeto adls a partir do file system do blob storage
try:  
  service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
    "https", 'cnibigdatadlsgen2'), credential='dpPvBydrRGcRUKZa5Ag/MJnLZ4Tg4jWORgw3sALbsPQgSuFkbteB19JPXxlTvDr8AATQ7b4S548alNkQj4R5Og==')
  adls = service_client.get_file_system_client("datalake")    
except Exception as e: 
  dbutils.notebook.exit(e)

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
pastas = [path_read]
pastas += get_paths(path_read)
for pasta in pastas:
  print(pasta)
  conjunto_acl += get_acl(pasta)
  conjunto_prop += get_prop(pasta)  

# COMMAND ----------

if conjunto_acl == []:
  dbutils.notebook.exit("Path vazia: " + path_read)

# COMMAND ----------

df_acl = spark.createDataFrame(conjunto_acl).withColumn("created_at",current_date())

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
                                  target.created_at = upsert.created_at   ") \
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
