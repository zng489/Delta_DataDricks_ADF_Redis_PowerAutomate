# Databricks notebook source
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

conjunto_size = get_size(path_read)

# COMMAND ----------

if conjunto_size == []:
  dbutils.notebook.exit("Path vazia: " + path_read)

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
