# Databricks notebook source
dbutils.widgets.text("env","dev") #tmp ou prod
env = dbutils.widgets.get("env")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

#otimiza tabela size
spark.sql(f"OPTIMIZE delta.`{path_tabela}`")

# COMMAND ----------

#otimiza tabela size
spark.sql(f"OPTIMIZE delta.`{path_esquema}`")

# COMMAND ----------

#otimiza tabela size
spark.sql(f"OPTIMIZE delta.`{path_campo}`")

# COMMAND ----------

#otimiza tabela size
spark.sql(f"OPTIMIZE delta.`{path_size}`")

# COMMAND ----------

#otimiza tabela acl
spark.sql(f"OPTIMIZE delta.`{path_acl}`")

# COMMAND ----------

#otimiza tabela prop
spark.sql(f"OPTIMIZE delta.`{path_prop}`")
