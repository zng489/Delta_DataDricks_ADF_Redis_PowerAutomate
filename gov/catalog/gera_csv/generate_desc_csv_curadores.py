# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,col

# COMMAND ----------

dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

default_dir_busca = ""
if env == 'dev':
  default_dir = '/'.join(['','tmp','dev'])
elif env == 'prod':
  default_dir = ""  

# COMMAND ----------

# captura os data stewards
path_gov = "{var_adls_uri}{default_dir}/gov/tables/".format(var_adls_uri=var_adls_uri,default_dir=default_dir)
data_steward = spark.read.format("delta").load(path_gov + "data_steward")
data_steward = data_steward.withColumn('name_data_steward', regexp_replace(col('name_data_steward'), " ", "_"))
data_steward.show()

# COMMAND ----------

path_gov

# COMMAND ----------

for var_data_steward in data_steward.rdd.collect():
  for var_tabela in ['field','table']:
    #executa atualização dos dbfs
    tabela = var_tabela
    steward = var_data_steward['cod_data_steward']
    steward_name = var_data_steward['name_data_steward']
    params = {"env": env,"gov_table": tabela, "gov_steward": steward, "gov_steward_name": steward_name}
    try:
      dbutils.notebook.run("generate_desc_csv_por_curador".format(env=env),0,params)
    except:
      print("atualização falhou")    
