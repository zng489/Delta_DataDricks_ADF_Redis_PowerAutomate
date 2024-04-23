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

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

default_dir = '/'.join(['','tmp','dev'])
if env == 'prod':
  default_dir = ""  

# COMMAND ----------

path = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir)
path += '/' + 'gov' + '/' + 'tables' + '/'

# COMMAND ----------

env, path

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# COMMAND ----------

#otimiza tabela size
spark.sql(f"OPTIMIZE delta.`{path + 'size'}`")

# COMMAND ----------

#otimiza tabela acl
spark.sql(f"OPTIMIZE delta.`{path + 'acl'}`")

# COMMAND ----------

#otimiza tabela prop
spark.sql(f"OPTIMIZE delta.`{path + 'prop'}`")

# COMMAND ----------


