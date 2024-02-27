# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/uld/me/rais_estabelecimento/2021/'.format(uri=var_adls_uri)
RAIS_2021 = spark.read.format("csv").option("header","true").option('sep',';').option('encoding','ISO-8859-1').load(path)
RAIS_2021.display()


# COMMAND ----------

