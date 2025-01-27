# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


DF = spark.read.format("parquet").option("header","true").option('sep', ',').option('encoding', 'ISO-8859-1').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/me/rais_vinculo/')

DF.createOrReplaceTempView('RAIS_TABLE')
#display(DF.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM RAIS_TABLE 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create Table MyTempTable_1 
# MAGIC (
# MAGIC   SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)
# MAGIC )

# COMMAND ----------

df_2008_2018 = spark.sql(" SELECT * FROM (SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)) WHERE FL_VINCULO_ATIVO_3112='1' ")

# COMMAND ----------


df_2008_2018 = spark.sql("SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)")

df_2008_2018.createOrReplaceTempView('df_2008_2018')

# COMMAND ----------

df_2008_2018 = spark.sql("SELECT * FROM df_2008_2018 WHERE FL_VINCULO_ATIVO_3112='1'")

# COMMAND ----------

