# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").option('mergeschema', 'false').load(path)

df = df.select('CD_CNAE20_SUBCLASSE_SECUNDARIA', 'NCM', 'DESCRICAO_NCM_2012', 'CD_ISIC40', 'DS_ISIC40')

# COMMAND ----------

# Add a new column with the length of the first name
df = df.withColumn("CD_CNAE20_SUBCLASSE_SECUNDARIALength", length(df["CD_CNAE20_SUBCLASSE_SECUNDARIA"]))
df = df.withColumn("NCMLength", length(df["NCM"]))
df = df.withColumn("DESCRICAO_NCM_2012Length", length(df["DESCRICAO_NCM_2012"]))
df = df.withColumn("CD_ISIC40Length", length(df["CD_ISIC40"]))
df = df.withColumn("DS_ISIC40Length", length(df["DS_ISIC40"]))

df.display()

# COMMAND ----------



# COMMAND ----------

CD_CNAE20_SUBCLASSE_SECUNDARIA = df.filter(df.CD_CNAE20_SUBCLASSE_SECUNDARIALength > 4000)
CD_CNAE20_SUBCLASSE_SECUNDARIA.display()

# COMMAND ----------

NCM = df.filter(df.NCMLength > 4000).select('NCM','NCMLength')
NCM.display()

# COMMAND ----------


DESCRICAO_NCM_2012 = df.filter(df.DESCRICAO_NCM_2012Length > 4000).select('DESCRICAO_NCM_2012','DESCRICAO_NCM_2012Length')

DESCRICAO_NCM_2012.display()

# COMMAND ----------

CD_ISIC40 = df.filter(col("CD_ISIC40Length") > 4000 )

CD_ISIC40.display()

# COMMAND ----------

DS_ISIC40 = df.filter(col("DS_ISIC40Length") > 4000 )
DS_ISIC40.display()

# COMMAND ----------

