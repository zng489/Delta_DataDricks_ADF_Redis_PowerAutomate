# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/raw/crw/rfb/cno/cno_totais/'.format(uri=var_adls_uri)
cadastro_cbo = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
cadastro_cbo.display()

# COMMAND ----------

import requests

https://cnicombr-my.sharepoint.com/personal/inteligencia_cni_com_br/_layouts/15/AccessDenied.aspx?Source=https%3A%2F%2Fcnicombr%2Dmy%2Esharepoint%2Ecom%2Fpersonal%2Finteligencia%5Fcni%5Fcom%5Fbr&correlation=aabb91a0%2Dc060%2D3000%2D31f8%2D4f4a8fc1797c

# COMMAND ----------

# MAGIC %md 
# MAGIC # CBO
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/cadastro_cbo/'.format(uri=var_adls_uri)
cadastro_cbo = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
cadastro_cbo.display()

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/cadastro_cbo_familia/'.format(uri=var_adls_uri)
cadastro_cbo_familia = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
cadastro_cbo_familia.display()

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/cadastro_cbo_perfil_ocupacional/'.format(uri=var_adls_uri)
cadastro_cbo_perfil_ocupacional = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
cadastro_cbo_perfil_ocupacional.display()

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/cadastro_cbo_sinonimo/'.format(uri=var_adls_uri)
cadastro_cbo_sinonimo = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
cadastro_cbo_sinonimo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # CNAE

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/ibge/cnae_subclasses/'.format(uri=var_adls_uri)
cnae_subclasses = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
cnae_subclasses.display()


# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/ibge/ipp_cnae_div_f/'.format(uri=var_adls_uri)
ipp_cnae_div_f = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
ipp_cnae_div_f.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # relatorio_dtb_brasil_municipio

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/raw/crw/ibge/relatorio_dtb_brasil_municipio/'.format(uri=var_adls_uri)
relatorio_dtb_brasil_municipio = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
relatorio_dtb_brasil_municipio.display()