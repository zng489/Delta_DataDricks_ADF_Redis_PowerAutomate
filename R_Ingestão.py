# Databricks notebook source
# MAGIC %r
# MAGIC remotes::install_github("rfsaldanha/microdatasus")
# MAGIC library(microdatasus)

# COMMAND ----------

import os
os.makedirs("/tmp/R/", exist_ok=True)

# COMMAND ----------

# MAGIC %ls /tmp/R/

# COMMAND ----------

# MAGIC %r
# MAGIC sum_function <- function(value, output_folder) {
# MAGIC   x <- c(2000, 2001)
# MAGIC   for (year in x) {
# MAGIC     dados_sinasc <- fetch_datasus(year_start = year, year_end = year, uf = "SP", information_system = value)
# MAGIC     file_path <- file.path(output_folder, paste0(value, "_", year, ".csv"))
# MAGIC     write.csv(dados_sinasc, file = file_path, row.names = FALSE)
# MAGIC   }
# MAGIC   return(NULL)
# MAGIC }
# MAGIC
# MAGIC output_folder <- "/tmp/R/"
# MAGIC sum_function("SIM-DO", output_folder)
# MAGIC

# COMMAND ----------

# MAGIC %ls /tmp/R/

# COMMAND ----------

import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('/tmp/R/SIM-DO_2000.csv')



# COMMAND ----------

df

# COMMAND ----------

spark_df = spark.createDataFrame(df)

# COMMAND ----------

spark_df.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#path = '{uri}/raw/crw/me/comex/ncm_exp/'.format(uri=var_adls_uri)

spark_df.write.format('parquet').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/uld/uniepro/testing', header = True, mode='overwrite')