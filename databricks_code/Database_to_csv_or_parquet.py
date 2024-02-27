# Databricks notebook source
def Saving_Files_From_SQL(Table_Name,address):
  df = spark.sql(f"select * from {Table_Name}")
  saving_df = df.coalesce(1).write.option('header','true').csv(f'{address}', mode='overwrite')
  # String problema => print(f"This is a book by {a_name}.")
  return saving_df

# COMMAND ----------

address =  'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/data/rais_05_10_15_20/vw_maioresocupacoesesetores'
Table_Name = 'rais_25.vw_maioresocupacoesesetores'
Saving_Files_From_SQL(Table_Name,address)

# COMMAND ----------



# COMMAND ----------

def Saving_Files(address,saving_address):
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  path = f'{var_adls_uri}{address}'
  file = spark.read.format("csv").option("header","true").option('sep',';').load(path)
  saving_df = df.coalesce(1).write.option('header','true').csv(f'{saving_address}', mode='overwrite')
  return saving_df

# COMMAND ----------

address = '/tmp/dev/trs/ms_sinan/acbi/'

# COMMAND ----------

saving_address = '/uds/uniepro/data/Vitor/ms_sinan_acbi'

# COMMAND ----------

datalake / tmp / dev / trs / inss / cat

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

address = '/tmp/dev/trs/inss/cat/'
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = f'{var_adls_uri}{address}'
file = spark.read.format("parquet").option("encoding", "ISO-8859-1").option("header","true").option('sep',';').load(path)
file.display()

# COMMAND ----------

address = '/uds/uniepro/data/Vitor/inss_cat/'
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
saving_address = f'{var_adls_uri}{address}'
saving = file.coalesce(1).write.option("encoding", "ISO-8859-1").option('header','true').csv(f'{saving_address}', mode='overwrite')

# COMMAND ----------

