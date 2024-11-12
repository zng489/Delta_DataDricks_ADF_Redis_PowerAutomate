# Databricks notebook source
dbutils.fs.ls("dbfs:/Folder")

"""
dbutils.fs.ls("dbfs:/ZY")
Out[2]: [FileInfo(path='dbfs:/ZY/airport_codes/', name='airport_codes/', size=0, modificationTime=1642009741000),
FileInfo(path='dbfs:/ZY/departuredelays/', name='departuredelays/', size=0, modificationTime=1642101456000),
FileInfo(path='dbfs:/ZY/prm/', name='prm/', size=0, modificationTime=1643126098000),
FileInfo(path='dbfs:/ZY/prm.csv/', name='prm.csv/', size=0, modificationTime=1643126165000),
FileInfo(path='dbfs:/ZY/prmm/', name='prmm/', size=0, modificationTime=1694701693000),
FileInfo(path='dbfs:/ZY/teste/', name='teste/', size=0, modificationTime=1642188743000)]
"""

# COMMAND ----------

# Make a Directory:
dbutils.fs.mkdirs("dbfs:/Folder")

# COMMAND ----------

Copy Files:

python
Copiar código
dbutils.fs.cp("/path/to/source/file", "/path/to/destination/file")


Move Files:

python
Copiar código
dbutils.fs.mv("/path/to/source/file", "/path/to/destination/file")


# COMMAND ----------

# Delete Files:
dbutils.fs.rm("dbfs:/ZY/teste", True)  # True to delete recursively

# COMMAND ----------

# Delete Files:
dbutils.fs.rm("dbfs:/Folder", True)  # True to delete recursively

# COMMAND ----------

dbutils.fs.rm("dbfs:/ZY", True)  # True to delete recursively

# COMMAND ----------

Read a File:

python
Copiar código
file_contents = dbutils.fs.head("/path/to/file")


Write a File:

python
Copiar código
dbutils.fs.put("/path/to/file", "file content", overwrite=True)
Example Usage

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest
import re

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/uds/uniepro/especialistas_negocios/delta/'.format(uri=var_adls_uri)

df = spark.read.format("delta").option("header","true").option("versionAsOf", 1).load(path)

# COMMAND ----------

for c in df.columns:
  #df = df.withColumnRenamed(c, c.lower())
  df = df.withColumnRenamed(c, re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', c).encode('ASCII', 'ignore').decode('ASCII').replace(' ', '_').replace('-', '_').lower()))
  

# COMMAND ----------

def rename_columns(df):
  from unicodedata import normalize  
  import re
  
  regex = re.compile(r'[.,;{}()\n\t=]')
  for col in df.columns:
      col_renamed = regex.sub('', normalize('NFKD', col.strip())
                             .encode('ASCII', 'ignore')        
                             .decode('ASCII')                 
                             .replace(' ', '_')                    
                             .replace('-', '_')
                             .replace('/', '_')
                             .replace('$', 'S')
                             .upper())
      df = df.withColumnRenamed(col, col_renamed)
  return df

# COMMAND ----------

df.coalesce(1).write.format('csv').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/mulheres_industria_fnme/censo_tecnico_csv/', header = True, mode='overwrite')
