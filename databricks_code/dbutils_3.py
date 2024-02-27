# Databricks notebook source
#dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("env", "prod", ["dev", "prod"])
dbutils.widgets.dropdown("layers", "raw", ["raw", "trs", "biz"])
dbutils.widgets.text("source_name","raw")
dbutils.widgets.text("source_type","external")

# COMMAND ----------

import time
import re
from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
  
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

def function_0():
  env = dbutils.widgets.get("env") # dev
  source_name = dbutils.widgets.get("source_name") # crw
  source_type = dbutils.widgets.get("source_type") # external
  default_dir = '/'.join(['','tmp','dev'])
  if env == 'prod':
    default_dir = ""
  #print(env)
  return default_dir

print(function_0())

# COMMAND ----------

def normaliza_path(path:'abfss address') -> 'string address':
  i = (3 if (path[0:6] == 'abfss:') else 0)
  return('/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]))


  

# COMMAND ----------

def gera_lista(caminho):
  conjunto = []
  try:lista = dbutils.fs.ls(caminho)
  except: dbutils.notebook.exit("Arquivo não encontrado")
  for item in lista:
    path = item.path
    path_normalizada = normaliza_path(path)
    if path_normalizada in ['trs/mtd']:
      conjunto = conjunto + gera_lista(path)
      continue
    else:
      conjunto.append(path)
  return conjunto

# COMMAND ----------

def varre_(path):
  path_sources = ['/' + 'raw' + '/' + 'bdo', '/' + 'raw' + '/' + 'crw', '/' + 'raw' + '/' + 'usr', '/' + 'trs', '/' + 'biz']
  path_schemas = []
  path_tables = []
  for path_source in path_sources:
    path_schemas += gera_lista (path + path_source)
  return path_schemas

# COMMAND ----------

def varre_retorna_lista(caminhos):
  conjunto = []
  for path_schema in caminhos:
    try:
      items = dbutils.fs.ls(path_schema)
      for item in items:
        path = item.path.rsplit('/', 1)[0]
        path_normalizada = normaliza_path(path)
        if path_normalizada in ['trs/mtd']:
          conjunto = conjunto + gera_lista (path)
          continue
        else:
          conjunto.append(path)
        path_tables = conjunto
    except:
      item = 'erro'
  return path_tables

# COMMAND ----------

def splitting(DataLake,layer):
  Paths_ = []
  for x in DataLake:
    D = x.split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('nr')[0].split('net')[-1] #.split('ID')[0] #x.split('.')[-1].
    if D.__contains__('='):
      pass
    elif D.__contains__('NU_'):
      pass
    elif D.__contains__('CD_'):
      pass
    elif D.__contains__('parquet'):
      pass
    elif D.__contains__('parquet/'):
      pass
    elif D.__contains__('csv/'):
      pass
    elif D.__contains__('csv'):
      pass
    elif re.search("^[0-9]", D):
      pass
    elif D.__contains__(layer):
      Paths_.append(D)
    else:
      pass
        #re.search("^The.*Spain$", txt)
  Layer_ = list(dict.fromkeys(Paths_)) 
  return Layer_

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

var = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = "{adl_path}{default_dir}".format(adl_path=var, default_dir=function_0()) 

# COMMAND ----------

DataLake = []
for i in varre_(path): 
  if i in ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/data_steward','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/schema','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/source','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/table','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/trello/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/sti/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/unigest/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/oba/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/universidade_corporativa/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/mtd/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/evt/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/dh/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/indicadores/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/fred/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/trello/']:
    pass
  else:
    try:
      #print(dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net" + "/" + i ))
      A = dbutils.fs.ls(i)
      for B in A:
        for C in dbutils.fs.ls(B[0]):
          DataLake.append(C.path)
    except:
      DataLake.append('This request is not authorized' + i)
#DataLake

# COMMAND ----------

Paths_ = []
for x in DataLake:
  D = x.split('.')[-1].split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('nr')[0].split('net')[-1] #.split('ID')[0]
  if D.__contains__('='):
    pass
  elif D.__contains__('NU_'):
    pass
  elif D.__contains__('CD_'):
    pass
  elif D.__contains__('parquet'):
    pass
  elif D.__contains__('parquet/'):
    pass
  elif D.__contains__('csv/'):
    pass
  elif D.__contains__('csv'):
    pass
  elif re.search("^[0-9]", D):
    pass
  elif D.__contains__('/raw/'):
    Paths_.append(D)
  else:
    pass
      #re.search("^The.*Spain$", txt)
Layer_ = list(dict.fromkeys(Paths_)) 

# COMMAND ----------

path_list = []
for i in Layer_: 
  try:
    ii = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net' + i
    #print(ii)
    D = dbutils.fs.ls(ii)
    for E in D:
      #print(E)
      for F in dbutils.fs.ls(E[0]):
        path_list.append(F.path)
  except:
    #pass
    path_list.append('This request is not authorized' + ii)
    #print(i)

# COMMAND ----------

P_ = []
for x in path_list:
  D = x.split('.')[-1].split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('nr')[0].split('net')[-1] #.split('ID')[0]
  if D.__contains__('='):
    pass
  elif D.__contains__('NU_'):
    pass
  elif D.__contains__('CD_'):
    pass
  elif D.__contains__('parquet'):
    pass
  elif D.__contains__('parquet/'):
    pass
  elif D.__contains__('csv/'):
    pass
  elif D.__contains__('csv'):
    pass
  elif re.search("^[0-9]", D):
    pass
  elif D.__contains__('/biz/'):
    pass
  elif D.__contains__('/trs/'):
    pass
  else:
    #print(D)
    P_.append(D)
      #re.search("^The.*Spain$", txt)
L_ = list(dict.fromkeys(P_)) 

# COMMAND ----------

from pyspark.sql import Row

L_
rdd1 = sc.parallelize(L_)

row_rdd = rdd1.map(lambda x: Row(x))

df=sqlContext.createDataFrame(row_rdd,['Paths'])

# COMMAND ----------



# COMMAND ----------

df.display()

# COMMAND ----------

