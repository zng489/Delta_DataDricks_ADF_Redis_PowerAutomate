# Databricks notebook source
import time
import re
from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
  
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

def def_environment(value):
  env = value
  default_dir = '/'.join(['','tmp','dev'])
  if env == 'prod':
    default_dir = ""
    #print(env)
  return default_dir
def normaliza_path(path:'abfss address') -> 'string address':
  i = (3 if (path[0:6] == 'abfss:') else 0)
  return('/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]))

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

def varre_(path):
  path_sources = ['/' + 'raw' + '/' + 'bdo', '/' + 'raw' + '/' + 'crw', '/' + 'raw' + '/' + 'usr', '/' + 'trs', '/' + 'biz']
  path_schemas = []
  path_tables = []
  for path_source in path_sources:
    path_schemas += gera_lista (path + path_source)
  return path_schemas

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


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

var = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = "{adl_path}{default_dir}".format(adl_path=var, default_dir=def_environment('prod')) 


DataLake = []
for i in varre_(path): 
  out_from_list = ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/data_steward','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/schema',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/source','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/table',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/trello/', 'abfss:datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/sti/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/unigest/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/oba/', 'abfss:datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/universidade_corporativa/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/mtd/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/evt/', 'abfss:datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/dh/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/indicadores/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/fred/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/trello/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/power_bi/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/jira_service_management/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/devops/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/datalake/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/azure_ad/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/sti/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/power_bi/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/jira_service_management/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/devops/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/raw/usr/datalake/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/azure_ad/',
           'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/datalake/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/STI/']
  if i in out_from_list:
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


L_ = list(map(lambda x: x.replace('CO_', ''), P_))
L_ = list(dict.fromkeys(L_))

from pyspark.sql import Row

L_
rdd1 = sc.parallelize(L_)

row_rdd = rdd1.map(lambda x: Row(x))

df=sqlContext.createDataFrame(row_rdd,['Paths'])
#df.display()


from pyspark.sql.types import StructField, StructType, StringType, MapType

data = []
for i in df.rdd.map(lambda x: x.Paths).collect():
  C = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net" + i
  try:
    A = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net" + "/" + i)    
    B = 'authorized'.upper()
  except:
    B = 'This request is not authorized'.upper()
    
  #data.append({'Paths'.upper():i,'Autorização'.upper():B, 'abfss://'.upper():C})
  data.append({'Paths'.upper():i,'Autorizacao'.upper():B})


df2 = spark.createDataFrame(data=data)
#df2.display()

# COMMAND ----------

df3 = df2.filter( col ('AUTORIZACAO') != 'THIS REQUEST IS NOT AUTHORIZED')
df4 = df3.rdd.map(lambda x: x.PATHS).collect()

# COMMAND ----------

data_size = []
for F in df4:
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  path = f'{var_adls_uri}{F}'
  try:
    total_size = 0
    for G in dbutils.fs.ls(path):
      for H in dbutils.fs.ls(G.path):
        for I in dbutils.fs.ls(H.path):
          total_size += I.size
  except:
    pass
  
  data_size.append({'paths':F,'size_bytes':str(total_size)}) 
data_size   

# COMMAND ----------

df_size = spark.createDataFrame(data=data_size)
df_size.createOrReplaceTempView('raw_size')

# COMMAND ----------

df_size.display()