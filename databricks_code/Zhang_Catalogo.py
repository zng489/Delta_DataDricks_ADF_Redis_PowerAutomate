# Databricks notebook source
dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
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

env = dbutils.widgets.get("env") # dev
source_name = dbutils.widgets.get("source_name") # crw
source_type = dbutils.widgets.get("source_type") # external
default_dir = '/'.join(['','tmp','dev'])
if env == 'prod':
  default_dir = ""
#else default_dir
#print(env)
#return default_dir

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


def CHECKING_ALL_PATHS_AND_LAYER(valor):
  DataLake = []
  for i in valor: 
    if i in ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/data_steward', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/schema',
            'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/source','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/table',
            'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/trello/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/sti/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/unigest/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/usr/oba/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/universidade_corporativa/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/mtd/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/evt/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/dh/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/indicadores/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/fred/', 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/trello/']:
      pass
    else:
      try:
        #print(dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net" + "/" + i ))
        A = dbutils.fs.ls(i)
        for B in A:
          #print(B[0])
          #print(dbutils.fs.ls(B[0]))
          for C in dbutils.fs.ls(B[0]):
            #print(C)
            #DataLake.append(C.path)
            for D in dbutils.fs.ls(C.path):
              for E in dbutils.fs.ls(D.path):
                for F in dbutils.fs.ls(E.path):
                  for G in dbutils.fs.ls(F.path):
                    DataLake.append(G.path)
      except:
        DataLake.append('This request is not authorized' + i)
  #DataLake
  return DataLake


def CLEANING_CHECKING_ALL_PATHS_AND_LAYER(DataLake):
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
    else:
      #print(D)
      Paths_.append(D)
      #re.search("^The.*Spain$", txt)

  Layer_ = list(dict.fromkeys(Paths_)) 
  return Layer_  
  
  
def get_source(path:'abfss address') -> 'retornará o primeiro nível do path': 
  camada = normaliza_path(path).split('/') #.replace("tmp" + "/" + "dev" + '/','').split('/')
  print(camada)
  final = camada[0]
  if final in ["tmp","dev" ]:
    if camada[2] == 'raw':
      final = camada[4]
      if final == 'bdo':
        pass
    if camada[2] == 'trs':
      final = camada[3]
    if camada[2] == 'biz':
      final = camada[3]
  elif final in ["raw"]:
    final = camada[2]

  elif final in ["trs"]:
    final = camada[1]
    
  elif final in ["biz"]:
    final = camada[1]
  return final
  
  
def get_schema(path):
  x = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')
  schema_name = x[1]
  if x[0] == 'trs':
    schema_name = x[2]
  if x[0] == 'raw' and x[1] in ['bdo','crw','usr','gov']:
    schema_name = x[3]
  if x[0] == 'raw' and x[1] in ['usr'] and x[2] in ['sti','uniepro','unigest']:
    schema_name = x[2] + '_' + x[3]
  if x[0] == 'biz':
    schema_name = x[2] 
  return schema_name


def get_table(path: 'abfss address') -> 'Nome da Tabela':
  x = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')
  table = x[len(x) - 2]
  return table  
  
  
def get_size(values):
  data = []
  data_size = []
  for A in values:
    try:
      total_size = 0
      for B in dbutils.fs.ls(A):
        if B.name[-1:] != '/':
          data_size.append(B.size)
          total_size += B.size
        else:
          total_size = 'This request is not authorized'
    except:
      total_size = 'This request is not authorized'
    data.append({'path':A,'size_bytes':str(total_size)})
    data    
  return data  
  
  
def get_size_layers(values_values):
  data_size = []
  for F in values_values:
    try:
      total_size = 0
      for G in dbutils.fs.ls(F):
        for H in dbutils.fs.ls(G.path):
          for I in dbutils.fs.ls(H.path):
            total_size += I.size
      
    except:
      total_size = 'This request is not authorized'
      
    data_size.append({'path':F,'size_bytes':str(total_size)}) 
  data_size   
  return data_size  
  
  
def SOURCE_SCHEMA_TABLE_RAW_(J):
  conjunto_tabela = []
  for item in J:
    source_name = get_source(item)
    schema_name = get_schema(item)
    table_name = get_table(item)
    table_path = normaliza_path(item)
    if source_name == schema_name:
      schema_name = ''
    elif source_name == table_name:
      table_name = " "
    elif schema_name == table_name:
      table_name = " "
    else:
      pass
    elemento = dict([('source_name',source_name), 
                     ('schema_name',schema_name), 
                     ('table_name',table_name), 
                     ('path__',table_path)
                    ]) 
    conjunto_tabela.append(elemento)
  conjunto_tabela
  SOURCE_SCHEMA_TABLE_RAW_ = spark.createDataFrame(conjunto_tabela)
  return SOURCE_SCHEMA_TABLE_RAW_  
  
 
def layers():
  layer = dbutils.widgets.get("layers")
  return layer
  
  

if __name__ == "__main__":
  import time
  import re
  from pyspark.sql import Row
  from pyspark.sql.functions import *
  from delta.tables import *
  from pyspark.sql.types import *
  from pyspark.sql import functions as F
  from pyspark.sql.functions import lit
  
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  
  var = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  path = "{adl_path}{default_dir}".format(adl_path=var, default_dir=function_0()) 

  CHECKING_ALL_PATHS_AND_LAYER(varre_(path))
  CLEANING_CHECKING_ALL_PATHS_AND_LAYER(CHECKING_ALL_PATHS_AND_LAYER(varre_(path)))
  
  df = (
    spark.createDataFrame(CLEANING_CHECKING_ALL_PATHS_AND_LAYER(CHECKING_ALL_PATHS_AND_LAYER(varre_(path))), StringType())
  ).withColumnRenamed("value", "paths")
  
  DF_ = (df.filter(col("paths").contains(f"{layers()}")))\
  .filter(~col("value").contains("bdo"))\
  .select('paths')

# COMMAND ----------

