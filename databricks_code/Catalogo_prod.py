# Databricks notebook source
#from cni_connectors import adls_gen1_connector as adls_conn 
#var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")


#dbutils.widgets.text("env","dev")

dbutils.widgets.text("env","prod")
dbutils.widgets.text("source_name","biz")
dbutils.widgets.text("source_type","external")


env = dbutils.widgets.get("env") # dev
source_name = dbutils.widgets.get("source_name") # crw
source_type = dbutils.widgets.get("source_type") # external


default_dir = '/'.join(['','tmp','dev'])
if env == 'prod':
  default_dir = ""

# COMMAND ----------

import time
#from cni_connectors import adls_gen1_connector as adls_conn

#var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

time.sleep(5)

from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F



#path = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir)



def normaliza_path(path:'abfss address') -> 'string address':
  i = (3 if (path[0:6] == 'abfss:') else 0)
  
  # Essa função 'pega' tudo estiver depois de 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/'
  # Por exemplo: 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/uds/uniepro/cbo_classificacoes/'
  # Restará apenas 'tmp/dev/raw/uds/uniepro/cbo_classificacoes/'

  #return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')
  return('/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:])) #.replace("tmp" + "/" + "dev" + '/','')



def gera_lista(caminho):  ## recebe o caminho da camada e gera uma lista de esquemas (path completo)
  
  # A função 'gera_lista' retorna uma lista de arquivos que estão contidos na pasta salva
  # Basicamente retorna os arquivos salvos na pasta 'nome da tabela'
  # Por exemplo 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/uniepro/fta_rfb_cno/cno_biz'
  # Retornará '[REDACTED]/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/_committed_1948776334523571779', '[REDACTED]/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/_committed_6259592133676061223', '[REDACTED]/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/_committed_7315066556330505739', '[REDACTED]/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/_committed_7632283806446112565', '[REDACTED]/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/_committed_vacuum3678127595308028408', '[REDACTED]/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/_started_7315066556330505739', '[REDACTED]/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/part-00000-tid-7315066556330505739-b57dcd81-2220-4e41-a35c-bcba3d1b3138-1062-1-c000.snappy.parquet']
  
  
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
  #print(conjunto)
  return conjunto
  
def varre_(path): ## recebe o caminho da camada 
  path_sources = ['/' + 'raw' + '/' + 'bdo', '/' + 'raw' + '/' + 'crw', '/' + 'raw' + '/' + 'usr', '/' + 'trs', '/' + 'biz']
  path_schemas = []
  path_tables = []
  for path_source in path_sources:

    #print(path,path_source)
    path_schemas += gera_lista (path + path_source)
    #print(path_schemas)
  return path_schemas


def varre_retorna_lista(caminhos):
  conjunto = []
  #conjunto_not_authorized = []
  #size = 0
  for path_schema in caminhos:
  #print(path_schema)
    #conjunto = []
    try:
      items = dbutils.fs.ls(path_schema)
      #print(items)
    
      for item in items:
        #print(item[0])
        #print(item.path)
        path = item.path.rsplit('/', 1)[0]
        #print(path)
        path_normalizada = normaliza_path(path)
        #print(path_normalizada)
      
        if path_normalizada in ['trs/mtd']:
          conjunto = conjunto + gera_lista (path)
          ##print(conjunto)
          continue
        else:
          conjunto.append(path)
          #conjunto += path
        
         #print(conjunto)
        path_tables = conjunto
        #print(path_tables)
    except:
      #lista = dbutils.fs.ls(path_schema)
      item = 'erro'
      #conjunto_not_authorized.append(item)
    #print(path_schema) 

  return path_tables
      ##print('zhang',path_schema)

# COMMAND ----------

var = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/'  
path = "{adl_path}{default_dir}".format(adl_path=var, default_dir=default_dir)

valor = varre_(path) 
#print(varre_(path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building Catalog

# COMMAND ----------

#DataLake = []
#for i in df_raw_list: 
#  if  i in ['tmp/dev/raw/usr/catalogo/data_steward','tmp/dev/raw/usr/catalogo/schema','tmp/dev/raw/usr/catalogo/source','tmp/dev/raw/usr/catalogo/table']:
#    pass
#  else:
#    try:
#      #print(dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net" + "/" + i ))
#      A = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net" + "/" + i )
#      for B in A:
#        #print(B[0])
#        #print(dbutils.fs.ls(B[0]))
#        for C in dbutils.fs.ls(B[0]):
#          #print(C)
#          DataLake.append(C.path)           
#    except:
#      DataLake.append(i)
#DataLake

def CHECKING_ALL_PATHS_AND_LAYER(valor):
  DataLake = []
  for i in valor: 
    if  i in ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/data_steward','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/schema','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/source','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/catalogo/table']:
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
            DataLake.append(C.path)           
      except:
        DataLake.append('This request is not authorized' + i)
  #DataLake
  return DataLake
  
  
  
  
import re
def CLEANING_CHECKING_ALL_PATHS_AND_LAYER(DataLake):
  Paths_ = []

  for x in DataLake:
    D = x.split('.')[-1].split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('ID')[0].split('nr')[0].split('net')[-1]
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
  
  
  
  
  
  
if __name__ == '__main__':
  A = CHECKING_ALL_PATHS_AND_LAYER(valor)
  CLEANING_CHECKING_ALL_PATHS_AND_LAYER(A)
  df = (
    spark.createDataFrame(CLEANING_CHECKING_ALL_PATHS_AND_LAYER(A), StringType())
  ).withColumnRenamed("value", "PATHS")
  
  RAW = (df.filter(col("PATHS").contains("raw")))\
  .filter(~col("value").contains("bdo"))\
  .select('PATHS')
  
  TRS = (df.filter(col("PATHS").contains("trs")))\
  .filter(~col("value").contains("bdo"))\
  .select('PATHS')
    
  BIZ = (df.filter(col("PATHS").contains("biz")))\
  .filter(~col("value").contains("bdo"))\
  .select('PATHS')  
  
  
  #display(RAW)
  #display(TRS)
  #display(BIZ)

# COMMAND ----------

RAW.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source, Schema, Table

# COMMAND ----------

import time
#from cni_connectors import adls_gen1_connector as adls_conn

#var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

time.sleep(5)

from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F



#path = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir)



def normaliza_path(path:'abfss address') -> 'string address':
  i = (3 if (path[0:6] == 'abfss:') else 0)
  
  # Essa função 'pega' tudo estiver depois de 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/'
  # Por exemplo: 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/uds/uniepro/cbo_classificacoes/'
  # Restará apenas 'tmp/dev/raw/uds/uniepro/cbo_classificacoes/'

  #return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')
  return('/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:])) #.replace("tmp" + "/" + "dev" + '/','')



def get_source(path:'abfss address') -> 'retornará o primeiro nível do path': 
  
  # Função get_source 'captura' a primeiro nivel dos path
  # Por exemplo 'tmp/dev/raw/uds/uniepro/cbo_classificacoes/'
  # Irá retornar apenas a string 'tmp' primeiro nivel
  
  camada = normaliza_path(path).split('/') #.replace("tmp" + "/" + "dev" + '/','').split('/')
  #print(camada)
  #['tmp', 'dev', 'raw', 'crw', 'aco_brasil', 'prod_aco_nacional', '']
  #['raw', 'crw', 'aco_brasil', '']
  print(camada)
  
  final = camada[0]
  #print(final)
  #tmp
  
  if final in ["tmp","dev" ]:
    #print(path)
    
    if camada[2] == 'raw':
      #print(camada[2])
      #raw
      final = camada[4]
      
      if final == 'bdo':
        pass
      
    # PARA CAMADA TMP DEV TRS  
    if camada[2] == 'trs':
      final = camada[3]
      
      
    # PARA CAMADA TMP DEV TRS  
    if camada[2] == 'biz':
      final = camada[3]
      
      
  # Para ambiente PROD  
  elif final in ["raw"]:
    final = camada[2]
    #print(final)
    
  elif final in ["trs"]:
    final = camada[1]
    #print(final)
    
  elif final in ["biz"]:
    final = camada[1]
    #print(final)

  #return final
  return final




def get_schema(path):
  
  # Se o path possuír os termos 'tmp', '/' e 'dev', tais termos serão 'excluidos' e retonara os valores em lista
  # Exemplo 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/uds/uniepro/cbo_classificacoes/'
  # Retornará ['raw', 'uds', 'uniepro', 'cbo_classificacoes', '']
  
  x = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')
  #print(x)
  #['biz', 'uniepro', 'fta_rfb_cno', 'cno_biz', '']
  
  schema_name = x[1]
  #print(schema_name)
  
  # Se o path for path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/mtd/TESTING/'
  # Se x[0] for igual a 'trs' 
  # E x[1] pertencer a ['mtd']
  # Retornará x[1]_x[2], por exemplo 'mtd_TESTING'
  
  #if x[0] == 'trs' and x[1] in ['mtd']:
    #schema_name = x[1] + '_' + x[2]
    
  if x[0] == 'trs':
    schema_name = x[2]
    #print(schema_name)

      
  if x[0] == 'raw' and x[1] in ['bdo','crw','usr','gov']:
    schema_name = x[3]
    #print('3',schema_name)
    
  if x[0] == 'raw' and x[1] in ['usr'] and x[2] in ['sti','uniepro','unigest']:
    schema_name = x[2] + '_' + x[3]
    
    
  if x[0] == 'biz':
    schema_name = x[2]
    
    
    
    
  #print(schema_name)  
  return schema_name




def get_table(path: 'abfss address') -> 'Nome da Tabela':
  
  # x = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')
  # (x[len(x) - 1])
  # Retornará o último valor da lista, por examplo:
  # path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/uniepro/fta_rfb_cno/cno_biz' -> cuidado com a barra, pois ela faz parte da contagem
  # ['biz', 'uniepro', 'fta_rfb_cno', 'cno_biz']
  # Irá retornar 'cno_biz'
  
  x = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')

  # print(x)
  #print(len(x))
  # cno_biz
  table = x[len(x) - 2]
  return table

#if __name__ == '__main__':
  
###############################################################################################################################  
  #get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/')
  #print(get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/'))
  
  #get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/aco_brasil/')
  #print(get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/aco_brasil/'))
  
  #get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/evt/arrecadacao/verso_guia_recolhimento/')
  #print(get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/evt/arrecadacao/verso_guia_recolhimento/'))  
  
  #get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/monitor_do_emprego_kpis/fta_kpis_transpose/')
  #print(get_source('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/monitor_do_emprego_kpis/fta_kpis_transpose/'))  
  
  
###############################################################################################################################    
  #get_schema('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/')
  #print(get_schema('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/'))
  
  #get_schema('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/comex/via/')
  #print(get_schema('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/comex/via/'))
  
  
###############################################################################################################################   

  #get_table('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/comex/via/')
  #print(get_table('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/comex/via/'))




  #get_table('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/')
  #print(get_table('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/uniepro/fta_rfb_cno/cno_biz/'))

# COMMAND ----------

# MAGIC %md
# MAGIC # RAW

# COMMAND ----------

DF_TEST = RAW.rdd.map(lambda x: x.PATHS).collect()
DF_TEST

J = []
for caminho in DF_TEST:
  CAMINHOS = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net' + caminho
  #print(CAMINHOS)
  J.append(CAMINHOS)
J


conjunto_tabela = []
path_tables = J


for item in path_tables:
  #print(item)
  source_name = get_source(item)
  #print(source_name)
  
  schema_name = get_schema(item)
  #print(schema_name)
  
  table_name = get_table(item)
  #print(table_name)
  
  table_path = normaliza_path(item)
  #print(table_path)
  
  
  if source_name == schema_name:
    schema_name = ''
    #return schema_name
  
  elif source_name == table_name:
    table_name = " "
    #return table_name
    
  elif schema_name == table_name:
    table_name = " "
    
  else:
    pass
  
    ####### source_type ##########################
  #source_type = 'external' if source_name == 'crw' else 'bigdata'
  
  ####### source_descriptione ##################
  #source_description = ''
  #if source_name == 'crw': source_description = 'Raw - Crawlers'
  #if source_name == 'raw': source_description = 'Raw - Sistemas'
  #if source_name == 'usr': source_description = 'Raw - Arquivos'
  #if source_name == 'trs': source_description = 'Trusted'
  #if source_name == 'biz': source_description = 'Business'
  
  elemento = dict([('source_name',source_name), 
                   ('schema_name',schema_name), 
                   ('table_name',table_name), 
                   ('path',table_path)
                  ])   
  conjunto_tabela.append(elemento)
  
  
SOURCE_SCHEMA_TABLE_RAW_ = spark.createDataFrame(conjunto_tabela)
#SOURCE_SCHEMA_TABLE_RAW_.display()

# COMMAND ----------

'''  

#values = ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/aco_brasil/prod_aco_nacional/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/']

def get_size_layers(values_values):
  data_size = []

  for F in values_values:
    #print(dbutils.fs.ls(F))
    #[FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201501/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201501/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201601/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201601/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201701/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201701/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201801/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201801/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201901/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201901/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/_SUCCESS', name='_SUCCESS', size=0)]
    total_size = 0
    for G in dbutils.fs.ls(F):
      #print(G)

      for H in dbutils.fs.ls(G.path):
        #print(H.size)
        data_size.append(H.size)
        total_size += H.size

    total_size
    #print(total_size)
  return total_size




def get_size(values):
  data = []
  data_size = []
  for A in values:
    
    #print(A)
    #abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/aco_brasil/prod_aco_nacional/
    
    total_size = 0
    try:
      for B in dbutils.fs.ls(A):
        #print(B)
        #abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/

        if B.name[-1:] != '/':
          #print(B.size)
          #print("LAST LAYER")


          data_size.append(B.size)
          total_size += B.size
          #element = {'path':B,'size':total_size}
        else:
          try:
            #print(dbutils.fs.ls(B.path))
            #pass

            total_size = get_size_layers(values)

            #total_size = 0
            #element = {'path':B,'size':0}
            #print('there are more')
          except:
            total_size = 1111
        #data.append({'path':A,'size':total_size})
    except:
      total_size = 11111
    data.append({'path':A,'size':total_size})  
      
    data
    #total_size
  #print(total_size)    
  return data

#if __name__ == '__main__':
  #get_size(values)
  #print(get_size(values))
  
'''

# COMMAND ----------

#dbutils.fs.ls('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/aco_brasil/prod_aco_nacional_f/')

# COMMAND ----------

DF_TEST = RAW.rdd.map(lambda x: x.PATHS).collect()
DF_TEST

J = []
for caminho in DF_TEST:
  CAMINHOS = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net' + caminho
  #print(CAMINHOS)
  J.append(CAMINHOS)
J

# COMMAND ----------

def get_size(values):
  data = []
  data_size = []
  for A in values:
    
    #print(A)
    #abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/aco_brasil/prod_aco_nacional/
    try:
      total_size = 0
      for B in dbutils.fs.ls(A):
        #print(B)
        #abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/

        if B.name[-1:] != '/':
          #print(B.size)
          #print("LAST LAYER")


          data_size.append(B.size)
          total_size += B.size
          #element = {'path':B,'size':total_size}
        else:
          #print(dbutils.fs.ls(B.path))
          #pass
          total_size = 'This request is not authorized'
          #element = {'path':B,'size':0}
          #print('there are more')

        #data.append({'path':A,'size':total_size})
      
    except:
      total_size = 'This request is not authorized'
      
      
    data.append({'path':A,'size_bytes':str(total_size)})
    data
    #total_size
  #print(total_size)    
  return data

# COMMAND ----------

PART_1_RAW_.display()

# COMMAND ----------

PART_1_RAW_ = spark.createDataFrame(get_size(J))
PART_1_RAW_This_request_is_not_authorized = PART_1_RAW_.filter(col('size_bytes') != 'This request is not authorized')
PART_1_RAW_This_request_is_not_authorized.display()

# COMMAND ----------


INTERME_RAW = PART_1_RAW_.filter(col('size_bytes') == 'This request is not authorized')
INTERME_RAW = INTERME_RAW.rdd.map(lambda x: x.path).collect()
#PART_2_RAW_

# COMMAND ----------

#INTERME_RAW 

# COMMAND ----------

'''
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
      total_size = 9999999
      
    data_size.append({'path':F,'size_bytes':total_size}) 
  data_size   
  return data_size
'''

# COMMAND ----------

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

# COMMAND ----------

values = INTERME_RAW 
E = get_size_layers(values)
#print(get_size_layers(values))

# COMMAND ----------

PART_2_RAW_ = spark.createDataFrame(E)
#PART_2_RAW_.display() 

# COMMAND ----------

from pyspark.sql.functions import lit


result = PART_1_RAW_This_request_is_not_authorized.union(PART_2_RAW_)
result.display()
# To remove the duplicates:

#result = result.dropDuplicates()

# COMMAND ----------

#df_reasult = result.dropDuplicates()
#df_reasult.display()

# COMMAND ----------


#df1=result.groupBy("path","size_bytes").count().filter("count > 1")
#df1.drop('count').show()
#df1.display()

# COMMAND ----------

@udf(returnType=StringType())
def normaliza_path(path:'abfss address') -> 'string address':
  i = (3 if (path[0:6] == 'abfss:') else 0)
  
  # Essa função 'pega' tudo estiver depois de 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/'
  # Por exemplo: 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/uds/uniepro/cbo_classificacoes/'
  # Restará apenas 'tmp/dev/raw/uds/uniepro/cbo_classificacoes/'

  #return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')
  return('/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:])) #.replace("tmp" + "/" + "dev" + '/','')
  

# COMMAND ----------

_CATALAGO_RAW_PROD = result.withColumn("_paths", normaliza_path("path")) 

# COMMAND ----------

_CATALAGO_RAW_PROD.display()

# COMMAND ----------

SOURCE_SCHEMA_TABLE_RAW_.display()

# COMMAND ----------

_CATALAGO_RAW_PROD__ = _CATALAGO_RAW_PROD.join(SOURCE_SCHEMA_TABLE_RAW_, _CATALAGO_RAW_PROD._paths == SOURCE_SCHEMA_TABLE_RAW_.path, "right").drop(SOURCE_SCHEMA_TABLE_RAW_.path) #.show() 

# COMMAND ----------

_CATALAGO_RAW_PROD__.display()

# COMMAND ----------

_CATALAGO_RAW_PROD__ = _CATALAGO_RAW_PROD__.select('path','_paths','source_name','schema_name','table_name','size_bytes')
_CATALAGO_RAW_PROD__.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # teste

# COMMAND ----------

df.dropDuplicates(['name', 'height']).show()
+---+------+-----+

# COMMAND ----------

@udf(returnType=StringType())
def Converter(str):
    result = ""
    a = str.split(" ")
      
    for q in a:
        if q == 'J' or 'C' or 'M':
            result += q[1:2].upper()
        else:
            result += q
    return result
  
  
df.withColumn("Special Names", Converter("Name")) \
    .show()

# COMMAND ----------

DF_TEST = result.rdd.map(lambda x: x.path).collect()
DF_TEST

# COMMAND ----------

# ESTranHO 318 e 203

'''
DF_TEST = SOURCE_SCHEMA_TABLE_RAW_.rdd.map(lambda x: x.path).collect()
DF_TEST

J = []
for caminho in DF_TEST:
  CAMINHOS = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net' + caminho
  #print(CAMINHOS)
  J.append(CAMINHOS)
J
'''

conjunto_tabela = []
path_tables = DF_TEST


for item in path_tables:
  #print(item)
  source_name = get_source(item)
  #print(source_name)
  
  schema_name = get_schema(item)
  #print(schema_name)
  
  table_name = get_table(item)
  #print(table_name)
  
  table_path = normaliza_path(item)
  #print(table_path)
  
  
  if source_name == schema_name:
    schema_name = ''
    #return schema_name
  
  elif source_name == table_name:
    table_name = " "
    #return table_name
    
  elif schema_name == table_name:
    table_name = " "
    
  else:
    pass
  
    ####### source_type ##########################
  #source_type = 'external' if source_name == 'crw' else 'bigdata'
  
  ####### source_descriptione ##################
  #source_description = ''
  #if source_name == 'crw': source_description = 'Raw - Crawlers'
  #if source_name == 'raw': source_description = 'Raw - Sistemas'
  #if source_name == 'usr': source_description = 'Raw - Arquivos'
  #if source_name == 'trs': source_description = 'Trusted'
  #if source_name == 'biz': source_description = 'Business'
  
  elemento = dict([('source_name',source_name), 
                   ('schema_name',schema_name), 
                   ('table_name',table_name), 
                   ('path',table_path)
                  ])   
  conjunto_tabela.append(elemento)
  
  
  
Z = spark.createDataFrame(conjunto_tabela)
#SOURCE_SCHEMA_TABLE_RAW_.display()

# COMMAND ----------

Z.display()
  
  
  
#SOURCE_SCHEMA_TABLE_RAW_ = spark.createDataFrame(conjunto_tabela)
#SOURCE_SCHEMA_TABLE_RAW_.display()

# COMMAND ----------

re = Z.dropDuplicates()

# COMMAND ----------

re.display()

# COMMAND ----------

def normaliza_path(path:'abfss address') -> 'string address':
  i = (3 if (path[0:6] == 'abfss:') else 0)
  
  # Essa função 'pega' tudo estiver depois de 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/'
  # Por exemplo: 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/uds/uniepro/cbo_classificacoes/'
  # Restará apenas 'tmp/dev/raw/uds/uniepro/cbo_classificacoes/'

  #return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')
  return('/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:])) #.replace("tmp" + "/" + "dev" + '/','')


# COMMAND ----------

SOURCE_SCHEMA_TABLE_RAW_

# COMMAND ----------

PART_1_RAW_
PART_2_RAW_

# COMMAND ----------

from pyspark.sql.functions import lit

cols = ['id', 'uniform', 'normal', 'normal_2']    

df_1_new = df_1.withColumn("normal_2", lit(None)).select(cols)
df_2_new = df_2.withColumn("normal", lit(None)).select(cols)

result = df_1_new.union(df_2_new)

# To remove the duplicates:

result = result.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # AAAAAAAAAAAAAAAAAAAAAAAAAAAA

# COMMAND ----------

# MAGIC %md
# MAGIC # TRS

# COMMAND ----------

TRS.display()

# COMMAND ----------

DF_TRS = TRS.rdd.map(lambda x: x.PATHS).collect()
DF_TRS

J = []
for caminho in DF_TRS:
  CAMINHOS = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net' + caminho
  #print(CAMINHOS)
  J.append(CAMINHOS)
J

# COMMAND ----------


conjunto_tabela = []
path_tables = J


for item in path_tables:
  #print(item)
  source_name = get_source(item)
  #print(source_name)
  
  schema_name = get_schema(item)
  #print(schema_name)
  
  table_name = get_table(item)
  #print(table_name)
  
  table_path = normaliza_path(item)
  #print(table_path)
  
  
  if source_name == schema_name:
    schema_name = ''
    #return schema_name
  
  elif source_name == table_name:
    table_name = " "
    #return table_name
    
  elif schema_name == table_name:
    table_name = " "
    
  else:
    pass
  
    ####### source_type ##########################
  #source_type = 'external' if source_name == 'crw' else 'bigdata'
  
  ####### source_descriptione ##################
  #source_description = ''
  #if source_name == 'crw': source_description = 'Raw - Crawlers'
  #if source_name == 'raw': source_description = 'Raw - Sistemas'
  #if source_name == 'usr': source_description = 'Raw - Arquivos'
  #if source_name == 'trs': source_description = 'Trusted'
  #if source_name == 'biz': source_description = 'Business'
  
  elemento = dict([('source_name',source_name), 
                   ('schema_name',schema_name), 
                   ('table_name',table_name), 
                   ('path',table_path)
                  ])   
  conjunto_tabela.append(elemento)

# COMMAND ----------

conjunto_tabela

# COMMAND ----------

_TRS_ = spark.createDataFrame(conjunto_tabela)
_TRS_.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # BIZ

# COMMAND ----------

BIZ.display()

# COMMAND ----------

DF_BIZ = BIZ.rdd.map(lambda x: x.PATHS).collect()
DF_BIZ

J = []
for caminho in DF_BIZ:
  CAMINHOS = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net' + caminho
  #print(CAMINHOS)
  J.append(CAMINHOS)

# COMMAND ----------

conjunto_tabela = []
path_tables = J


for item in path_tables:
  #print(item)
  source_name = get_source(item)
  #print(source_name)
  
  schema_name = get_schema(item)
  #print(schema_name)
  
  table_name = get_table(item)
  #print(table_name)
  
  table_path = normaliza_path(item)
  #print(table_path)
  
  
  if source_name == schema_name:
    schema_name = ''
    #return schema_name
  
  elif source_name == table_name:
    table_name = " "
    #return table_name
    
  elif schema_name == table_name:
    table_name = " "
    
  else:
    pass
  
    ####### source_type ##########################
  #source_type = 'external' if source_name == 'crw' else 'bigdata'
  
  ####### source_descriptione ##################
  #source_description = ''
  #if source_name == 'crw': source_description = 'Raw - Crawlers'
  #if source_name == 'raw': source_description = 'Raw - Sistemas'
  #if source_name == 'usr': source_description = 'Raw - Arquivos'
  #if source_name == 'trs': source_description = 'Trusted'
  #if source_name == 'biz': source_description = 'Business'
  
  elemento = dict([('source_name',source_name), 
                   ('schema_name',schema_name), 
                   ('table_name',table_name), 
                   ('path',table_path)
                  ])   
  conjunto_tabela.append(elemento)

# COMMAND ----------

_BIZ_ = spark.createDataFrame(conjunto_tabela)
_BIZ_.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

#values = ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/aco_brasil/prod_aco_nacional/','abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/']


def normaliza_path(path:'abfss address') -> 'string address':
  i = (3 if (path[0:6] == 'abfss:') else 0)
  
  # Essa função 'pega' tudo estiver depois de 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/'
  # Por exemplo: 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/uds/uniepro/cbo_classificacoes/'
  # Restará apenas 'tmp/dev/raw/uds/uniepro/cbo_classificacoes/'

  #return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')
  return('/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:])) #.replace("tmp" + "/" + "dev" + '/','



def get_size_layers(values_values):
  data_size = []

  for F in values:
    #print(dbutils.fs.ls(F))
    #[FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201501/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201501/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201601/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201601/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201701/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201701/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201801/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201801/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201901/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201901/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/_SUCCESS', name='_SUCCESS', size=0)]
    total_size = 0
    for G in dbutils.fs.ls(F):
      #print(G)

      for H in dbutils.fs.ls(G.path):
        #print(H.size)
        data_size.append(H.size)
        total_size += H.size

    total_size
    #print(total_size)
  return total_size




def get_size(values):
  data = []
  data_size = []
  for A in values:
    
    print(A)
    #abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/aco_brasil/prod_aco_nacional/
    
    total_size = 0

    for B in dbutils.fs.ls(A):
      print(B)
      #abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/

      if B.name[-1:] != '/':
        #print(B.size)
        #print("LAST LAYER")


        data_size.append(B.size)
        total_size += B.size
        #element = {'path':B,'size':total_size}
      else:
        try:
          #print(dbutils.fs.ls(B.path))
          #pass

          total_size = get_size_layers(values)

          #total_size = 0
          #element = {'path':B,'size':0}
          #print('there are more')
        except:
          total_size = 99
      #data.append({'path':A,'size':total_size})

    data.append({'path':normaliza_path(A),'size':total_size})  
      
    data
    #total_size
  #print(total_size)    
  return data

if __name__ == '__main__':
  #values = ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/caged_ajustes/']
  get_size_layers(J)
  print(get_size_layers(J))

# COMMAND ----------

def get_size_layers(values_values):
  data_size = []

  for F in values:
    #print(dbutils.fs.ls(F))
    #[FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201501/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201501/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201601/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201601/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201701/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201701/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201801/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201801/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/CD_ANO_MES_COMPETENCIA_DECLARADA=201901/', name='CD_ANO_MES_COMPETENCIA_DECLARADA=201901/', size=0), FileInfo(path='abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/ibge/caged_ajustes/_SUCCESS', name='_SUCCESS', size=0)]
    total_size = 0
    for G in dbutils.fs.ls(F):
      #print(G)

      for H in dbutils.fs.ls(G.path):
        #print(H.size)
        data_size.append(H.size)
        total_size += H.size

    total_size
    #print(total_size)
  return total_size

if __name__ == '__main__':
  values = ['abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/caged_ajustes/']
  get_size_layers(values)
  print(get_size_layers(values))

# COMMAND ----------

dbutils.fs.ls('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/caged_ajustes/CD_ANO_MOVIMENTACAO=2012/')

# COMMAND ----------

T = get_size(J)

# COMMAND ----------

dbutils.fs.ls('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/me/caged_ajustes/')