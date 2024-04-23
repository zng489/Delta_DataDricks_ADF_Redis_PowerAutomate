# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime
import sys

# COMMAND ----------

if momento == 'oficial': #desenv ou oficial
  from cni_connectors import adls_gen1_connector as adls_conn
  var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
  from cni_connectors import pbi_stage_connector as pbi_conn
  var_pbi_uri = pbi_conn.adls_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic", container="powerbi-logs")
else:
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  var_pbi_uri = 'abfss://powerbi-logs@cnipowerbicorporativo2.dfs.core.windows.net'

# COMMAND ----------

dbutils.widgets.text("env","dev") #dev ou prod
dbutils.widgets.text("path","")

env = dbutils.widgets.get("env")
var_path = dbutils.widgets.get("path")

# COMMAND ----------

default_dir = '/'.join(['','tmp','dev', 'gov', 'v2'])
if env == 'prod': default_dir = '/'.join(['', 'gov', 'v2'])

default_dir_read = '/'.join(['','tmp','dev'])
if env_read == 'prod': default_dir_read = ""  

default_dir_dh = '/'.join(['','tmp','dev'])
if env == 'prod': default_dir_dh = "" 

# COMMAND ----------

path = "{adl_path}{default_dir}/{var_path}".format(adl_path=var_adls_uri,default_dir=default_dir,var_path=var_path)
path_acl = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['','tables','acl'])
path_prop = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['','tables','prop'])
path_size = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['','tables','size'])
path_tabela = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['','tables','tabela'])
path_esquema = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['','tables','esquema'])
path_campo = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['','tables','campo'])
path_catalogo = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['','catalogo'])
path_adls_report = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir) + '/'.join(['', 'adls_report'])
path_dh = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir_dh) +  '/'.join(['', 'trs', 'dh'])
path_read = "{adl_path}{default_dir_read}/{var_path}".format(adl_path=var_adls_uri,default_dir_read=default_dir_read,var_path=var_path)
path_pbi = "{pbi_path}".format(pbi_path=var_pbi_uri) + '/'.join(['','automacao_powerbi'])

# COMMAND ----------

print('env',': ',env)
print('var_path',': ',var_path)
print('path',': ',path)
print('path_acl',': ',path_acl)
print('path_prop',': ',path_prop)
print('path_size',': ',path_size)
print('path_tabela',': ',path_tabela)
print('path_esquema',': ',path_esquema)
print('path_campo',': ',path_campo)
print('path_catalogo',': ',path_catalogo)
print('path_adls_report',': ',path_adls_report)
print('path_read',': ',path_read)
print('path_read',': ',path_read)
print('pbi_path',': ',path_pbi)
print('path_dh',': ',path_dh)

# COMMAND ----------

def last_version (dataframe, chave_primaria, coluna_de_versao):
  key = chave_primaria.replace(" ", "").split(',')
  chave = key.copy()
  chave.append(coluna_de_versao)
  tb_last_version = dataframe.groupBy(key).agg(max(coluna_de_versao).alias(coluna_de_versao))
  return dataframe.join(tb_last_version, chave, 'inner')    

# COMMAND ----------

# retorna o caminho relativo e normalizado do path informado (remove as barras iniciais e finais caso existam)
def normaliza_path(path):
  if path[-1] == '/': path = path[0:-1]
  if path[0] == '/': path = path[1:]
  i = (3 if (path[0:6] == 'abfss:') else 0)
  return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')

# COMMAND ----------

# retorna um array com os 3 níveis do catálogo no datalake ['camada','esquema','tabela']
def path_niveis (path):
  a = normaliza_path(path).split('/')
  if a[0:2] == ['tmp', 'dev']:
    a = a[2:]
  n1 = '-'.join(a[0:1])
  n2 = '-'.join(a[1:-1])
  n3 = '-'.join(a[-1:])     
  if a[0] == 'raw':
    n1 = '-'.join(a[0:2])
    n2 = '-'.join(a[2:-1])
    n3 = '-'.join(a[-1:])
  t = [n1, n2, n3]
  return t

# COMMAND ----------

# retorna o caminho completo até o nível de tabela (pasta) no datalake (desprezando os níveis de partição e de arquivos)
def get_table_level(path):
  array = normaliza_path(path).split('/')
  array_retorno = []
  stop = ['=', '.', '_SUCCESS', '_committed_', '_started_']
  for item in array:
    if not any(x in item for x in stop):
      array_retorno.append(item)
  return '/'.join(array_retorno)

df_get_table_level = udf(lambda z:get_table_level(z),StringType())

# COMMAND ----------

# retorna uma lista com as pastas e subpastas do datalake a partir da variável path informada
def get_paths (path):
  conjunto = []
  try: lista = dbutils.fs.ls(path)
  except: dbutils.notebook.exit("Path não encontrado: " + path)
  for item in lista:
    if item.name[-1:] == '/':
      if item.name.find("=") == -1:
        conjunto.append(item.path)
        conjunto += get_paths(item.path)
    else:
      continue
  return conjunto

# COMMAND ----------

# retorna a pasta (o nível) de acordo com o nível informado
# exemplo: para o path = "raw/crw/ibge/ipca"
# se escolhido o nível 3 o retorno será a string "ibge"
def get_nivel (path, nivel):
  array = normaliza_path(path).split('/')
  retorno = '-'
  if len(array) >= nivel:
    retorno = array[nivel-1]
  if retorno == '': retorno = '-'
  return retorno

# possibilita o funcionamento da função no dataframe
get_nivel_df = udf(lambda z,x:get_nivel(z,x),StringType())
