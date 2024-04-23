# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("env","dev")
dbutils.widgets.text("source_name","crw")
dbutils.widgets.text("source_type","external")

# COMMAND ----------

env = dbutils.widgets.get("env")
source_name = dbutils.widgets.get("source_name")
source_type = dbutils.widgets.get("source_type")

# COMMAND ----------

# ajusta camada
path_main = '/' + 'raw' + '/' + 'bdo'
if source_name in ['biz', 'trs']:
  path_main = '/' + source_name
elif source_name != 'raw':
  path_main = '/' + 'raw' + '/' + source_name
print(path_main)  

# COMMAND ----------

default_dir = '/'.join(['','tmp','dev'])
if env == 'prod':
  default_dir = ""  

# COMMAND ----------

path = "{adl_path}{default_dir}".format(adl_path=var_adls_uri,default_dir=default_dir)
path_source = path + '/'.join(['', 'gov', 'tables', 'source'])
path_schema = path + '/'.join(['', 'gov', 'tables', 'schema'])
path_table  = path + '/'.join(['', 'gov', 'tables', 'table'])
path_field  = path + '/'.join(['', 'gov', 'tables', 'field'])

# COMMAND ----------

env, path, path_source, path_schema, path_table, path_field

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# COMMAND ----------

def normaliza_path(path):
  i = (3 if (path[0:6] == 'abfss:') else 0)
  return '/'.join(path.replace('/','%%/%%').strip().split('%%/%%')[i:]) #.replace("tmp" + "/" + "dev" + '/','')

def get_source(path):
  camada = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')
  final = camada[0]
  if camada[0] == 'raw':
    final = camada[1]
    if final == 'bdo':
      final = camada[0]
  return final

def get_schema(path):
  x = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')
  schema_name = x[1]
  if x[0] == 'trs' and x[1] in ['mtd']:
    schema_name = x[1] + '_' + x[2]   
  if x[0] == 'raw' and x[1] in ['bdo','crw','usr','gov']:
    schema_name = x[2]      
  if x[0] == 'raw' and x[1] in ['usr'] and x[2] in ['sti','uniepro','unigest']:
    schema_name = x[2] + '_' + x[3]   
  return schema_name

def get_table(path):
  x = normaliza_path(path).replace("tmp" + "/" + "dev" + '/','').split('/')
  return (x[len(x) - 1])

# COMMAND ----------

def gera_lista (caminho):  ## recebe o caminho da camada e gera uma lista de esquemas (path completo)
  conjunto = []
  try: lista = dbutils.fs.ls(caminho)
  except: dbutils.notebook.exit("Arquivo não encontrado")
  for item in lista:
    path = item.path
    path_normalizada = normaliza_path(path)
    if path_normalizada in ['trs/mtd/']:
      conjunto = conjunto + gera_lista(path)
      continue
    else:
      conjunto.append(path)
  return conjunto

# COMMAND ----------

def varre(path, path_main): ## recebe o caminho da camada 
  #path_sources = ['/' + 'raw' + '/' + 'bdo', '/' + 'raw' + '/' + 'crw', '/' + 'raw' + '/' + 'usr', '/' + 'trs', '/' + 'biz']
  path_sources = [path_main]
  path_schemas = []
  path_tables = []
  for path_source in path_sources: 
    path_schemas += gera_lista (path + path_source)
  for path_schema in path_schemas:
    path_tables += gera_lista (path_schema)
  return path_tables

# COMMAND ----------

conjunto_tabela = []
path_tables = varre(path, path_main)
for item in path_tables:
  source_name = get_source(item)
  schema_name = get_schema(item)
  table_name = get_table(item)
  table_path = normaliza_path(item)
  
  ####### source_type ##########################
  source_type = 'external' if source_name == 'crw' else 'bigdata'
  
  ####### source_descriptione ##################
  source_description = ''
  if source_name == 'crw': source_description = 'Raw - Crawlers'
  if source_name == 'raw': source_description = 'Raw - Sistemas'
  if source_name == 'usr': source_description = 'Raw - Arquivos'
  if source_name == 'trs': source_description = 'Trusted'
  if source_name == 'biz': source_description = 'Business'
  
  elemento = dict([('source_name',source_name), 
                   ('source_type', source_type),
                   ('source_description', source_description),
                   ('schema_name',schema_name), 
                   ('table_name',table_name), 
                   ('path',table_path)
                  ])   
  conjunto_tabela.append(elemento)
  

# COMMAND ----------

conjunto_campo = []
fields_to_drop = ["dh_inicio_vigencia","dh_ultima_atualizacao_oltp","dh_insercao_trs","kv_process_control",\
                  "dh_insercao_raw","dh_inclusao_lancamento_evento","dh_atualizacao_entrada_oltp","dh_fim_vigencia",\
                  "dh_referencia","dh_atualizacao_saida_oltp","dh_insercao_biz","dh_arq_in","dh_insercao_raw","dh_exclusao_valor_pessoa",\
                  "dh_ultima_atualizacao_oltp","dh_fim_vigencia","dh_insercao_trs","dh_inicio_vigencia","dh_referencia",\
                  "dh_atualizacao_entrada_oltp","dh_atualizacao_saida_oltp","dh_inclusao_lancamento_evento","dh_atualizacao_oltp",\
                  "dh_inclusao_valor_pessoa","dh_primeira_atualizacao_oltp"]  

for item in path_tables:
  source_name = get_source(item)
  schema_name = get_schema(item)
  table_name = get_table(item)
  table_path = normaliza_path(item)
  is_table = 1
  try:
    table = spark.read.format("parquet").load(item)
  except:
    try:
      table = spark.read.format("delta").load(item)
    except:
      is_table = 0
  if is_table == 1:
    for name, type in table.dtypes:
      if name not in fields_to_drop:
        elemento2 = dict([('source_name',source_name), 
                          ('source_type', source_type),
                          ('schema_name',schema_name), 
                          ('table_name',table_name), 
                          ('field_name',name),
                          ('data_type',type)
                         ])
        conjunto_campo.append(elemento2)

# COMMAND ----------

df_table = spark.createDataFrame(conjunto_tabela).filter('lower(table_name) not like ("%bkp%")').orderBy('path')
df_source = df_table.select('source_name','source_type','source_description').distinct().orderBy('path')
df_schema = df_table.select('source_name','source_type','schema_name').distinct().orderBy('path')
df_field = spark.createDataFrame(conjunto_campo).filter('lower(table_name) not like ("%bkp%")').orderBy('source_name','schema_name','table_name','field_name')

# COMMAND ----------

df_source_merge = (
  df_source.withColumnRenamed("source_description", "description")
           .withColumn("cod_data_steward", lit(1))
           .withColumn("created_at",current_timestamp())
           .withColumn("updated_at",current_timestamp())
)

delta_source = DeltaTable.forPath(spark, path_source)

source_set = {
  "source_name"     : "upsert.source_name",
  "source_type"     : "upsert.source_type",
  "description"     : "upsert.description",
  "cod_data_steward": "upsert.cod_data_steward",
  "created_at"      : "upsert.created_at",
  "updated_at"      : "upsert.updated_at"  
}

delta_source \
  .alias("target") \
  .merge(df_source_merge.alias("upsert"), "target.source_name = upsert.source_name") \
  .whenNotMatchedInsert(values=source_set) \
  .execute()

# COMMAND ----------

df_schema_merge = (
  df_schema.withColumn("description", lit(''))
           .withColumn("replica", lit(1))
           .withColumn("cod_data_steward", lit(1))
           .withColumn("created_at",current_timestamp())
           .withColumn("updated_at",current_timestamp())
)

delta_schema = DeltaTable.forPath(spark, path_schema)

schema_set = {
  "source_name"     : "upsert.source_name",
  "source_type"     : "upsert.source_type",
  "schema_name"     : "upsert.schema_name",
  "description"     : "upsert.description",
  "replica"         : "upsert.replica",
  "cod_data_steward": "upsert.cod_data_steward",
  "created_at"      : "upsert.created_at",
  "updated_at"      : "upsert.updated_at"  
}

delta_schema \
  .alias("target") \
  .merge(df_schema_merge.alias("upsert"), "target.source_name = upsert.source_name and \
                                           target.schema_name = upsert.schema_name") \
  .whenNotMatchedInsert(values=schema_set) \
  .execute()

# COMMAND ----------

df_table_merge = (
  df_table.withColumn("description", lit(''))
          .withColumn("chave", lit(''))
          .withColumn("col_version", lit(''))
          .withColumn("tipo_carga", lit(''))
          .withColumn("replica", lit(1))
          .withColumn("dsc_business_subject", lit(''))
          .withColumn("cod_data_steward", lit(1))
          .withColumn("created_at",current_timestamp())
          .withColumn("updated_at",current_timestamp())
)

delta_table = DeltaTable.forPath(spark, path_table)

table_set = {
  "source_name"          : "upsert.source_name",
  "source_type"          : "upsert.source_type",
  "schema_name"          : "upsert.schema_name",
  "table_name"           : "upsert.table_name",
  "description"          : "upsert.description",
  "replica"              : "upsert.replica",
  "path"                 : "upsert.path",
  "chave"                : "upsert.chave",
  "col_version"          : "upsert.col_version",
  "tipo_carga"           : "upsert.tipo_carga",
  "cod_data_steward"     : "upsert.cod_data_steward",
  "dsc_business_subject" : "upsert.dsc_business_subject",
  "created_at"           : "upsert.created_at",
  "updated_at"           : "upsert.updated_at"  
}

delta_table \
  .alias("target") \
  .merge(df_table_merge.alias("upsert"), "target.source_name = upsert.source_name and \
                                          target.schema_name = upsert.schema_name and \
                                          target.table_name  = upsert.table_name") \
  .whenNotMatchedInsert(values=table_set) \
  .execute()

# COMMAND ----------

df_field_merge = (
  df_field.withColumn("data_steward", lit(''))
          .withColumn("login", lit(''))
          .withColumn("description", lit(''))
          .withColumn("personal_data", lit('0'))
          .withColumn("is_derivative", lit(1))
          .withColumn("dsc_business_subject", lit(''))
          .withColumn("cod_data_steward", lit(1))
          .withColumn("ind_relevance", lit(0))
          .withColumn("created_at",current_timestamp())
          .withColumn("updated_at",current_timestamp())
)

delta_field = DeltaTable.forPath(spark, path_field)

field_set = {
  "source_name"          : "upsert.source_name",
  "source_type"          : "upsert.source_type",
  "schema_name"          : "upsert.schema_name",
  "table_name"           : "upsert.table_name",
  "field_name"           : "upsert.field_name",
  "data_type"            : "upsert.data_type",
  "data_steward"         : "upsert.data_steward",
  "login"                : "upsert.login",
  "description"          : "upsert.description",
  "personal_data"        : "upsert.personal_data",
  "is_derivative"        : "upsert.is_derivative",
  "dsc_business_subject" : "upsert.dsc_business_subject",
  "cod_data_steward"     : "upsert.cod_data_steward",
  "ind_relevance"        : "upsert.ind_relevance",
  "created_at"           : "upsert.created_at",
  "updated_at"           : "upsert.updated_at"  
}

delta_field \
  .alias("target") \
  .merge(df_field_merge.alias("upsert"), "target.source_name = upsert.source_name and \
                                          target.schema_name = upsert.schema_name and \
                                          target.table_name  = upsert.table_name and \
                                          target.field_name  = upsert.field_name") \
  .whenNotMatchedInsert(values=field_set) \
  .execute()

# COMMAND ----------

"""
#Pegar o tamanho do path
tabela = dbutils.fs.ls(path + '/' + 'raw' + '/' + 'crw' + '/' + 'ibge' + '/' + 'ipca')
size = 0
print(tabela)
for x in tabela:
  size += x.size
print(size)
"""
