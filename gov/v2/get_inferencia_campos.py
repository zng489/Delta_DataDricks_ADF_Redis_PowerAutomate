# Databricks notebook source
dbutils.widgets.text("env","dev") #tmp ou prod
dbutils.widgets.text("path","uds")
env = dbutils.widgets.get("env")
var_path = dbutils.widgets.get("path")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, var_path, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

# captura os arquivos a partir do path informado, varrendo todas as
# subpastas e retornando uma lista com o path do arquivo e o tamanho em bytes
def get_tables (path): 
  conjunto = []
  try: lista = dbutils.fs.ls(path)
  except: dbutils.notebook.exit("Path não encontrado: " + path)
  for item in lista:
    tipo = 'file'
    if item.name[-1] == '/':
      tipo = 'folder'
      if item.name.find('=') >= 0:
        tipo = 'partition'
    if tipo != 'folder':
      array = normaliza_path(item.path).split('/')[:-1]
      p = '/'.join(array)
      t = array[-1]
      dicio = dict([  ('path', p),
                      ('tabela', t),
                   ])
      if not dicio in conjunto:
        conjunto.append(dicio)
    else:
      conjunto += get_tables (item.path)
  return conjunto

# COMMAND ----------

conjunto = []
conjunto += get_tables(path_read)
conjunto

# COMMAND ----------

conjunto_campo = []
fields_to_drop = ["dh_inicio_vigencia","dh_ultima_atualizacao_oltp","dh_insercao_trs","kv_process_control",\
                  "dh_insercao_raw","dh_inclusao_lancamento_evento","dh_atualizacao_entrada_oltp","dh_fim_vigencia",\
                  "dh_referencia","dh_atualizacao_saida_oltp","dh_insercao_biz","dh_arq_in","dh_insercao_raw","dh_exclusao_valor_pessoa",\
                  "dh_ultima_atualizacao_oltp","dh_fim_vigencia","dh_insercao_trs","dh_inicio_vigencia","dh_referencia",\
                  "dh_atualizacao_entrada_oltp","dh_atualizacao_saida_oltp","dh_inclusao_lancamento_evento","dh_atualizacao_oltp",\
                  "dh_inclusao_valor_pessoa","dh_primeira_atualizacao_oltp"]  
index = 0
for item in conjunto:
  tabela     = item['tabela']
  path       = item['path']
  is_table = 1
  try:
    table = spark.read.format("parquet").load('/'.join([var_adls_uri,path])).limit(1000)
  except:
    try:
      table = spark.read.format("delta").load('/'.join([var_adls_uri,path])).limit(1000)
    except:
      is_table = 0
  if is_table == 1:
    index += 1
    for name, type in table.dtypes:
      if name not in fields_to_drop:
        elemento2 = dict([('path',path), 
                          ('indice',index),
                          ('tabela',tabela), 
                          ('campo',name),
                          ('campo_tipo',type)
                         ])
        conjunto_campo.append(elemento2)

# COMMAND ----------

df_campo = spark.createDataFrame(conjunto_campo) \
.select(
   col('*')
  ,concat(col('indice')
          , expr('case when substr(path,0,3) = "raw" then 1 \
                       when substr(path,0,3) = "trs" then 2 \
                       when substr(path,0,3) = "biz" then 3 \
                       else 4 end') \
          , lit('-'), expr('regexp_replace(path,"/","-")') \
         ).alias('ordem') \
) \
.select('path','ordem','tabela','campo','campo_tipo')

# COMMAND ----------

df_merge = (
  df_campo
  .withColumn('dt_criacao', current_timestamp())
  .withColumn('dt_atualizacao', current_timestamp())
  .withColumn('ativo', lit(1))
  .withColumn('campo_ds', lit(''))
  .withColumn('cLivre1', lit(''))
  .withColumn('cLivre2', lit(''))
  .withColumn('cLivre3', lit(''))
  .withColumn('cLivre4', lit(''))
  .withColumn('cLivre5', lit(''))
)

# COMMAND ----------

df_delta = DeltaTable.forPath(spark, path_campo)

# COMMAND ----------

var_set = {
  'path'           : 'upsert.path',
  'ordem'          : 'upsert.ordem',
  'tabela'         : 'upsert.tabela',
  'campo'          : 'upsert.campo',
  'campo_tipo'     : 'upsert.campo_tipo',
  'dt_criacao'     : 'upsert.dt_criacao',
  'dt_atualizacao' : 'upsert.dt_atualizacao',
  'ativo'          : 'upsert.ativo',
  'campo_ds'       : 'upsert.campo_ds',
  'cLivre1'        : 'upsert.cLivre1',
  'cLivre2'        : 'upsert.cLivre2',
  'cLivre3'        : 'upsert.cLivre3',
  'cLivre4'        : 'upsert.cLivre4',
  'cLivre5'        : 'upsert.cLivre5'
}

# COMMAND ----------

df_delta.alias("target") \
.merge(df_merge.alias("upsert"), "target.path = upsert.path") \
.whenNotMatchedInsert(values=var_set) \
.execute()
