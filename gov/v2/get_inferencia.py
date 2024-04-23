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
      niveis = path_niveis(p)
      dicio = dict([  ('path', p),
                      ('niveis', len(array)),
                      ('camada', niveis[0]),
                      ('esquema', niveis[1]),
                      ('tabela', niveis[2]),
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

df_tabela = spark.createDataFrame(conjunto) \
.select(
   col('*')
  ,concat(expr('case when substr(path,0,3) = "raw" then 1 \
                     when substr(path,0,3) = "trs" then 2 \
                     when substr(path,0,3) = "biz" then 3 \
                    else 4 end') \
         , lit('-'), expr('regexp_replace(path,"/","-")') \
        ).alias('ordem') \
  ,expr('case when camada = "raw-bdo" then "Raw - Sistemas" \
              when camada = "raw-crw" then "Raw - Crawlers" \
              when camada = "raw-usr" then "Raw - Arquivos" \
              when camada = "trs"     then "Trusted" \
              when camada = "biz"     then "Business" \
              else "-" end' \
       ).alias('camada_ds') \
  ,expr('regexp_replace(camada,"-","/")').alias("path_camada")
  ,concat( expr('regexp_replace(camada,"-","/")'), lit('/'), expr('regexp_replace(esquema,"-","/")') ).alias("path_esquema")
) \
.select('path','path_camada','path_esquema','ordem','niveis','camada','esquema','tabela','camada_ds')

# COMMAND ----------

df_esquema = df_tabela \
.select(
   col('path_esquema')
  ,concat( expr('case when substr(path_camada,0,3) = "raw" then 1 \
               when substr(path_camada,0,3) = "trs" then 2 \
               when substr(path_camada,0,3) = "biz" then 3 \
               else 4 end') \
           ,lit('-'), expr('regexp_replace(path_esquema,"/","-")') \
   ).alias('ordem')
  ,col('esquema')
  ,col('path_camada')
  ,col('camada') 
) \
.distinct()

# COMMAND ----------

df_merge = (
  df_esquema 
  .withColumn('esquema_ds', lit(''))
  .withColumn('esquema_tx', lit(''))
  .withColumn('cLivre1', lit(''))
  .withColumn('cLivre2', lit(''))
  .withColumn('cLivre3', lit(''))
  .withColumn('cLivre4', lit(''))
  .withColumn('cLivre5', lit(''))
  .withColumn('dt_criacao', current_timestamp())
  .withColumn('dt_atualizacao', current_timestamp())
  .withColumn('ativo', lit(1))
)

# COMMAND ----------

var_set = {
  'path_esquema'   : 'upsert.path_esquema',
  'ordem'          : 'upsert.ordem',
  'esquema'        : 'upsert.esquema',
  'path_camada'    : 'upsert.path_camada',
  'camada'         : 'upsert.camada',
  'esquema_ds'     : 'upsert.esquema_ds',
  'esquema_tx'     : 'upsert.esquema_tx',
  'cLivre1'        : 'upsert.cLivre1',
  'cLivre2'        : 'upsert.cLivre2',
  'cLivre3'        : 'upsert.cLivre3',
  'cLivre4'        : 'upsert.cLivre4',
  'cLivre5'        : 'upsert.cLivre5',
  'dt_criacao'     : 'upsert.dt_criacao',
  'dt_atualizacao' : 'upsert.dt_atualizacao',
  'ativo'          : 'upsert.ativo'
}

# COMMAND ----------

df_delta = DeltaTable.forPath(spark, path_esquema)

# COMMAND ----------

df_delta.alias("target") \
.merge(df_merge.alias("upsert"), "target.path_esquema = upsert.path_esquema") \
.whenNotMatchedInsert(values=var_set) \
.execute()

# COMMAND ----------

df_merge = (
  df_tabela
  .withColumn('chave', lit(''))
  .withColumn('col_version', lit(''))
  .withColumn('tipo_carga', lit(''))
  .withColumn('dt_criacao', current_timestamp())
  .withColumn('dt_atualizacao', current_timestamp())
  .withColumn('ativo', lit(1))
  .withColumn('login_datasteward', lit(''))
  .withColumn('login_dataowner', lit(''))
  .withColumn('tabela_ds', lit(''))
  .withColumn('tabela_tx', lit(''))
  .withColumn('acesso_default', lit(0))
  .withColumn('eixo', lit(''))
  .withColumn('base', lit(''))
  .withColumn('base_tx', lit(''))
  .withColumn('cLivre1', lit(''))
  .withColumn('cLivre2', lit(''))
  .withColumn('cLivre3', lit(''))
  .withColumn('cLivre4', lit(''))
  .withColumn('cLivre5', lit(''))
)

# COMMAND ----------

var_set = {
  'path'              : 'upsert.path',
  'path_camada'       : 'upsert.path_camada',
  'path_esquema'      : 'upsert.path_esquema',
  'ordem'             : 'upsert.ordem',
  'niveis'            : 'upsert.niveis',
  'tabela'            : 'upsert.tabela',
  'esquema'           : 'upsert.esquema',
  'camada'            : 'upsert.camada',
  'camada_ds'         : 'upsert.camada_ds',
  'chave'             : 'upsert.chave',
  'col_version'       : 'upsert.col_version',
  'tipo_carga'        : 'upsert.tipo_carga',
  'dt_criacao'        : 'upsert.dt_criacao',
  'dt_atualizacao'    : 'upsert.dt_atualizacao',
  'ativo'             : 'upsert.ativo',
  'login_datasteward' : 'upsert.login_datasteward',
  'login_dataowner'   : 'upsert.login_dataowner',
  'tabela_ds'         : 'upsert.tabela_ds',
  'tabela_tx'         : 'upsert.tabela_tx',
  'eixo'              : 'upsert.eixo',
  'base'              : 'upsert.base',
  'base_tx'           : 'upsert.base_tx',
  'cLivre1'           : 'upsert.cLivre1',
  'cLivre2'           : 'upsert.cLivre2',
  'cLivre3'           : 'upsert.cLivre3',
  'cLivre4'           : 'upsert.cLivre4',
  'cLivre5'           : 'upsert.cLivre5' 
}

# COMMAND ----------

df_delta = DeltaTable.forPath(spark, path_tabela)

# COMMAND ----------

df_delta.alias("target") \
.merge(df_merge.alias("upsert"), "target.path = upsert.path") \
.whenNotMatchedInsert(values=var_set) \
.execute()
