# Databricks notebook source
dbutils.widgets.text("env","dev") #tmp ou prod
env = dbutils.widgets.get("env")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

path_report_oni = path_read + '/'.join(['uds','gov_oni','v2','adls_report'])
path_report_oni

# COMMAND ----------

path = path_adls_report + '/'.join(['','parquet'])
print(path)
report = spark.read.format("parquet").load(path)

# COMMAND ----------

remove_grupo_users = ['"ANALISTA_STI_INTELIGENCIA"','"DESENV_SUSTENTACAO_STI"','"GB-ASC-ACTI-BigData-RW"','"staff-bsb-bigdata"', '"OBSERVATORIO_FIEPE"', '"GB-ASC-ACTI-KEYRUS"']
filtro_remove_grupo_users = 'nome not in (' + ','.join(remove_grupo_users) + ')'

# COMMAND ----------

remove_users_app = ['"cnibigdataapp"','"cnibigdatafactory"','"exec-bigdata01"', '"exec-bigdata02"'] 
filtro_remove_users_app = 'nome not in (' + ','.join(remove_users_app) + ')'
print(filtro_remove_users_app)

# COMMAND ----------

# adls_report_observatorio
report_oni = report\
.select(
  col('path'),
  col('nome'),
  col('id').alias('id_aad'),
  col('hierarquia').alias('grupos'),
  col('permissao'),
  col('data')
)\
.filter('tipo = "u"')\
.filter('id is not null')\
.filter('permissao = "r-x"')\
.filter('pasta_1 in ("raw","trs","biz")')\
.filter('path not in ("raw","raw/crw","raw/bdo","raw/usr","raw/gov","trs","biz")')\
.filter('path not in ("raw/bdo/gestaorh","trs/dh","biz/dh")')\
.filter('hierarquia not in ("GB-ASC-ACTI-KEYRUS","ANALISTA_STI_INTELIGENCIA","DESENV_SUSTENTACAO_STI","GB-ASC-ACTI-BigData-RW")')\
.filter(filtro_remove_grupo_users)\
.filter(filtro_remove_users_app)\
.orderBy('path')

# COMMAND ----------

report_oni_final = last_version(report_oni,'path,id_aad','data')

# COMMAND ----------

report_oni_final.write.format("parquet").mode("overwrite").save(path_report_oni)

# COMMAND ----------

path_report_oni
