# Databricks notebook source
# MAGIC %run ./__libs_xls

# COMMAND ----------

dbutils.widgets.text("env","dev") #tmp ou prod
dbutils.widgets.text("source_type","tabela")
env = dbutils.widgets.get("env")
source_type = dbutils.widgets.get("source_type")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

caminho_csv = 'erro'
if   source_type == 'tabela':
  caminho_csv = path_tabela
elif source_type == 'esquema':
  caminho_csv = path_esquema
elif source_type == 'campo':
  caminho_csv = path_campo

df = DeltaTable.forPath(spark, caminho_csv).toDF()

# COMMAND ----------

if   source_type == 'tabela':
  df = df\
  .select(
    col('path').alias('path_tabela'),
    col('camada'),
    col('ativo'),
    col('tabela_ds'),
    col('login_datasteward'),
    col('login_dataowner'),
    col('eixo'),    
    col('cLivre1'), 
    col('cLivre2')    
  )
elif source_type == 'esquema':
  df = df\
  .select(
    col('path_esquema'),
    col('esquema'),
    col('camada'),
    col('ativo'),
    col('esquema_ds')
  )
elif source_type == 'campo':
  df = df\
  .select(
    col('path').alias('path_campo'),
    col('tabela'),
    col('campo'),
    col('campo_tipo'),
    col('ativo'),
    col('campo_ds'),
    col('ind_relevancia'),
    col('dado_pessoal'),
    col('tema'),    
    col('cLivre1'), 
    col('cLivre2')
  )


# COMMAND ----------

path_write = path + '/'.join(['csv',source_type + '.xlsx'])
path_write

panda_df = df.toPandas()

from openpyxl.utils.dataframe import dataframe_to_rows
wb = Workbook()
ws = wb.active
for r in dataframe_to_rows(panda_df, index=False, header=True):
    ws.append(r)
	
SaveExcelToLake(wb, path_write)	
