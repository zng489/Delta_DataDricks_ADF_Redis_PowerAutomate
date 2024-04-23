# Databricks notebook source
dbutils.widgets.text("env","dev") #tmp ou prod
env = dbutils.widgets.get("env")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

esquema = DeltaTable.forPath(spark, path_esquema).toDF()
tabela = DeltaTable.forPath(spark, path_tabela).toDF()
campo = DeltaTable.forPath(spark, path_campo).toDF()
size = DeltaTable.forPath(spark, path_size).toDF()
contrato = DeltaTable.forPath(spark, path_dh + '/'.join(['', 'contrato'])).toDF()

# COMMAND ----------

estrutura = DeltaTable.forPath(spark, path_dh + '/'.join(['', 'estrutura'])).toDF()
estrutura = last_version(estrutura, 'ID_AGRUP_UNIDADE', 'UNIDADE_AREA_ANO_MES_REFERENCIA')
estrutura = last_version(estrutura, 'ID_AGRUP_AREA', 'AREA_SEGMENTO_ANO_MES_REFERENCIA')
estrutura = estrutura.withColumn('UNIDADE_AREA_EXTINCAO2', expr('case when UNIDADE_AREA_EXTINCAO is null then current_timestamp() else UNIDADE_AREA_EXTINCAO end'))
estrutura = last_version(estrutura, 'ID_CONTRATO_UNIDADE', 'UNIDADE_AREA_EXTINCAO2')

df_estrutura = last_version (estrutura, 'id_contrato_unidade', 'unidade_area_ano_mes_referencia')
df_estrutura = last_version (df_estrutura, 'id_agrup_area','area_segmento_ano_mes_referencia')
df_estrutura = df_estrutura.select('id_contrato_unidade','area_desc','segmento_desc').distinct()

# COMMAND ----------

df_contrato = last_version (contrato, 'ID_CONTRATO', 'ANO_MES_REFERENCIA')
df_contrato = df_contrato.filter("tipo_contrato in ('0001','0002')")
df_contrato = df_contrato.select('e_mail','pessoa_nome','id_contrato_unidade')
df_contrato = df_contrato.distinct()

# COMMAND ----------

df_pessoa = df_contrato.join(df_estrutura,['id_contrato_unidade'],'inner').drop('id_contrato_unidade')

# COMMAND ----------

dt_max = size.select(max ('created_at')).collect()[0].__getitem__(0).strftime("%Y-%m-%d")
df_size = size\
.filter('created_at = "' + dt_max + '"')\
.withColumn("path", df_get_table_level(col("path")))

df_size = df_size.groupBy("path").agg(sum("size").alias("tabela_bytes"))

# COMMAND ----------

df_esquema = esquema\
.filter('ativo = true')\
.select(
   col('path_esquema')
  ,col('esquema_ds')
)

# COMMAND ----------

df_tabela = tabela\
.filter('ativo = true')\
.join(df_esquema,['path_esquema'],'inner')\
.select(
   col('path')
  ,col('camada')
  ,col('camada_ds')
  ,col('esquema')
  ,col('esquema_ds')  
  ,col('tabela')
  ,col('niveis').alias('tabela_niveis')
  ,col('ordem').alias('tabela_ordem')
  ,col('dt_atualizacao').alias('tabela_atualizacao')
  ,col('tabela_ds')
  ,col('eixo').alias('tabela_tema')
  ,col('login_datasteward')
  ,col('login_dataowner')  
)

df_tabela = df_tabela\
.join(df_size, ['path'], 'left')

#df_tabela.count()

# COMMAND ----------

df_campo = campo\
.filter('ativo = true')\
.select(
   col('path')
  ,col('campo')
  ,col('campo_ds')
  ,col('campo_tipo')  
  ,col('tema').alias('campo_tema')
  ,col('ind_relevancia').alias('campo_ind_relevancia')
  ,col('dado_pessoal').alias('campo_dado_pessoal')
  ,col('dt_atualizacao').alias('campo_atualizacao')  
)

#df_campo.count()

# COMMAND ----------

df_tabela_pessoa = df_tabela\
.join(df_pessoa, df_tabela.login_datasteward == df_pessoa.e_mail, 'left')\
.select (
   col('*')
  ,col('pessoa_nome').alias('datasteward')
)\
.drop('pessoa_nome').drop('e_mail').drop('area_desc').drop('segmento_desc')

#df_tabela_pessoa.count()

# COMMAND ----------

df_tabela_pessoa = df_tabela_pessoa\
.join(df_pessoa, df_tabela.login_dataowner == df_pessoa.e_mail, 'left')\
.select (
   col('*')
  ,col('pessoa_nome').alias('dataowner')
)\
.drop('pessoa_nome').drop('e_mail')

#df_tabela_pessoa.count()

# COMMAND ----------

df_tabela_pessoa_campo = df_tabela_pessoa\
.join(df_campo, ['path'], 'inner')\
.select (
   col('*')
  ,expr('case when dataowner = "" then 0 \
              when dataowner is null then 0 \
              else 1 end') \
        .alias('tem_dataowner')
  ,expr('case when datasteward = "" then 0 \
              when datasteward is null then 0 \
              else 1 end') \
        .alias('tem_datasteward')  
  ,expr('case when esquema_ds = "" then 0 \
              when esquema_ds is null then 0 \
              else 1 end') \
        .alias('esquema_preenchido')  
  ,expr('case when tabela_ds = "" then 0 \
              when tabela_ds is null then 0 \
              else 1 end') \
        .alias('tabela_preenchida')  
  ,expr('case when campo_ds = "" then 0 \
              when campo_ds is null then 0 \
              else 1 end') \
        .alias('campo_preenchido') 
  ,expr('case when campo_dado_pessoal = "" then 0 \
              when campo_dado_pessoal is null then 0 \
              else 1 end') \
        .alias('campo_dado_pessoal_preenchido') 
  ,expr('case when campo_ind_relevancia = "" then 0 \
              when campo_ind_relevancia is null then 0 \
              else 1 end') \
        .alias('campo_ind_relevancia_preenchido') 
)  

# COMMAND ----------

tabela.count(), df_tabela.count(), df_tabela_pessoa.count(), df_campo.count(), df_tabela_pessoa_campo.count()

# COMMAND ----------

df_catalogo = df_tabela_pessoa_campo\
.select(
   col('path')
  ,col('camada')
  ,col('camada_ds')
  ,col('esquema')
  ,col('esquema_ds')
  ,col('tabela')
  ,col('tabela_ds')
  ,col('tabela_tema')
  ,col('tabela_atualizacao')
  ,col('tabela_niveis')
  ,col('tabela_ordem')
  ,col('area_desc').alias('area')
  ,col('segmento_desc').alias('segmento')
  ,col('dataowner')
  ,col('datasteward')
  ,col('campo')
  ,col('campo_ds')
  ,col('campo_tipo')
  ,col('campo_tema')
  ,col('campo_ind_relevancia')
  ,col('campo_dado_pessoal')
  ,col('campo_atualizacao')
  ,col('tem_dataowner')
  ,col('tem_datasteward')
  ,col('esquema_preenchido')
  ,col('tabela_preenchida')
  ,col('campo_preenchido')
  ,col('campo_dado_pessoal_preenchido')
  ,col('campo_ind_relevancia_preenchido')
  ,col('tabela_bytes')
)\
.withColumn('tabela_megabytes', round(df_tabela_pessoa_campo['tabela_bytes']/1024/1024,3)) \
.withColumn('tabela_gigabytes', round(df_tabela_pessoa_campo['tabela_bytes']/1024/1024/1024,3)) \
.withColumn('tabela_terabytes', round(df_tabela_pessoa_campo['tabela_bytes']/1024/1024/1024/1024,3))

# COMMAND ----------

df_catalogo.write.format("parquet").mode("overwrite").save(path_catalogo + '/'.join(['','parquet']))
