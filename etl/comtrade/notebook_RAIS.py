# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de dados da RAIS para projeto complexo industrial da saúde
# MAGIC
# MAGIC Esse notebook extrai dados da RAIS de modo a calcular o número de vínculos, massa salarial e número de estabelecimentos para as firmas do complexo industrial da saúde. Esses dados são complementados pelo IPCA, com esse último se deflaciona os valores da massa salarial nominal de modo a se obter a massa salarial real.
# MAGIC
# MAGIC Trello: https://trello.com/c/klT2V5te/1229-ceis-sustenta%C3%A7%C3%A3o-do-produto<br>
# MAGIC Especialista: Danilo Severian<br>
# MAGIC Cientista: Beatriz Bonato
# MAGIC
# MAGIC

# COMMAND ----------

# Define livrarias
from pyspark.sql.window import Window
from functools import reduce
from operator import add
import pyspark.sql.functions as f
from pyspark.sql.types import DecimalType, StringType
window = Window.partitionBy()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê os dados
# MAGIC
# MAGIC Le os dados da RAIS e do IPCA

# COMMAND ----------

# Le dados
df_rais_vinculos_bruto = spark.read.table("datalake__trs.oni.oni_mte_rais_identificada__rais_vinculo")
df_rais_estabelecimentos_bruto_ident = spark.read.table("datalake__trs.oni.oni_mte_rais_identificada__rais_estabelecimento")
df_ipca_bruto = spark.read.table("datalake__trs.oni.ibge__ipca")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza dos dados
# MAGIC
# MAGIC Extrai-se o índice fixo para o último ano do IPCA.
# MAGIC
# MAGIC Filtra-se a rais para as cnaes do complexo industrial da saúde e se calcula o número de vinculos, massa salarial e número de estabelecimentos, por ano, município e CNAE.
# MAGIC
# MAGIC Une-se a base calculada a partir da RAIS com o índice fixo do IPCA e então calcula-se a massa salarial REAL
# MAGIC

# COMMAND ----------

# Definidno IPCA
df_ipca = df_ipca_bruto \
  .where((f.col('DS_MES_COMPETENCIA')==f.lit("dezembro")) & (f.col('NR_ANO') >= 2008)) \
  .orderBy("NR_ANO") \
  .withColumn('base_fixa',f.last(f.col('VL_IPCA')).over(window)) \
  .withColumn('INDICE_FIXO_ULTIMO_ANO',f.col('VL_IPCA')/f.col('base_fixa')) \
  .select('NR_ANO','INDICE_FIXO_ULTIMO_ANO') \
  .withColumnRenamed('NR_ANO', 'ANO')

# Estabelece ano base como 2015
valor_2015 = df_ipca.where(f.col('ANO') == 2015).select('INDICE_FIXO_ULTIMO_ANO').collect()[0]["INDICE_FIXO_ULTIMO_ANO"]

df_ipca = df_ipca \
  .withColumn('INDICE_2015', f.col('INDICE_FIXO_ULTIMO_ANO')/valor_2015) \
  .drop('INDICE_FIXO_ULTIMO_ANO')

# COMMAND ----------

# Limpa dados para setores específicos entre 2008 e ano mais recente
df_rais_estabelecimentos_tipicos = df_rais_estabelecimentos_bruto_ident \
  .where(f.col('CD_CNAE20_SUBCLASSE').isin([2110600, 2121101, 2121102, 2121103, 2123800, 2660400, 3250701, 3250702, 3250703, 3250704, 3250705, 3250707, 3292202])) \
  .where((f.col('FL_IND_RAIS_NEGAT') == 0)) \
  .where((f.col('NR_ANO') >= 2008)) \
  .groupby(f.col("NR_ANO"), f.col('CD_MUNICIPIO'), f.col('CD_CNAE20_SUBCLASSE')) \
  .count() \
  .withColumnRenamed('count', 'estabelecimentos') \
  .withColumnRenamed('NR_ANO', 'ANO') \
  .withColumn('CD_CNAE20_SUBCLASSE', f.col('CD_CNAE20_SUBCLASSE').cast(StringType()))

# Limpa dados para setores atípicos entre 2008 e ano mais recente
df_rais_estabelecimentos_atipicos = df_rais_estabelecimentos_bruto_ident \
  .where(f.col('ID_CNPJ_CEI').isin(['00394429009914', '00394502007157', '17503475000101', '33485939000142', '33781055000135', '61189445000156', '77964393000188'])) \
  .where((f.col('FL_IND_RAIS_NEGAT') == 0)) \
  .where((f.col('NR_ANO') >= 2008)) \
  .groupby(f.col("NR_ANO"), f.col('CD_MUNICIPIO'), f.col('CD_CNAE20_SUBCLASSE')) \
  .count() \
  .withColumnRenamed('count', 'estabelecimentos') \
  .withColumnRenamed('NR_ANO', 'ANO') \
  .withColumn('CD_CNAE20_SUBCLASSE', f.col('CD_CNAE20_SUBCLASSE').cast(StringType()))

# COMMAND ----------

df_tipicos = df_rais_vinculos_bruto \
  .where(f.col('CD_CNAE20_SUBCLASSE').isin([2110600, 2121101, 2121102, 2121103, 2123800, 2660400, 3250701, 3250702, 3250703, 3250704, 3250705, 3250707, 3292202])) \
  .where((f.col('FL_VINCULO_ATIVO_3112') == 1)) \
  .where(f.col('ANO') >= 2008) \
  .withColumn('renda_total_ano', f.when(f.col('NR_MES_TEMPO_EMPREGO') > 12, f.col('VL_REMUN_MEDIA_NOM') * 12 ).otherwise(f.col('VL_REMUN_MEDIA_NOM') * f.ceil(f.col('NR_MES_TEMPO_EMPREGO')))) \
  .groupby(f.col("ANO"), f.col('CD_MUNICIPIO'), f.col('CD_CNAE20_SUBCLASSE')) \
  .agg(f.sum('renda_total_ano').alias("massa_salarial_total"), 
       f.count('*').alias('vinculos_3112')) \
  .join(df_rais_estabelecimentos_tipicos, on = ['ANO', 'CD_MUNICIPIO', 'CD_CNAE20_SUBCLASSE'], how = 'full') \
  .withColumn('setor', f.when(f.col('CD_CNAE20_SUBCLASSE').isin([2110600, 2121101, 2121102, 2121103, 2123800]), f.lit('Indústria de base química e biotecnológica')).otherwise(f.lit('Indústria de base mecânica, eletrônica e de materiais'))) \
  .join(df_ipca, on=['ANO'] , how = 'left') \
  .fillna(0, ['vinculos_3112', 'massa_salarial_total', 'estabelecimentos']) \
  .withColumn('massa_salarial_total_real', f.col('massa_salarial_total')/f.col('INDICE_2015')) 

# COMMAND ----------

df_atipicos = df_rais_vinculos_bruto \
  .where(f.col('ID_CNPJ_CEI').isin(['00394429009914', '00394502007157', '17503475000101', '33485939000142', '33781055000135', '61189445000156', '77964393000188'])) \
  .where((f.col('FL_VINCULO_ATIVO_3112') == 1)) \
  .where(f.col('ANO') >= 2008) \
  .withColumn('renda_total_ano', f.when(f.col('NR_MES_TEMPO_EMPREGO') > 12, f.col('VL_REMUN_MEDIA_NOM') * 12 ).otherwise(f.col('VL_REMUN_MEDIA_NOM') * f.ceil(f.col('NR_MES_TEMPO_EMPREGO')))) \
  .groupby(f.col("ANO"), f.col('CD_MUNICIPIO'), f.col('CD_CNAE20_SUBCLASSE')) \
  .agg(f.sum('renda_total_ano').alias("massa_salarial_total"), 
       f.count('*').alias('vinculos_3112')) \
  .join(df_rais_estabelecimentos_atipicos, on = ['ANO', 'CD_MUNICIPIO', 'CD_CNAE20_SUBCLASSE'], how = 'full') \
  .withColumn('setor', f.when(f.col('CD_CNAE20_SUBCLASSE').isin([2110600, 2121101, 2121102, 2121103, 2123800]), f.lit('Indústria de base química e biotecnológica')).otherwise(f.lit('Indústria de base química e biotecnológica'))) \
  .join(df_ipca, on=['ANO'] , how = 'left') \
  .fillna(0, ['vinculos_3112', 'massa_salarial_total', 'estabelecimentos']) \
  .withColumn('massa_salarial_total_real', f.col('massa_salarial_total')/f.col('INDICE_2015')) 

# COMMAND ----------

# Empilha os DataFrames
df = df_tipicos.union(df_atipicos)

# COMMAND ----------

# Filtra dados para ANO > 2007
df = df.where(f.col('ANO') > 2007)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva resultados

# COMMAND ----------

# Salvando resultados
path_resultado_parquet = "/Volumes/oni_lab/default/uds_oni_observatorio_nacional/complexo_industrial_saude/rais"
df.coalesce(1).write.parquet(path_resultado_parquet, mode='overwrite')