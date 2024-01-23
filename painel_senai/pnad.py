# Databricks notebook source
# importa bibliotecas
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
window = Window.partitionBy()

# Define caminhos
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

## PNAD
path_pnadc='{uri}/trs/ibge/pnadc/'.format(uri=var_adls_uri)

## UF
path_uf='{uri}/trs/oni/ibge/uf/'.format(uri=var_adls_uri)

# COMMAND ----------

# Leitura dos dados
## PNAD
df_pnadc = spark.read.parquet(path_pnadc)
df_uf = spark.read.parquet(path_uf).select('cd_uf','ds_uf').withColumnRenamed('cd_uf','UF')

# COMMAND ----------

# Limpeza da base
VD3004_index = ['1','2','3','4','5','6','7']

VD3004_conteudo=['Sem instrução e menos de 1 ano de estudo','Fundamental incompleto ou equivalente','Fundamental completo ou equivalente','Médio incompleto ou equivalente','Médio completo ou equivalente','Superior incompleto ou equivalente','Superior completo']
# define the schema for the DataFrame
VD3004_schema = StructType([
    StructField("VD3004", StringType(), True),
    StructField("ds_educacao_nivel", StringType(), True)
])

data_VD3004 = zip(VD3004_index, VD3004_conteudo)
df_VD3004 = spark.createDataFrame(data_VD3004, schema=VD3004_schema)

# Define jovens
df = df_pnadc \
  .withColumn('ds_faixa_etaria', f.when((f.col('V2009')>=f.lit(15)) & (f.col('V2009')<=f.lit(17)), f.lit('Entre 15 e 17 anos')).otherwise(
    f.when((f.col('V2009')>=f.lit(18)) & (f.col('V2009')<=f.lit(24)), f.lit('Entre 18 e 24 anos')).otherwise(
      f.when((f.col('V2009')>=f.lit(25)) & (f.col('V2009')<=f.lit(29)), f.lit('Entre 25 e 29 anos')).otherwise(
        f.lit('Não é jovem'))))) \
    .join(df_VD3004, 'VD3004', 'left') \
    .join(df_uf, 'UF', 'left')

# Define Janela
window = Window.partitionBy('ANO','TRIMESTRE','ds_uf')

# COMMAND ----------

# Define funções de avaliação PNAD
## Avalia trabalho
def aval_trab(df, janela):
  df_final = df \
  .withColumn("vl_informais", f.when((f.col("VD4002") == f.lit(1)) & ((f.col("VD4009").isin([2, 4, 6, 10])) | ((f.col("VD4009").isin([8, 9])) & (f.col('VD4012') == f.lit(2)))), f.col('V1028')).otherwise(f.lit(0))) \
  .withColumn("vl_desocupados", f.when(f.col("VD4002") == f.lit(2), f.col("V1028")).otherwise(f.lit(0))) \
  .withColumn('vl_nem_nem', f.when(((f.col("VD4002") == f.lit(2)) | (f.col("VD4001")==f.lit(2))) & (f.col("V3002")==f.lit(2)), f.col("V1028")).otherwise(f.lit(0))) \
  .withColumn("vl_forca_trabalho",f.when(f.col("VD4001")==f.lit(1),f.col("V1028")).otherwise(f.lit(0))) \
  .groupBy('ANO','TRIMESTRE','ds_uf','ds_faixa_etaria').agg(
    f.sum('vl_informais').alias('vl_informais'),
    f.sum('vl_desocupados').alias('vl_desocupados'),
    f.sum('vl_forca_trabalho').alias('vl_forca_trabalho'),
    f.round((f.sum('vl_desocupados')/f.sum('vl_forca_trabalho')) * 100, 2).alias('tx_desocupacao'),
    f.sum('vl_nem_nem').alias('vl_nem_nem'),
    f.sum('V1028').alias('vl_total_jovens')) \
  .where(f.col('ds_faixa_etaria')!=f.lit('Não é jovem')) \
  .withColumn('vl_forca_trabalho_total', f.sum('vl_forca_trabalho').over(janela)) \
  .withColumn('vl_participacao_jovem_forca_trabalho', f.round((f.col('vl_forca_trabalho')/f.sum('vl_forca_trabalho_total').over(janela)) * 100, 2)) \
  .withColumn('tx_nem_nem',f.round((f.col('vl_nem_nem')/f.col('vl_total_jovens')) * 100, 2))

  return(df_final)

## Avalia educação
def aval_educ(df):
  df_final = df \
    .withColumn('vl_nem_nem',f.when(((f.col("VD4002") == f.lit(2)) | (f.col("VD4001") == f.lit(2))) & (f.col("V3002") == f.lit(2)), f.col("V1028")).otherwise(f.lit(0))) \
    .withColumn('vl_estuda', f.when(f.col('V3002') == f.lit(1), f.col('V1028')).otherwise(f.lit(0))) \
    .withColumn('vl_nao_estuda', f.when(f.col('V3002') == f.lit(2), f.col('V1028')).otherwise(f.lit(0))) \
    .withColumn("vl_pessoas_desocupadas",f.when(f.col("VD4002") == f.lit(2),f.col("V1028")).otherwise(f.lit(0))) \
    .groupBy('ANO','TRIMESTRE','ds_uf','ds_faixa_etaria','ds_educacao_nivel') \
    .agg(
      f.sum('V1028').alias('vl_total_jovens'),
      f.sum('vl_nem_nem').alias('vl_nem_nem'),
      f.sum('vl_pessoas_desocupadas').alias('vl_pessoas_desocupadas'),
      f.sum('vl_estuda').alias('vl_estuda'),
      f.sum('vl_nao_estuda').alias('vl_nao_estuda'))
    
  return(df_final)


# COMMAND ----------

# Indicadores de mercado de trabalho

## Jovens
### Nível brasil
df_faixas_br = df \
  .withColumn('ds_uf', f.lit('BR')) 
df_faixas_br = aval_trab(df_faixas_br, janela = window)

### Nível UF
df_faixas_uf = aval_trab(df, janela = window) 

### União das duas bases
df_faixas = df_faixas_br.union(df_faixas_uf).orderBy(f.desc('ANO'),f.desc('TRIMESTRE'),f.desc('ds_faixa_etaria'), f.desc('ds_uf'))

## 18 anos
### Nível brasil
df_faixas_br_18= df \
  .where(f.col('V2009') == f.lit(18)) \
  .withColumn('ds_uf', f.lit('BR')) \

df_faixas_br_18 = aval_trab(df_faixas_br_18, janela = window)

### Nível UF
df_faixas_uf_18 = df.where(f.col('V2009')==f.lit(18))
df_faixas_uf_18 = aval_trab(df_faixas_uf_18, janela = window)

### União das duas bases
df_faixas_final = df_faixas.union(df_faixas_br_18).union(df_faixas_uf_18)

# Calcula indicadores
indicadores_educ = df \
  .where((f.col('ANO')==f.lit(2021)) & (f.col('TRIMESTRE')==f.lit(4)) & (f.col('ds_faixa_etaria')==f.lit('Entre 18 e 24 anos'))) \
  .withColumn('vl_nem_nem',f.when(((f.col("VD4002")==f.lit(2)) | (f.col("VD4001")==f.lit(2))) & (f.col("V3002")==f.lit(2)), f.col("V1028")).otherwise(f.lit(0))) \
  .groupBy('ANO','TRIMESTRE','ds_educacao_nivel') \
    .agg(f.sum('vl_nem_nem').alias('vl_nem_nem'),
         f.sum('V1028').alias('vl_total_jovens'),
         f.round((f.sum('vl_nem_nem')/f.sum('V1028')) * 100,  2).alias('tx_nem_nem'))

# COMMAND ----------


# Indicadores de educação
## Jovens
### Nível Brasil
df_educ_br = df \
  .withColumn('ds_uf', f.lit('BR'))

df_educ_br = aval_educ(df_educ_br)

### Nível UF
df_educ_uf = aval_educ(df)

### União das duas bases
df_educ = df_educ_br.union(df_educ_uf).orderBy(f.desc('ANO'),f.desc('TRIMESTRE'), f.desc('ds_uf'))

## Recorte para 18 anos
### Nível Brasil
df_educ_br_18 = df \
  .where(f.col('V2009') == f.lit(18)) \
  .withColumn('ds_uf', f.lit('BR')) \
  .withColumn('ds_faixa_etaria', f.lit('18 anos')) 
df_educ_br_18 = aval_educ(df_educ_br_18)

### Nível UF
df_educ_uf_18 = df \
  .where(f.col('V2009') == f.lit(18)) \
  .withColumn('ds_faixa_etaria', f.lit('18 anos')) 
df_educ_uf_18 = aval_educ(df_educ_uf_18)

### Define base final  
df_educ_final = df_educ.union(df_educ_br_18).union(df_educ_uf_18).orderBy(f.desc('ANO'),f.desc('TRIMESTRE'), f.desc('ds_uf'))

# COMMAND ----------

# Salva resultados finais
df_educ_final.write.parquet(var_adls_uri + '/tmp/dev/biz/oni/painel_indicadores_senai/indicadores_pnad', mode='overwrite')
