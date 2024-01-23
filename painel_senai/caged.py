# Databricks notebook source
# importa bibliotecas
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
window = Window.partitionBy()

# Define caminhos
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

## Dimensões
path_uf='{uri}/trs/oni/ibge/uf/'.format(uri=var_adls_uri)
path_cnae='{uri}/biz/oni/bases_referencia/cnae/cnae_20/cnae_grupo/'.format(uri=var_adls_uri)
path_cbo='{uri}/biz/oni/bases_referencia/cbo/cbo6/'.format(uri=var_adls_uri)
raw_ipca_path = '{uri}/raw/crw/ibge/ipca'.format(uri=var_adls_uri)

## PNAD
path_pnadc='{uri}/trs/ibge/pnadc/'.format(uri=var_adls_uri)

## Caged
path_novo_caged='{uri}/trs/me/novo_caged/'.format(uri=var_adls_uri)
path_caged='{uri}/trs/me/caged/'.format(uri=var_adls_uri)

# COMMAND ----------

# Leitura dos dados

## Dimensão
### UF
df_uf=spark.read.parquet(path_uf).select('cd_uf','ds_uf').withColumnRenamed('cd_uf','UF')
### IPCA
df_ipca = (spark.read.parquet(raw_ipca_path))
### CNAE
df_cnae=spark.read.parquet(path_cnae)
### CBO
df_cbo=spark.read.parquet(path_cbo)

## PNAD
df_pnadc=spark.read.parquet(path_pnadc)

## CAGED e Novo CAGED
df_caged=spark.read.parquet(path_caged)
df_novo_caged=spark.read.parquet(path_novo_caged)

# COMMAND ----------

# CAGED e Novo CAGED
mes_index = ["01","02","03","04","05","06","07","08","09","10","11","12"]
mes_conteudo=["janeiro","fevereiro","mar?o","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]

# define the schema for the DataFrame
mes_schema = StructType([
    StructField("cd_mes", StringType(), True),
    StructField("MES", StringType(), True)
])

data_mes = zip(mes_index, mes_conteudo)
df_mes = spark.createDataFrame(data_mes, schema=mes_schema)

trimestre_index = ["01","02","03","04","05","06","07","08","09","10","11","12"]
trimestre_conteudo=[1,1,1,2,2,2,3,3,3,4,4,4]
# define the schema for the DataFrame
trimestre_schema = StructType([
    StructField("MES", StringType(), True),
    StructField("TRIMESTRE", StringType(), True)
])

data_trimestre = zip(mes_index, trimestre_conteudo)
df_trimestre = spark.createDataFrame(data_trimestre, schema=trimestre_schema)


anos_caged=df_caged.select('CD_ANO_MOVIMENTACAO').distinct().rdd.flatMap(lambda x: x).collect() #lista com todos os anos do CAGED
anos_novo_caged=df_novo_caged.select('CD_ANO_COMPETENCIA').distinct().rdd.flatMap(lambda x: x).collect() #lista com todos os anos do Novo CAGED

anos_caged_total=anos_caged+anos_novo_caged

ultimo_mes=df_novo_caged.select(f.col('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO').cast(IntegerType())).select(f.max('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO'))

ultimo_mes = ultimo_mes.collect()[0][0]

# COMMAND ----------

df_ipca = df_ipca \
  .withColumn('MES',f.split(f.col('MES_ANO')," ")[0]) \
  .withColumn('ANO',f.split(f.col('MES_ANO')," ")[1]) \
  .join(df_mes,'MES','left') \
  .withColumn('CD_ANO_MES',f.concat_ws('',f.col('ANO'),f.col('cd_mes'))) \
  .withColumn('CD_ANO_MES', f.col('CD_ANO_MES').cast('int')) \
  .where((f.col('ANO').isin(anos_caged_total)) & ((f.col('CD_ANO_MES')<=ultimo_mes))) \
  .orderBy(f.asc('CD_ANO_MES')) \
  .withColumn('ANO',f.col('ANO').cast('int')) \
  .withColumn('base_fixa',f.last('VALOR_IPCA').over(window)) \
  .withColumn('INDICE_FIXO_ULTIMO_ANO',f.col('VALOR_IPCA')/f.col('base_fixa')) \
  .withColumn('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO',f.col('CD_ANO_MES').cast(StringType()))\
  .select('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO','INDICE_FIXO_ULTIMO_ANO') 

# COMMAND ----------

# Definição das variáveis
window = Window.partitionBy()

## Novo Caged
df_novo_caged_indicador = df_novo_caged \
  .withColumnRenamed('CD_UF','UF') \
  .withColumn('MES', f.substring('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO',5,6)) \
  .join(df_trimestre,'MES','left') \
  .withColumn('cd_cnae_subclasse', f.lpad(f.col('CD_CNAE20_SUBCLASSE'),7,'0')) \
  .withColumn('cd_cnae_divisao', f.substring(f.col('cd_cnae_subclasse'),1,2)) \
  .withColumn('cd_cnae_grupo',f .substring(f.col('cd_cnae_subclasse'),1,3)) \
  .withColumn('CD_SALDO_MOV', f.when(f.col('ORIGEM')=="CAGEDEXC",f.col('CD_SALDO_MOV')*f.lit(-1))\
    .otherwise(f.col('CD_SALDO_MOV'))) \
  .withColumn('ds_faixa_etaria', f.when((f.col('VL_IDADE')>=f.lit(15)) & (f.col('VL_IDADE')<=f.lit(17)), f.lit('Entre 15 e 17 anos')).otherwise(
    f.when((f.col('VL_IDADE')>=f.lit(18)) & (f.col('VL_IDADE')<=f.lit(24)), f.lit('Entre 18 e 24 anos')).otherwise(
      f.when((f.col('VL_IDADE')>=f.lit(25)) & (f.col('VL_IDADE')<=f.lit(29)), f.lit('Entre 25 e 29 anos')).otherwise(
        f.lit('Não ´[e] jovem'))))) \
  .join(df_ipca,'CD_ANO_MES_COMPETENCIA_MOVIMENTACAO','left') \
  .join(df_cnae,'cd_cnae_grupo','left') \
  .join(df_uf,'UF','left') \
  .withColumn('VL_SALARIO_MENSAL_REAL', f.col('VL_SALARIO_MENSAL')/f.col('INDICE_FIXO_ULTIMO_ANO')) \
  .withColumn("VL_ADMITIDOS",f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_ADMITIDOS_APRENDIZ",f.when((f.col('CD_SALDO_MOV')==f.lit(1)) & (f.col('FL_IND_APRENDIZ')==f.lit(1)),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_ADM_REAL",f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('VL_SALARIO_MENSAL_REAL')).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_ADM_NOMINAL",f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('VL_SALARIO_MENSAL')).otherwise(f.lit(0))) \
  .withColumn("VL_DEMITIDOS",f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_DEMITIDOS_APRENDIZ",f.when((f.col('CD_SALDO_MOV')==f.lit(-1)) & (f.col('FL_IND_APRENDIZ')==f.lit(1)),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_DEM_NOMINAL",f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.col('VL_SALARIO_MENSAL')).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_DEM_REAL",f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.col('VL_SALARIO_MENSAL_REAL')) \
  .otherwise(f.lit(0)))

## Caged
df_caged_indicador = df_caged \
  .withColumnRenamed('CD_ANO_MOVIMENTACAO', 'CD_ANO_COMPETENCIA') \
  .withColumnRenamed('CD_UF','UF') \
  .withColumn('MES', f.substring('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO',5,6)) \
  .join(df_trimestre,'MES','left') \
  .withColumn('cd_cnae_subclasse',f.lpad(f.col('CD_CNAE20_SUBCLASSE'),7,'0')) \
  .withColumn('cd_cnae_divisao',f.substring(f.col('cd_cnae_subclasse'),1,2)) \
  .withColumn('cd_cnae_grupo',f.substring(f.col('cd_cnae_subclasse'),1,3)) \
  .withColumn('ds_faixa_etaria', f.when((f.col('VL_IDADE')>=f.lit(15)) & (f.col('VL_IDADE')<=f.lit(17)), f.lit('Entre 15 e 17 anos')).otherwise(
    f.when((f.col('VL_IDADE')>=f.lit(18)) & (f.col('VL_IDADE')<=f.lit(24)), f.lit('Entre 18 e 24 anos')).otherwise(
        f.when((f.col('VL_IDADE')>=f.lit(25)) & (f.col('VL_IDADE')<=f.lit(29)), f.lit('Entre 25 e 29 anos')).otherwise(
          f.lit('Não é jovem'))))) \
  .join(df_ipca,'CD_ANO_MES_COMPETENCIA_MOVIMENTACAO','left') \
  .join(df_cnae,'cd_cnae_grupo','left') \
  .join(df_uf,'UF','left') \
  .withColumn('VL_SALARIO_MENSAL_REAL', f.col('VL_SALARIO_MENSAL')/f.col('INDICE_FIXO_ULTIMO_ANO')) \
  .withColumn("VL_ADMITIDOS",f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_ADMITIDOS_APRENDIZ",f.when((f.col('CD_SALDO_MOV')==f.lit(1)) & (f.col('FL_IND_APRENDIZ')==f.lit(1)),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_ADM_REAL",f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('VL_SALARIO_MENSAL_REAL')).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_ADM_NOMINAL",f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('VL_SALARIO_MENSAL')).otherwise(f.lit(0))) \
  .withColumn("VL_DEMITIDOS",f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_DEMITIDOS_APRENDIZ",f.when((f.col('CD_SALDO_MOV')==f.lit(-1)) & (f.col('FL_IND_APRENDIZ')==f.lit(1)),f.lit(1)).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_DEM_NOMINAL",f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.col('VL_SALARIO_MENSAL')).otherwise(f.lit(0))) \
  .withColumn("VL_SAL_DEM_REAL",f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.col('VL_SALARIO_MENSAL_REAL')).otherwise(f.lit(0)))


# COMMAND ----------


# Saldo Novo CAGED UF
window = Window.partitionBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo')

df_novo_caged_uf = df_novo_caged_indicador \
  .groupBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo','ds_faixa_etaria') \
  .agg(
    f.sum('VL_ADMITIDOS').alias('VL_ADMITIDOS'),
    f.sum('VL_DEMITIDOS').alias('VL_DEMITIDOS'),
    f.sum('VL_ADMITIDOS_APRENDIZ').alias('VL_ADMITIDOS_APRENDIZ'),
    f.sum('VL_DEMITIDOS_APRENDIZ').alias('VL_DEMITIDOS_APRENDIZ')) \
  .withColumn('VL_PROP_ADM', f.round((f.col('VL_ADMITIDOS')/f.sum('VL_ADMITIDOS').over(window)) * 100, 2)) \
  .withColumn('VL_PROP_DEM', f.round((f.col('VL_DEMITIDOS')/f.sum('VL_DEMITIDOS').over(window)) * 100, 2)) \
  .withColumn('VL_PROP_ADM_APRENDIZ', f.round((f.col('VL_ADMITIDOS_APRENDIZ')/f.sum('VL_ADMITIDOS_APRENDIZ').over(window)) * 100, 2)) \
  .withColumn('VL_PROP_DEM_APRENDIZ', f.round((f.col('VL_DEMITIDOS_APRENDIZ')/f.sum('VL_DEMITIDOS_APRENDIZ').over(window)) * 100, 2))

## Salário Novo CAGED UF
window = Window.partitionBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo')

df_novo_caged_uf_sal = df_novo_caged_indicador \
  .where((f.col('VL_SALARIO_MENSAL_REAL')<=f.lit(30000)) & (f.col('VL_SALARIO_MENSAL_REAL')>=f.lit(300)) & (f.col('ORIGEM')=="CAGEDMOV")) \
  .groupBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo','ds_faixa_etaria') \
  .agg(
    f.round(f.sum('VL_SAL_ADM_REAL')/f.sum('VL_ADMITIDOS'),2).alias('VL_SAL_ADMISSAO_REAL'),
    f.round(f.sum('VL_SAL_ADM_NOMINAL')/f.sum('VL_ADMITIDOS'),2).alias('VL_SAL_ADMISSAO_NOMINAL'),
    f.round(f.sum('VL_SAL_DEM_REAL')/f.sum('VL_DEMITIDOS'),2).alias('VL_SAL_DEMISSAO_REAL'),
    f.round(f.sum('VL_SAL_DEM_NOMINAL')/f.sum('VL_DEMITIDOS'),2).alias('VL_SAL_DEMISSAO_NOMINAL'))
  
df_novo_caged_uf_final=df_novo_caged_uf.join(df_novo_caged_uf_sal,['CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo','ds_faixa_etaria'],'left')
## Saldo CAGED UF

#CAGED Brasil
window=Window.partitionBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo')
df_caged_uf = df_caged_indicador \
  .groupBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo','ds_faixa_etaria') \
  .agg(
    f.sum('VL_ADMITIDOS').alias('VL_ADMITIDOS'),
    f.sum('VL_DEMITIDOS').alias('VL_DEMITIDOS'),
    f.sum('VL_ADMITIDOS_APRENDIZ').alias('VL_ADMITIDOS_APRENDIZ'),
    f.sum('VL_DEMITIDOS_APRENDIZ').alias('VL_DEMITIDOS_APRENDIZ')) \
  .withColumn('VL_PROP_ADM', f.round((f.col('VL_ADMITIDOS')/f.sum('VL_ADMITIDOS').over(window)) * 100, 2)) \
  .withColumn('VL_PROP_DEM', f.round((f.col('VL_DEMITIDOS')/f.sum('VL_DEMITIDOS').over(window)) * 100, 2)) \
  .withColumn('VL_PROP_ADM_APRENDIZ', f.round((f.col('VL_ADMITIDOS_APRENDIZ')/f.sum('VL_ADMITIDOS_APRENDIZ').over(window)) * 100, 2)) \
  .withColumn('VL_PROP_DEM_APRENDIZ', f.round((f.col('VL_DEMITIDOS_APRENDIZ')/f.sum('VL_DEMITIDOS_APRENDIZ').over(window)) * 100, 2))

## Salário CAGED UF
#Novo CAGED Brasil
window = Window.partitionBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo')

df_caged_uf_sal = df_caged_indicador \
  .where((f.col('VL_SALARIO_MENSAL_REAL') <= f.lit(30000)) & (f.col('VL_SALARIO_MENSAL_REAL') >= f.lit(300))) \
  .groupBy('CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo','ds_faixa_etaria') \
  .agg(
    f.round(f.sum('VL_SAL_ADM_REAL')/f.sum('VL_ADMITIDOS'), 2).alias('VL_SAL_ADMISSAO_REAL'),
    f.round(f.sum('VL_SAL_ADM_NOMINAL')/f.sum('VL_ADMITIDOS'), 2).alias('VL_SAL_ADMISSAO_NOMINAL'),
    f.round(f.sum('VL_SAL_DEM_REAL')/f.sum('VL_DEMITIDOS'), 2).alias('VL_SAL_DEMISSAO_REAL'),
    f.round(f.sum('VL_SAL_DEM_NOMINAL')/f.sum('VL_DEMITIDOS'), 2).alias('VL_SAL_DEMISSAO_NOMINAL'))

# Definição bases finais
df_caged_uf_final = df_caged_uf \
  .join(df_caged_uf_sal,['CD_ANO_COMPETENCIA','TRIMESTRE','ds_uf','cd_cnae_grupo','nm_cnae_grupo','ds_faixa_etaria'], 'left')

df_caged_final = df_caged_uf_final \
  .union(df_novo_caged_uf_final).withColumn('ds_uf',f.when(f.col('ds_uf').isNull(),f.lit('N?o informado')).otherwise(f.col('ds_uf'))) \
  .withColumn('nm_cnae_grupo', f.when(f.col('cd_cnae_grupo')==f.lit('999'),f.lit('Não informado')).otherwise(f.col('nm_cnae_grupo')))

# COMMAND ----------

df_caged_final.display()

# COMMAND ----------

# Observação dos resultados nulos por coluna
df_caged_final.select([f.count(f.when(f.isnan(c) | f.col(c).isNull(), c)).alias(c) for c in df_caged_final.columns]).display()

# COMMAND ----------

# Salva resultados finais
df_caged_final.write.parquet(var_adls_uri + '/tmp/dev/biz/oni/painel_indicadores_senai/indicadores_caged', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC display(df_caged_final)
# MAGIC df_caged_final.select([f.count(f.when(f.isnan(c) | f.col(c).isNull(), c)).alias(c) for c in df_caged_final.columns]
# MAGIC    ).display()
# MAGIC df_caged_final.write.parquet(var_adls_uri + '/tmp/dev/biz/oni/painel_indicadores_senai/indicadores_caged', mode='overwrite')
# MAGIC path_base_caged='{uri}/tmp/dev/biz/oni/painel_indicadores_senai/indicadores_caged'.format(uri=var_adls_uri)
# MAGIC base_caged=spark.read.parquet(path_base_caged)
# MAGIC display(base_caged)
# MAGIC base_caged.select([f.count(f.when(f.isnan(c) | f.col(c).isNull(), c)).alias(c) for c in base_caged.columns]
# MAGIC    ).display()
# MAGIC
