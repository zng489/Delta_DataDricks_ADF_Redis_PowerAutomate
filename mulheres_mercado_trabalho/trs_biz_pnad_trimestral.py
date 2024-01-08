# Databricks notebook source
import json
import re
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

source_pnadc = "{uri}/trs/ibge/pnadc/".format(uri=var_adls_uri)

# COMMAND ----------

df_pnadc = spark.read.parquet(source_pnadc)

# COMMAND ----------

raw_deflatores_path='{uri}/raw/crw/ibge/deflatores'.format(uri=var_adls_uri)
deflatores=(spark.read.parquet(raw_deflatores_path))
deflatores_columns = ['Ano','trim','UF','Habitual','Efetivo']
df_deflatores=(deflatores
           .select(*deflatores_columns))
df_deflatores=(deflatores
               .withColumnRenamed('Ano','ANO')
               .withColumn('TRIMESTRE',f.when((f.col('trim')==f.lit('01-02-03')), f.lit(1)).otherwise(
               f.when((f.col('trim')==f.lit('04-05-06')), f.lit(2)).otherwise(
               f.when((f.col('trim')==f.lit('07-08-09')), f.lit(3)).otherwise(
               f.when((f.col('trim')==f.lit('10-11-12')), f.lit(4))))))
              .where(f.col('trim').isNotNull())
           .drop('trim'))

# COMMAND ----------

def select_columns_pnadc(df: DataFrame)-> DataFrame:
  pnadc_columns = ["ANO","TRIMESTRE","UF","UPA", "V1008", "V1022", "V1028","V2007","V2009","V3002","V4040","VD3004","VD3005","VD4001", 
                   "VD4002","VD4003","VD4004","VD4004A","VD4005","VD4009","VD4012","VD4017","VD4018","V4019","V4076","V40761","V40762","V40763","V2010","V4013","V4039"]
  return df.select(*pnadc_columns)

# COMMAND ----------

df_pnadc = select_columns_pnadc(df_pnadc)

# COMMAND ----------

def formating_and_filtering_variables(df: DataFrame) -> DataFrame:
  return df.join(df_deflatores,['ANO','TRIMESTRE','UF']).withColumnRenamed("TRIMESTRE","nm_trimestre")\
          .withColumnRenamed("ANO","nm_ano")\
          .withColumnRenamed("UF","cd_uf")\
          .withColumnRenamed("V4013","cd_cnae_dom")\
          .withColumnRenamed("V2007","cd_sexo")\
          .withColumn('ds_sexo',f.when(f.col("cd_sexo")==f.lit(1),"Masculino")\
                                  .otherwise(f.when(f.col("cd_sexo")==f.lit(2),"Feminino")))

# COMMAND ----------

df_pnadc = formating_and_filtering_variables(df_pnadc)

# COMMAND ----------

def calculate_employed(df: DataFrame) -> DataFrame:
  return (df_pnadc.withColumn('cd_cnae_dom', f.lpad('cd_cnae_dom',5,'0'))
          .withColumn('cd_cnae_dom_div', f.substring('cd_cnae_dom',1,2))
          .withColumn("vl_ocupados",f.when(f.col("VD4002") == f.lit(1),f.col("V1028")).otherwise(f.lit(0)))\
                 .withColumn("vl_protecao_social",f.when((f.col("VD4002")==f.lit(1)) & \
                                                                ((f.col("VD4009").isin([2, 4,6,10])) | 
                                                                 ((f.col("VD4009").isin([8,9])) & \
                                                                (f.col('VD4012')==f.lit(2)))),f.col('V1028')).otherwise(f.lit(0)))
                 .withColumn('vl_horas_trabalhadas', f.when(f.col('VD4002')==f.lit(1), f.col('V4039')*f.lit(4)*f.col('V1028')).otherwise(f.lit(0))))

# COMMAND ----------

df_pnadc = calculate_employed(df_pnadc)

# COMMAND ----------

def calculate_population(df: DataFrame) -> DataFrame:
  return (df_pnadc.withColumn("vl_desocupados",f.when(f.col("VD4002") == f.lit(2),f.col("V1028")).otherwise(f.lit(0)))
          .withColumn('vl_populacao_14anosmais', f.when(f.col('V2009') >= f.lit(14), f.col('V1028')).otherwise(f.lit(0)))
          .withColumn("vl_subocupados",f.when((f.col("VD4004A") == f.lit(1)) |(f.col("VD4004") == f.lit(1)),f.col("V1028")).otherwise(f.lit(0)))
          .withColumn("vl_subutilizados",f.when((f.col("VD4004A") == f.lit(1)) |(f.col("VD4004") == f.lit(1)) #subocupados por insuficiências de horas
                                                 | (f.col('VD4003') == f.lit(1)) | #pessoas fora da força de trabalho e na força de trabalho potencial
                                                 (f.col('VD4002')==f.lit(2)) #pessoas desocupadas
                                                 ,f.col("V1028")).otherwise(f.lit(0)))
          .withColumn("vl_forca_de_trabalho",f.when(f.col("VD4001") == f.lit(1),f.col("V1028")).otherwise(f.lit(0)))
          .withColumn("vl_forca_de_trabalho_ampliada",f.when((f.col("VD4001") == f.lit(1)) | (f.col('VD4003') == f.lit(1)),f.col("V1028")).otherwise(f.lit(0))))  

# COMMAND ----------

df_pnadc = calculate_population(df_pnadc)

# COMMAND ----------

def calculate_salary_and_population(df: DataFrame) -> DataFrame:
  return df_pnadc.withColumn("vl_massa_salarial",f.when(f.col("VD4002") == f.lit(1),(f.col("V1028")*f.col("VD4017")*f.col('Efetivo'))).otherwise(f.lit(0)))

# COMMAND ----------

df_pnadc = calculate_salary_and_population(df_pnadc)

# COMMAND ----------

def calculate_value_learning(df: DataFrame)-> DataFrame:
  return (df.withColumn("vl_anos_estudos",f.when((f.col("V2009") >=f.lit(5)),(f.col("VD3005")* f.col("V1028")).cast("integer"))
                      .otherwise(f.lit(0)))
           .withColumn("ds_grau_instrucao_agregado", f.when(f.col('VD3004').isin(['1','2','3','4']),f.lit('Até o fundamental completo')).otherwise(
             f.when(f.col('VD3004').isin(['5','6']), f.lit('Médio completo')).otherwise(
               f.when(f.col('VD3004')==f.lit(7), f.lit('Superior completo')).otherwise(f.lit('Ignorado')))))
           .withColumn("cd_grau_instrucao_agregado", f.when(f.col('VD3004').isin(['1','2','3','4']),f.lit(1)).otherwise(
             f.when(f.col('VD3004').isin(['5','6']), f.lit(2)).otherwise(
               f.when(f.col('VD3004')==f.lit(7), f.lit(3)).otherwise(f.lit(-1))))))

# COMMAND ----------

df_pnadc = calculate_value_learning(df_pnadc)

# COMMAND ----------

def create_age_group(df: DataFrame)-> DataFrame:
  return (df.withColumn("cd_faixa_etaria",f.when(((f.col("V2009")<=f.lit(20))),f.lit(1))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(20)) & (f.col("V2009")<=f.lit(30))),f.lit(2))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(30)) & (f.col("V2009")<=f.lit(40))),f.lit(3))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(40)) & (f.col("V2009")<=f.lit(50))),f.lit(4))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(50)) & (f.col("V2009")<=f.lit(60))),f.lit(5))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(60)) & (f.col("V2009")<=f.lit(70))),f.lit(6))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(70)) & (f.col("V2009")<=f.lit(80))),f.lit(7))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(80))),f.lit(8)).otherwise(f.lit(9))))))))\
                                        ))\
                       .withColumn("ds_faixa_etaria",f.when(((f.col("V2009")<=f.lit(20))),f.lit('Até 20 anos'))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(20)) & (f.col("V2009")<=f.lit(30))),f.lit('21 a 30 anos'))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(30)) & (f.col("V2009")<=f.lit(40))),f.lit('31 a 40 anos'))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(40)) & (f.col("V2009")<=f.lit(50))),f.lit('41 a 50 anos'))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(50)) & (f.col("V2009")<=f.lit(60))),f.lit('51 a 60 anos'))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(60)) & (f.col("V2009")<=f.lit(70))),f.lit('61 a 70 anos'))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(70)) & (f.col("V2009")<=f.lit(80))),f.lit('71 a 80 anos'))\
                                        .otherwise(f.when(((f.col("V2009")>f.lit(80))),f.lit('Maiores de 80 anos')).otherwise('Não informado')))))))\
                                        )))

# COMMAND ----------

df_pnadc = create_age_group(df_pnadc)

# COMMAND ----------

def create_job_type(df: DataFrame)-> DataFrame:
  return (df.withColumn("cd_tipo_vinculo",f.when(f.col('VD4009').isin('1','3','5','7'), f.lit(1))\
                        .otherwise(f.when(f.col('V4019')==f.lit(1),f.lit(2))\
                          .otherwise(f.lit(3))))\
                       .withColumn("nm_tipo_vinculo",f.when(f.col('VD4009').isin('1','3','5','7'), f.lit('CLT ou público'))\
                        .otherwise(f.when(f.col('V4019')==f.lit(1),f.lit('CNPJ'))\
                          .otherwise(f.lit('Informal')))))

# COMMAND ----------

df_pnadc = create_job_type(df_pnadc)

# COMMAND ----------

def calculate_agregated_values_ocupados(df: DataFrame) -> DataFrame:
  return (df.where(f.col('VD4002')==f.lit(1))
  .groupBy("nm_ano","nm_trimestre","cd_uf","cd_faixa_etaria","ds_faixa_etaria",
                    "cd_grau_instrucao_agregado","ds_grau_instrucao_agregado","cd_cnae_dom_div","cd_tipo_vinculo","nm_tipo_vinculo","cd_sexo","ds_sexo").agg(
                      f.sum('vl_horas_trabalhadas').alias('vl_massa_horas_trab'),
                  f.sum(f.col("vl_ocupados")).alias("vl_ocupados"),
                  f.sum(f.col("vl_protecao_social")).alias("vl_protecao_social"),
                 f.sum(f.col("vl_anos_estudos")).alias("vl_anos_estudo_ocupados"),
                 f.sum(f.col("vl_massa_salarial")).alias("vl_massa_salarial")))

# COMMAND ----------

df_ocupados = calculate_agregated_values_ocupados(df_pnadc)

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count
df_ocupados.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_ocupados.columns]
   ).display()

# COMMAND ----------

def calculate_agregated_values_population(df: DataFrame) -> DataFrame:
  return df.groupBy("nm_ano","nm_trimestre","cd_uf","cd_faixa_etaria","ds_faixa_etaria",
                    "cd_grau_instrucao_agregado","ds_grau_instrucao_agregado","cd_sexo","ds_sexo").agg(
                      f.sum(f.col("V1028")).alias("vl_populacao"),
                   f.sum(f.col("vl_populacao_14anosmais")).alias("vl_populacao_14anosmais"),   
                  f.sum(f.col("vl_desocupados")).alias("vl_desocupados"),
                  f.sum(f.col("vl_subocupados")).alias("vl_subocupados"),
                 f.sum(f.col("vl_subutilizados")).alias("vl_subutilizados"),
                 f.sum(f.col("vl_forca_de_trabalho")).alias("vl_forca_de_trabalho"),
                 f.sum(f.col("vl_forca_de_trabalho_ampliada")).alias("vl_forca_de_trabalho_ampliada"),
                 f.sum('vl_anos_estudos').alias('vl_anos_estudo'))

# COMMAND ----------

df_populacao=calculate_agregated_values_population(df_pnadc)

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count
df_populacao.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_populacao.columns]
   ).display()

# COMMAND ----------

display(df_ocupados.groupBy('nm_ano','nm_trimestre').agg(f.sum('vl_massa_salarial').alias('vl_massa_salarial'),
                                                         f.sum('vl_ocupados').alias('vl_ocupados')).orderBy(f.asc('nm_ano'),f.asc('nm_trimestre')))

# COMMAND ----------

display(df_populacao.groupBy('nm_ano','nm_trimestre').agg(f.sum('vl_populacao').alias('vl_populacao'),
  f.round((f.sum('vl_subutilizados')/f.sum('vl_forca_de_trabalho_ampliada'))*100,2).alias('tx_subutilizacao'),
                                                          f.sum('vl_subocupados').alias('vl_subocupados'),
                                                          f.sum('vl_desocupados').alias('vl_desocupados'),
                                                          f.sum('vl_forca_de_trabalho').alias('vl_forca_de_trabalho')
                                                          ).withColumn('tx_subocupacao', f.round(((f.col('vl_subocupados')+f.col('vl_desocupados'))/f.col('vl_forca_de_trabalho'))*100,2)).orderBy(f.asc('nm_ano'),f.asc('nm_trimestre')).select('nm_ano','nm_trimestre','vl_populacao','tx_subocupacao','tx_subutilizacao'))

# COMMAND ----------

display(df_ocupados)

# COMMAND ----------

display(df_populacao)

# COMMAND ----------

df_ocupados.write.parquet(var_adls_uri + '/uds/oni/observatorio_nacional/mulheres_industria_fnme/pnad_ocupados', mode="overwrite")

# COMMAND ----------

df_populacao.write.parquet(var_adls_uri + '/uds/oni/observatorio_nacional/mulheres_industria_fnme/pnad_populacao', mode="overwrite")

# COMMAND ----------

path_ocup='{uri}/uds/oni/observatorio_nacional/mulheres_industria_fnme/pnad_ocupados'.format(uri=var_adls_uri)
df_ocup=(spark.read.parquet(path_ocup))
display(df_ocup)

# COMMAND ----------

display(df_ocup.groupBy('nm_ano','nm_trimestre').agg(f.sum('vl_massa_salarial').alias('vl_massa_salarial'),
                                                         f.sum('vl_ocupados').alias('vl_ocupados')).orderBy(f.asc('nm_ano'),f.asc('nm_trimestre')))

# COMMAND ----------

path_pop='{uri}/uds/oni/observatorio_nacional/mulheres_industria_fnme/pnad_populacao'.format(uri=var_adls_uri)
df_pop=(spark.read.parquet(path_pop))
display(df_pop)

# COMMAND ----------

display(df_pop.groupBy('nm_ano','nm_trimestre').agg(f.sum('vl_populacao').alias('vl_populacao'),
  f.round((f.sum('vl_subutilizados')/f.sum('vl_forca_de_trabalho_ampliada'))*100,2).alias('tx_subutilizacao'),
                                                          f.sum('vl_subocupados').alias('vl_subocupados'),
                                                          f.sum('vl_desocupados').alias('vl_desocupados'),
                                                          f.sum('vl_forca_de_trabalho').alias('vl_forca_de_trabalho')
                                                          ).withColumn('tx_subocupacao', f.round(((f.col('vl_subocupados')+f.col('vl_desocupados'))/f.col('vl_forca_de_trabalho'))*100,2)).orderBy(f.asc('nm_ano'),f.asc('nm_trimestre')).select('nm_ano','nm_trimestre','vl_populacao','tx_subocupacao','tx_subutilizacao'))
