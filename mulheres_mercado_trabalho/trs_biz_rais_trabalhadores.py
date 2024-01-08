# Databricks notebook source
import json
import re
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

source_contribuintes = "{uri}/trs/oni/bases_do_projeto/painel_dos_estados/contribuintes/".format(uri=var_adls_uri)
source_instrucao = "{uri}/trs/oni/bases_do_projeto/painel_dos_estados/grau_instrucao_rais/".format(uri=var_adls_uri)
source_tam_estabelecimento = "{uri}/trs/oni/bases_do_projeto/painel_dos_estados/tam_estabelecimento/".format(uri=var_adls_uri)
source_rais_vinculo = "{uri}/trs/me/rais_vinculo".format(uri=var_adls_uri)
source_cadastro_cbo = "{uri}/trs/me/cadastro_cbo/".format(uri=var_adls_uri)
source_cnae_subclasses = "{uri}/biz/oni/bases_referencia/cnae/cnae_20/cnae_subclasse/".format(uri=var_adls_uri)
source_ipca = "{uri}/raw/crw/ibge/ipca/".format(uri=var_adls_uri)

# COMMAND ----------

df_contribuintes = spark.read.parquet(source_contribuintes)
df_grau_instrucao = spark.read.parquet(source_instrucao)
df_tam_estabelecimento = spark.read.parquet(source_tam_estabelecimento)
df_rais_vinculo = spark.read.parquet(source_rais_vinculo)
df_cbo = spark.read.parquet(source_cadastro_cbo)
df_cnae_subclasses = spark.read.parquet(source_cnae_subclasses)
df_ipca =  spark.read.parquet(source_ipca)

# COMMAND ----------

display(df_rais_vinculo.where(f.col('NR_MES_TEMPO_EMPREGO')<f.lit(1)).groupBy('ANO').agg(f.count(f.lit(1))).orderBy(f.asc('ANO')))

# COMMAND ----------

def select_last_month_year(df_ipca: DataFrame) -> DataFrame:
  window = Window.partitionBy()
  df_ipca = (df_ipca
             .withColumn('ds_mes',f.split(f.col('MES_ANO')," ")[0])
             .withColumn('nm_ano',f.split(f.col('MES_ANO')," ")[1])
             .withColumnRenamed('ds_mes','nm_mes')
             .withColumn('nm_mes', 
                         f.when(f.col('nm_mes')==f.lit('janeiro'), f.lit(1)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('fevereiro'), f.lit(2)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('março'), f.lit(3)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('abril'), f.lit(4)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('maio'), f.lit(5)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('junho'), f.lit(6)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('julho'), f.lit(7)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('agosto'), f.lit(8)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('setembro'), f.lit(9)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('outubro'), f.lit(10)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('novembro'), f.lit(11)).otherwise(
                           f.when(f.col('nm_mes')==f.lit('dezembro'), f.lit(12))))))))))))))
             #.where((f.col('nm_ano')>f.lit(year_initial_date)))
             .orderBy("nm_ano","nm_mes")
             .withColumn('base_fixa',f.last(f.col('VALOR_IPCA')).over(window))
             .withColumn('vl_indice_fixo_ultimo_ano',f.col('VALOR_IPCA')/f.col('base_fixa'))
             .select('nm_ano',"nm_mes",'vl_indice_fixo_ultimo_ano'))
  
  return df_ipca.where(f.col("nm_mes")==f.lit(12))\
                 .select('nm_ano','vl_indice_fixo_ultimo_ano')

# COMMAND ----------

df_ipca_=select_last_month_year(df_ipca)

# COMMAND ----------

display(df_ipca)

# COMMAND ----------

def rename_column(df: DataFrame, old_col: str, new_col: str) -> DataFrame:
  return df.withColumnRenamed(old_col, new_col)

# COMMAND ----------

df_cbo = df_cbo.select("cd_cbo1","ds_cbo1","cd_cbo2","ds_cbo2","cd_cbo4","ds_cbo4","ds_tipo_familia","cd_ocup_corporativa_industriais")
df_cbo = df_cbo.dropDuplicates()

# COMMAND ----------

def filter_rais_ipca(df_rais_vinculo: DataFrame, df_ipca: DataFrame) -> DataFrame:
  return df_rais_vinculo.withColumnRenamed("ANO","nm_ano")\
                .withColumn("CD_CNAE20_SUBCLASSE",f.lpad(f.col("CD_CNAE20_SUBCLASSE"),7,'0'))\
                .withColumn("cd_cnae_divisao",f.substring(f.col("CD_CNAE20_SUBCLASSE"),1,2))\
                .join(df_ipca_,["nm_ano"],"left")\
                .withColumn("VL_REMUN_MEDIA_REAL", f.col("VL_REMUN_MEDIA_NOM")/f.col("vl_indice_fixo_ultimo_ano"))\
                .drop("vl_indice_fixo_ultimo_ano")\
                .withColumn('VL_REMUN_MEDIA_REAL',f.when(((f.col('VL_REMUN_MEDIA_REAL')<f.lit(300)) | (f.col('VL_REMUN_MEDIA_REAL')>f.lit(50000))),f.lit(0))\
                                                   .otherwise(f.col('VL_REMUN_MEDIA_REAL')))

# COMMAND ----------

df_rais = filter_rais_ipca(df_rais_vinculo, df_ipca_)

# COMMAND ----------

def calculate_average_pay(df_rais: DataFrame,df_cbo: DataFrame,df_contribuintes: DataFrame) -> DataFrame:
  contribuintes = [data[0] for data in df_contribuintes.select('contribuintes').collect()]
  return (df_rais.withColumnRenamed("CD_CBO4","cd_cbo4")\
                   .join(df_cbo,["cd_cbo4"],"left")\
                   .withColumn("cd_cbo_agrupamento", f.when((f.col('cd_cbo1')==f.lit(3)) | ((f.col('cd_cbo1')>f.lit(3)) \
                   & (f.col('cd_cbo1')<f.lit(10)) & (f.col('ds_tipo_familia')==f.lit('Técnicas'))), f.lit(1)) \
                     .otherwise(f.when((f.col('cd_cbo1')>f.lit(3)) & (f.col('cd_cbo1')<f.lit(10)), f.lit(2)) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(2), f.lit(3)) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(0), f.lit(4)) \
                     .otherwise(f.when(f.col('cd_cbo2')==f.lit(11), f.lit(5)) \
                     .otherwise(f.when((f.col('cd_cbo2')>f.lit(11)) & (f.col('cd_cbo2')<f.lit(15)), f.lit(6)) \
                     .otherwise(f.lit(7))))))))
                     
                   .withColumn("ds_cbo_agrupamento", f.when((f.col('cd_cbo1')==f.lit(3)) | ((f.col('cd_cbo1')>f.lit(3)) \
                   & (f.col('cd_cbo1')<f.lit(10)) & (f.col('ds_tipo_familia')==f.lit('Técnicas'))), f.lit('Técnicos de nível médio')) \
                     .otherwise(f.when((f.col('cd_cbo1')>f.lit(3)) & (f.col('cd_cbo1')<f.lit(10)), f.lit('Trabalhadores auxiliares e operacionais')) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(2), f.lit('Especialistas e analistas')) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(0), f.lit('Forças armadas, policias e bombeiros militares')) \
                     .otherwise(f.when(f.col('cd_cbo2')==f.lit(11), f.lit('Membros superiores e dirigentes do setor público')) \
                     .otherwise(f.when((f.col('cd_cbo2')>f.lit(11)) & (f.col('cd_cbo2')<f.lit(15)), f.lit('Diretores e gerentes')) \
                     .otherwise(f.lit('Não informado')))))))))
                     

# COMMAND ----------

df_rais_vinculo = calculate_average_pay(df_rais,df_cbo,df_contribuintes)

# COMMAND ----------

def create_range_age(df: DataFrame) -> DataFrame:
  return df.withColumn("cd_faixa_etaria",f.when(((f.col("VL_IDADE")<=f.lit(20))),f.lit(1))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(20)) & (f.col("VL_IDADE")<=f.lit(30))),f.lit(2))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(30)) & (f.col("VL_IDADE")<=f.lit(40))),f.lit(3))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(40)) & (f.col("VL_IDADE")<=f.lit(50))),f.lit(4))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(50)) & (f.col("VL_IDADE")<=f.lit(60))),f.lit(5))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(60)) & (f.col("VL_IDADE")<=f.lit(70))),f.lit(6))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(70)) & (f.col("VL_IDADE")<=f.lit(80))),f.lit(7))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(80))),f.lit(8)).otherwise(f.lit(9))))))))\
                                        ))\
                       .withColumn("ds_faixa_etaria",f.when(((f.col("VL_IDADE")<=f.lit(20))),f.lit('Até 20 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(20)) & (f.col("VL_IDADE")<=f.lit(30))),f.lit('21 a 30 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(30)) & (f.col("VL_IDADE")<=f.lit(40))),f.lit('31 a 40 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(40)) & (f.col("VL_IDADE")<=f.lit(50))),f.lit('41 a 50 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(50)) & (f.col("VL_IDADE")<=f.lit(60))),f.lit('51 a 60 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(60)) & (f.col("VL_IDADE")<=f.lit(70))),f.lit('61 a 70 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(70)) & (f.col("VL_IDADE")<=f.lit(80))),f.lit('71 a 80 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(80))),f.lit('Maiores de 80 anos')).otherwise('Não informado')))))))\
                                        ))

# COMMAND ----------

df_rais_vinculo = create_range_age(df_rais_vinculo)

# COMMAND ----------

def create_range_job_time(df: DataFrame) -> DataFrame:
  return (df.withColumn("cd_tempo_emprego",f.when(((f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(3))),f.lit(1))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(3)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(12))),f.lit(2))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(12)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(24))),f.lit(3))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(24)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(60))),f.lit(4))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(60)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(120))),f.lit(5))\
                                        .otherwise(f.when((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(120)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(180)),f.lit(6))\
                                        .otherwise(f.when(f.col("NR_MES_TEMPO_EMPREGO")>f.lit(180),f.lit(7))
                                                   .otherwise(f.lit(8)))))))\
                                        ))\
            .withColumn("ds_tempo_emprego",f.when(((f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(3))),f.lit("Até 3 meses"))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(3)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(12))),f.lit("De 3 meses até 1 ano"))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(12)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(24))),f.lit("De 1 a 2 anos"))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(24)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(60))),f.lit("De 2 a 5 anos"))\
                                        .otherwise(f.when(((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(60)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(120))),f.lit("De 5 a 10 anos"))\
                                        .otherwise(f.when((f.col("NR_MES_TEMPO_EMPREGO")>f.lit(120)) & (f.col("NR_MES_TEMPO_EMPREGO")<=f.lit(180)),f.lit("De 10 a 15 anos"))\
                                        .otherwise(f.when(f.col("NR_MES_TEMPO_EMPREGO")>f.lit(180),f.lit("Acima de 15 anos"))
                                                   .otherwise(f.lit("Não informado")))))))\
                                        )))

# COMMAND ----------

  df_rais_vinculo = create_range_job_time(df_rais_vinculo)

# COMMAND ----------

teste=(df_rais_vinculo.where(f.col("FL_VINCULO_ATIVO_3112")==f.lit(1))\
    .withColumn('cd_grau_instrucao', f.col('cd_grau_instrucao').cast('int'))
    .join(df_grau_instrucao,'cd_grau_instrucao','left'))

# COMMAND ----------

display(teste.where(f.col('cd_grau_instrucao_agregado').isNull()).select('cd_grau_instrucao').distinct())

# COMMAND ----------

def calculate_salary_remuration(df_rais_vinculo: DataFrame) -> DataFrame:
  df = (df_rais_vinculo.where(f.col("FL_VINCULO_ATIVO_3112")==f.lit(1))\
    .withColumn('cd_grau_instrucao', f.col('cd_grau_instrucao').cast('int'))
    .join(df_grau_instrucao,'cd_grau_instrucao','left')
    .withColumn('ds_sexo',f.when(f.col("CD_SEXO")==f.lit(1),"Masculino")\
                                  .otherwise(f.when(f.col("CD_SEXO")==f.lit(2),"Feminino")))
                         .withColumn("cd_tamanho_estabelecimento",f.col("CD_TAMANHO_ESTABELECIMENTO").cast("integer"))\
                         .withColumn("cd_sexo",f.col("CD_SEXO").cast("integer"))\
                         .withColumn("cd_grau_instrucao",f.col("CD_GRAU_INSTRUCAO").cast("integer"))\
                         .withColumn("vl_po_formal", f.lit(1)) 
                         .withColumn("vl_po_salarioajust",f.when(((f.col("VL_REMUN_MEDIA_REAL")< f.lit(300)) | (f.col("VL_REMUN_MEDIA_REAL")>f.lit(50000)) ),f.lit(0))\
                                     .otherwise(f.lit(1)))\
                         .withColumn("vl_massa_horas_mes",f.when(((f.col("VL_REMUN_MEDIA_REAL")< f.lit(300)) | (f.col("VL_REMUN_MEDIA_REAL")>f.lit(50000)) ),f.lit(0))\
                                     .otherwise(f.col('QT_HORA_CONTRAT')*f.lit(4)))\
                          .withColumnRenamed("CD_UF","cd_uf")\
                          .withColumnRenamed("CD_GRAU_INSTRUCAO","cd_grau_instrucao")\
                          .withColumnRenamed("CD_TAMANHO_ESTABELECIMENTO","cd_tamanho_estabelecimento")\
                          .withColumnRenamed("CD_SEXO","cd_sexo")\
                            .withColumn('cd_grau_instrucao_agregado', f.when(f.col('cd_grau_instrucao_agregado').isNull(),f.lit(-1)).otherwise(f.col('cd_grau_instrucao_agregado')))
                            .withColumn('nm_grau_instrucao_agregado', f.when(f.col('nm_grau_instrucao_agregado').isNull(),f.lit("Não informado")).otherwise(f.col('nm_grau_instrucao_agregado')))
                        .groupBy("cd_uf","nm_ano","cd_cnae_divisao","cd_cbo_agrupamento","ds_cbo_agrupamento","cd_grau_instrucao_agregado",'nm_grau_instrucao_agregado','cd_faixa_etaria',"ds_faixa_etaria","cd_tempo_emprego","ds_tempo_emprego","cd_tamanho_estabelecimento","cd_sexo","ds_sexo")
                        .agg(f.sum("vl_po_formal").alias("vl_po_formal"),
                             f.sum("VL_REMUN_MEDIA_REAL").alias("vl_massa_salarial"),
                             f.sum("vl_po_salarioajust").alias("vl_po_salarioajust"),
                             f.sum("vl_massa_horas_mes").alias('vl_massa_horas')
                             )).join(f.broadcast(df_tam_estabelecimento),["cd_tamanho_estabelecimento"],"left").drop("dh_insertion_trs",'dh_insertion_biz',"dh_insercao_trs","dh_insercao_biz","kv_process_control")
  return df

# COMMAND ----------

df_rais_vinculo = calculate_salary_remuration(df_rais_vinculo)

# COMMAND ----------

df_rais_vinculo=df_rais_vinculo.select('nm_ano','cd_uf','cd_cnae_divisao','cd_cbo_agrupamento','ds_cbo_agrupamento','cd_tamanho_estabelecimento','nm_tamanho_estabelecimento','cd_tamanho_estabelecimento_agregado','nm_tamanho_estabelecimento_agregado','cd_grau_instrucao_agregado','nm_grau_instrucao_agregado','cd_faixa_etaria','ds_faixa_etaria','cd_tempo_emprego','ds_tempo_emprego','cd_sexo','ds_sexo','vl_po_formal','vl_massa_salarial','vl_po_salarioajust','vl_massa_horas').distinct().withColumnRenamed('nm_ano','ano')

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count
df_rais_vinculo.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_rais_vinculo.columns]
   ).display()

# COMMAND ----------

display(df_rais_vinculo.groupBy('ano').agg(f.sum('vl_po_formal').alias('vl_po_formal')))

# COMMAND ----------

display(df_rais_vinculo.where(f.col('cd_tempo_emprego')==f.lit(1)).groupBy('ano','ds_sexo').agg(f.sum('vl_po_formal').alias('vl_po_formal')))

# COMMAND ----------

display(df_rais_vinculo)

# COMMAND ----------

df_rais_vinculo.write.parquet(var_adls_uri + '/uds/oni/observatorio_nacional/mulheres_industria_fnme/rais_vinculo', mode="overwrite")
