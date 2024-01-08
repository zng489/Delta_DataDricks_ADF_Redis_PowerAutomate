# Databricks notebook source
# MAGIC %md
# MAGIC # Avaliação da bases de CNPJ da RFB para o Painel das Mulheres

# COMMAND ----------

# Define livrarias
import pyspark.sql.functions as f
# Caminho do data lake
caminho_data_lake = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

 # Carrega dados do data lake
socios_bruto = spark.read.parquet(caminho_data_lake + '/trs/rfb_cnpj/cadastro_socio_f')
nomes_bruto = spark.read.csv(caminho_data_lake + '/uds/oni/observatorio_nacional/mulheres_industria_fnme/tabelas_auxiliares/nomes.csv', header = True)
estbl_bruto = spark.read.parquet(caminho_data_lake + '/trs/rfb_cnpj/cadastro_estbl_f')
df_uf = spark.read.parquet(caminho_data_lake +'/trs/oni/ibge/uf')

# COMMAND ----------

estbl_bruto.withColumn('CD_SIT_CADASTRAL', f.col('CD_SIT_CADASTRAL').cast('integer')).select('CD_SIT_CADASTRAL','DS_SIT_CADASTRAL').distinct().show()

# COMMAND ----------

nomes = nomes_bruto \
  .fillna(0, ['frequency_female', 'frequency_male']) \
  .withColumn('prop_mulher', f.col('frequency_female')/(f.col('frequency_female') + f.col('frequency_male')))

estbl = estbl_bruto \
  .where(f.col('DS_MATRIZ_FILIAL') == 'Matriz') \
  .select('CD_CNPJ_BASICO', 'SG_UF', 'CD_SIT_CADASTRAL', 'CD_CNAE20_SUBCLASSE_PRINCIPAL') \
  .distinct() \
  .where(~((f.col('CD_CNPJ_BASICO').isin(['42938862'])) & (f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL') != '0115600')))

socios = socios_bruto \
  .withColumn('first_name', f.regexp_extract(f.col('NM_SOCIO_RAZAO_SOCIAL'), '^[A-Z]+', 0)) \
  .join(nomes, on = 'first_name', how = 'left') \
  .join(estbl, on = 'CD_CNPJ_BASICO', how = 'left')

media_mulheres = socios.select(f.mean(socios.prop_mulher)).collect()[0][0]

socios = (socios \
  .fillna(media_mulheres, 'prop_mulher') \
  .withColumn('prop_homem', 1 - f.col('prop_mulher')) \
  .withColumn('CNAE', f.regexp_extract(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'), '^.{2}', 0)) \
  .withColumn('ANO', f.year('DT_ENTRADA_SOCIEDADE')) \
  .withColumn('CD_SIT_CADASTRAL',f.col('CD_SIT_CADASTRAL').cast('integer'))
  .withColumn('cd_sit_cadastral', f.when(f.col('CD_SIT_CADASTRAL')==f.lit(2), f.lit(1)).otherwise(f.when(f.col('CD_SIT_CADASTRAL').isin([1,4,3,8]),f.lit(2)).otherwise(f.lit(3)))) \
  .withColumn('ds_sit_cadastral', f.when(f.col('CD_SIT_CADASTRAL')==f.lit(2), f.lit("Ativa")).otherwise(f.when(f.col('CD_SIT_CADASTRAL').isin([1,4,3,8]),f.lit("Inativa")).otherwise(f.lit("Não informado"))))
  .withColumn('ds_faixa_etaria_socio', f.when(f.col('CD_FAIXA_ETARIA_SOCIO').isin([1,2]), f.lit('Até 20 anos')).otherwise(
    f.when(f.col('CD_FAIXA_ETARIA_SOCIO')==f.lit(3), f.lit('21 a 30 anos')).otherwise(
      f.when(f.col('CD_FAIXA_ETARIA_SOCIO')==f.lit(4), f.lit('31 a 40 anos')).otherwise(
        f.when(f.col('CD_FAIXA_ETARIA_SOCIO')==f.lit(5), f.lit('41 a 50 anos')).otherwise(
          f.when(f.col('CD_FAIXA_ETARIA_SOCIO')==f.lit(6), f.lit('51 a 60 anos')).otherwise(
            f.when(f.col('CD_FAIXA_ETARIA_SOCIO')==f.lit(7), f.lit('61 a 70 anos')).otherwise(
              f.when(f.col('CD_FAIXA_ETARIA_SOCIO')==f.lit(8), f.lit('71 a 80 anos')).otherwise(
                f.when(f.col('CD_FAIXA_ETARIA_SOCIO')==f.lit(9), f.lit('Maiores de 80 anos')).otherwise(
                  f.lit('Não informado'))))))))))\
  .groupby('SG_UF', 'cd_sit_cadastral','ds_sit_cadastral' ,'CNAE', 'CD_FAIXA_ETARIA_SOCIO','ds_faixa_etaria_socio','ANO' ) \
  .agg(f.sum('prop_homem').alias('Masculino'), f.sum('prop_mulher').alias('Feminino')) \
  .unpivot(['SG_UF', 'cd_sit_cadastral','ds_sit_cadastral' ,'CNAE', 'CD_FAIXA_ETARIA_SOCIO','ds_faixa_etaria_socio','ANO'], ["Masculino", "Feminino"], "SEXO", "VALOR"))

# COMMAND ----------

socios=socios.toDF(*[c.lower() for c in socios.columns]).withColumnRenamed('cnae','cd_cnae').withColumnRenamed('valor','vl_socias').withColumnRenamed('sexo','ds_sexo').join(df_uf,'sg_uf','left').drop('sg_uf','ds_uf','dh_insertion_trs').withColumn('cd_sexo', f.when(f.col('ds_sexo')==f.lit('Feminino'), f.lit(2)).otherwise(f.when(f.col('ds_sexo')==f.lit('Masculino'), f.lit(1)))).select('ano','cd_uf','cd_cnae','cd_sit_cadastral','ds_sit_cadastral','cd_faixa_etaria_socio','ds_faixa_etaria_socio','cd_sexo','ds_sexo','vl_socias')

# COMMAND ----------

socios.columns

# COMMAND ----------

display(socios)

# COMMAND ----------

socios.select('cd_sit_cadastral','ds_sit_cadastral').distinct().show()

# COMMAND ----------

socios.select('cd_faixa_etaria_socio','ds_faixa_etaria_socio').distinct().show()

# COMMAND ----------

socios.coalesce(1).write.parquet(caminho_data_lake + '/uds/oni/observatorio_nacional/mulheres_industria_fnme/rfb', mode='overwrite')
