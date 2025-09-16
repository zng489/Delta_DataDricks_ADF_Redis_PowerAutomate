# Databricks notebook source
# MAGIC %md
# MAGIC # 
# MAGIC
# MAGIC Esse notebook...
# MAGIC
# MAGIC Especialista: Danilo Severian <br>
# MAGIC Cientista de dados: Beatriz Bonato<br>
# MAGIC Card no Trello: https://trello.com/c/klT2V5te/1229-ceis-sustenta%C3%A7%C3%A3o-do-produto

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql import Window

# COMMAND ----------

df_rais_vinculos_bruto = spark.read.table("datalake__trs.oni.oni_mte_rais_identificada__rais_vinculo")
df_rais_estabelecimentos_bruto = spark.read.table("datalake__trs.oni.oni_mte_rais_identificada__rais_estabelecimento")
df_ipca_bruto = spark.read.table("datalake__trs.oni.ibge__ipca")
df_cbo_ceis = spark.read.csv('/Volumes/oni_lab/default/uds_oni_observatorio_nacional/complexo_industrial_saude/tab_auxiliar/cbo_ceis.csv', header=True, sep=";")
df_municipio = spark.read.table('datalake__trs.oni.oni_ibge__geo_municipios')

# COMMAND ----------

rais_vinc = df_rais_vinculos_bruto.withColumn('ID_CNPJ_RAIZ', f.lpad(f.regexp_replace(f.col('ID_CNPJ_RAIZ'), r'\s+', ''), 8, '0')).withColumn('ID_CNPJ_CEI', f.lpad(f.regexp_replace(f.col('ID_CNPJ_CEI'), r'\s+', ''), 14, '0'))
rais_estbl = df_rais_estabelecimentos_bruto.withColumn('ID_CNPJ_RAIZ', f.lpad(f.regexp_replace(f.col('ID_CNPJ_RAIZ'), r'\s+', ''), 8, '0')).withColumn('ID_CNPJ_CEI', f.lpad(f.regexp_replace(f.col('ID_CNPJ_CEI'), r'\s+', ''), 14, '0'))

# COMMAND ----------

#Filtros da RAIS estbl
rais_estbl = rais_estbl.filter((f.col('NR_QTD_VINCULOS_ATIVOS') > 1) & (f.col('FL_IND_RAIS_NEGAT') == False))

# COMMAND ----------

rais_vinc_industria = rais_vinc.filter(
    (f.col('FL_VINCULO_ATIVO_3112') == 1) & 
    (f.col('ANO') > 2007))

# COMMAND ----------

retira_zero = ['VL_REMUN_MEDIA_NOM', 'VL_REMUN_JANEIRO_NOM', 'VL_REMUN_FEVEREIRO_NOM','VL_REMUN_MARCO_NOM','VL_REMUN_ABRIL_NOM','VL_REMUN_MAIO_NOM','VL_REMUN_JUNHO_NOM','VL_REMUN_JULHO_NOM','VL_REMUN_AGOSTO_NOM','VL_REMUN_SETEMBRO_NOM','VL_REMUN_OUTUBRO_NOM','VL_REMUN_NOVEMBRO_NOM','VL_REMUN_DEZEMBRO_NOM']

rais_vinc_zero = rais_vinc_industria.na.fill(0, subset=retira_zero)

# COMMAND ----------

subclasses = [2110600, 2121101, 2121102, 2121103, 2123800, 2660400, 3250701, 3250702, 3250703, 3250704, 3250705, 3250707, 3292202]

rais_vinc_subclasse = rais_vinc_zero.filter(f.col('CD_CNAE20_SUBCLASSE').isin(subclasses))

# COMMAND ----------

rais_vinc_subclasse2 = rais_vinc_subclasse.select('ANO', 'ID_CNPJ_CEI', 'ID_CPF', 'CD_MUNICIPIO', 'CD_CBO', 'VL_REMUN_MEDIA_NOM', 'VL_REMUN_JANEIRO_NOM', 'VL_REMUN_FEVEREIRO_NOM','VL_REMUN_MARCO_NOM','VL_REMUN_ABRIL_NOM','VL_REMUN_MAIO_NOM','VL_REMUN_JUNHO_NOM','VL_REMUN_JULHO_NOM','VL_REMUN_AGOSTO_NOM','VL_REMUN_SETEMBRO_NOM','VL_REMUN_OUTUBRO_NOM','VL_REMUN_NOVEMBRO_NOM','VL_REMUN_DEZEMBRO_NOM', 'NR_MES_TEMPO_EMPREGO')

# COMMAND ----------

cnpj_atipico = ['00394429009914', '00394502007157', '17503475000101', '33485939000142', '33781055000135', '61189445000156', '77964393000188']

rais_vinc_cnpj = rais_vinc_zero.filter(f.col('ID_CNPJ_CEI').isin(cnpj_atipico))

# COMMAND ----------

rais_vinc_cnpj2 = rais_vinc_cnpj.select('ANO', 'ID_CNPJ_CEI', 'ID_CPF', 'CD_MUNICIPIO', 'CD_CBO', 'VL_REMUN_MEDIA_NOM','VL_REMUN_JANEIRO_NOM', 'VL_REMUN_FEVEREIRO_NOM','VL_REMUN_MARCO_NOM','VL_REMUN_ABRIL_NOM','VL_REMUN_MAIO_NOM','VL_REMUN_JUNHO_NOM','VL_REMUN_JULHO_NOM','VL_REMUN_AGOSTO_NOM','VL_REMUN_SETEMBRO_NOM','VL_REMUN_OUTUBRO_NOM','VL_REMUN_NOVEMBRO_NOM','VL_REMUN_DEZEMBRO_NOM', 'NR_MES_TEMPO_EMPREGO')

# COMMAND ----------

rais_vinc_cbo = rais_vinc_subclasse2.union(rais_vinc_cnpj2)

# COMMAND ----------

rais_vinc_cbo_filtrado = (rais_vinc_cbo.join(rais_estbl, [rais_vinc_cbo['ID_CNPJ_CEI'] == rais_estbl['ID_CNPJ_CEI'], rais_vinc_cbo['ANO'] == rais_estbl['NR_ANO']],
        'inner').select(rais_vinc_cbo['*']))

# COMMAND ----------

# Junta com a base de municipios
df_municipio = df_municipio.withColumn('ds_municipio_uf', f.split(df_municipio['ds_municipio_uf'], '/')[0])

rais_vinc_cbo2 = rais_vinc_cbo_filtrado.join(df_municipio, rais_vinc_cbo_filtrado['CD_MUNICIPIO'] == df_municipio['cd_ibge_6'],'left').select(rais_vinc_cbo_filtrado['*'], df_municipio['ds_municipio_uf'], df_municipio['nm_microrregiao'], df_municipio['ds_uf'], df_municipio['nm_regiao'])

# COMMAND ----------

rais_vinc_cbo3 = rais_vinc_cbo2.join(df_cbo_ceis, rais_vinc_cbo2['CD_CBO'] == df_cbo_ceis['COD_CBO'], 'left').select(rais_vinc_cbo2['*'], df_cbo_ceis['DESC_CLAS_ONI'])

# COMMAND ----------

display(rais_vinc_cbo3)

# COMMAND ----------

rais_vinc_cbo4 = rais_vinc_cbo3.withColumn('VL_MASSA_SALARIAL', f.when(f.col('ANO') >= 2015, f.col('VL_REMUN_JANEIRO_NOM') + f.col('VL_REMUN_FEVEREIRO_NOM') + f.col('VL_REMUN_MARCO_NOM') + f.col('VL_REMUN_ABRIL_NOM') + f.col('VL_REMUN_MAIO_NOM') + f.col('VL_REMUN_JUNHO_NOM') + f.col('VL_REMUN_JULHO_NOM') + f.col('VL_REMUN_AGOSTO_NOM') + f.col('VL_REMUN_SETEMBRO_NOM') + f.col('VL_REMUN_OUTUBRO_NOM') + f.col('VL_REMUN_NOVEMBRO_NOM') + f.col('VL_REMUN_DEZEMBRO_NOM')).otherwise(f.col('VL_REMUN_MEDIA_NOM') * f.when(f.col('NR_MES_TEMPO_EMPREGO') >= 12, f.lit(12)).otherwise(f.col('NR_MES_TEMPO_EMPREGO'))))

# COMMAND ----------

rais_vinc_cbo5 = rais_vinc_cbo4.groupBy('ANO', 'DESC_CLAS_ONI', 'nm_microrregiao', 'ds_uf', 'nm_regiao'
).agg(f.countDistinct('ID_CPF').alias('CPF_distinto'), f.avg('VL_REMUN_MEDIA_NOM').alias('VL_REMUN_MEDIA_NOM'),
    f.sum('VL_MASSA_SALARIAL').alias('VL_MASSA_SALARIAL'))

# COMMAND ----------

rais_vinc_cbo6 = rais_vinc_cbo5.withColumn('DESC_CLAS_ONI', f.when(rais_vinc_cbo4['DESC_CLAS_ONI'].isNull(), 'CBO não declarada').otherwise(rais_vinc_cbo4['DESC_CLAS_ONI']))

# COMMAND ----------

window_spec = Window.partitionBy('nm_microrregiao', 'ANO')
rais_vinc_cbo7 = rais_vinc_cbo6.withColumn('CPF_distinto_micro', f.sum('CPF_distinto').over(window_spec))

# COMMAND ----------

window_spec2 = Window.partitionBy('DESC_CLAS_ONI', 'ANO')
rais_vinc_cbo8 = rais_vinc_cbo7.withColumn('CPF_distinto_cbo', f.sum('CPF_distinto').over(window_spec2))

# COMMAND ----------

window_spec3 = Window.partitionBy('DESC_CLAS_ONI', 'ANO')
rais_vinc_cbo9 = rais_vinc_cbo8.withColumn('VL_MASSA_MICRO_CBO', f.sum('VL_MASSA_SALARIAL').over(window_spec3))

# COMMAND ----------

window_spec4 = Window.partitionBy('nm_microrregiao', 'ANO')
rais_vinc_cbo10 = rais_vinc_cbo9.withColumn('VL_MASSA_MICRO', f.sum('VL_MASSA_SALARIAL').over(window_spec4))

# COMMAND ----------

window_spec_ano = Window.partitionBy('ANO')

rais_vinc_cbo11 = rais_vinc_cbo10.withColumn('CPF_distinto_ano', f.sum('CPF_distinto').over(window_spec_ano)
).withColumn('MASSA_ANO', f.sum('VL_MASSA_SALARIAL').over(window_spec_ano))

# COMMAND ----------

rais_vinc_cbo12 = rais_vinc_cbo11.withColumn('QL_VINC_CBO', (f.col('CPF_distinto') / f.col('CPF_distinto_micro')) / (f.col('CPF_distinto_cbo') / f.col('CPF_distinto_ano')))

# COMMAND ----------

####
rais_vinc_cbo13 = rais_vinc_cbo12.withColumn('QL_MASSA_CBO', (f.col('VL_MASSA_SALARIAL') / f.col('VL_MASSA_MICRO')) / (f.col('VL_MASSA_MICRO_CBO') / f.col('MASSA_ANO')))

# COMMAND ----------

df_final = rais_vinc_cbo13.withColumnRenamed('ANO', 'Ano') \
    .withColumnRenamed('DESC_CLAS_ONI', 'Classificação Ocupacional') \
    .withColumnRenamed('nm_microrregiao', 'Microrregião') \
    .withColumnRenamed('ds_uf', 'UF') \
    .withColumnRenamed('nm_regiao', 'Macrorregião') \
    .withColumnRenamed('CPF_distinto', 'Vínculos da Classe Ocupacional na Microrregião') \
    .withColumn('Média Salarial da Classe Ocupacional na Microrregião', f.round('VL_REMUN_MEDIA_NOM', 2)) \
    .withColumn('Massa Salarial da Classe Ocupacional na Microrregião', f.round('VL_MASSA_SALARIAL', 2)) \
    .withColumnRenamed('CPF_distinto_micro', 'Vìnculos do CEIS na Microrregião') \
    .withColumnRenamed('CPF_distinto_cbo', 'Vínculos na Classificação Ocupacional (Brasil)') \
    .withColumn('Massa Salarial da Classe Ocupacional (Brasil)', f.round('VL_MASSA_MICRO_CBO', 2)) \
    .withColumn('Massa Salarial do CEIS na Microrregião', f.round('VL_MASSA_MICRO', 2)) \
    .withColumn('QL Vínculos na Classe Ocupacional', f.round('QL_VINC_CBO', 8)) \
    .withColumn('QL Massa Salarial da Classe Ocupacional', f.round('QL_MASSA_CBO', 8)) \
    .drop('VL_REMUN_MEDIA_NOM') \
    .drop('MASSA_ANO') \
    .drop('VL_MASSA_SALARIAL') \
    .drop('VL_MASSA_MICRO_CBO') \
    .drop('VL_MASSA_MICRO') \
    .drop('CPF_distinto_ano') \
    .drop('QL_VINC_CBO') \
    .drop('QL_MASSA_CBO') 

# COMMAND ----------

# Salvando resultados
path_resultado_parquet = "/Volumes/oni_lab/default/uds_oni_observatorio_nacional/complexo_industrial_saude/ql_vinculos"
df_final.coalesce(1).write.parquet(path_resultado_parquet, mode='overwrite')