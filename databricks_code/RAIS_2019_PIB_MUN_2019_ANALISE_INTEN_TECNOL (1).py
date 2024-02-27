# Databricks notebook source
# DBTITLE 1,Análise - Relação Indústria e Crescimento econômico
# MAGIC %md
# MAGIC Análise descritiva de questões relacionadas ao crescimento econômico do país. Análise regional - Mapas!
# MAGIC Zonas Metropolitanas e não metropolitanas.
# MAGIC
# MAGIC Análise de presença setorial por Intensidade tecnológica a partir de classificação da OCDE. 
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

rais_vinculo.display()

# COMMAND ----------

rais_vinculo.display()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)

# COMMAND ----------

df.display()

# COMMAND ----------

windowSpec  = Window.partitionBy("salary",'gender')
df = df.withColumn('TOTAL_TRABALHADORES_CNAE_MUN', f.count(f.col('salary')).over(windowSpec)).withColumn('TOTAL_TR', f.count(f.col('salary')).over(windowSpec))
df.display()

# COMMAND ----------


"""
## IMPORTAÇÃO DA RAIS VINCULOS NO PERIODO. 
rais_vinculo_path_2019 = '{uri}/trs/me/rais_vinculo/ANO=2019'.format(uri=var_adls_uri)
rais_vinculo_2019 = (spark.read.parquet(rais_vinculo_path_2019)
                      .withColumn('ANO' , f.lit('2019')))
"""

#ANAELY: FAZER FILTRO DAS VARIÁVEIS JÁ AQUI E CALCULAR O TOTAL DE TRABLAHADORES

rais_vinculo_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)

rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN  = spark.read.parquet(rais_vinculo_path).select('ANO',"CD_CNAE20_DIVISAO",'CD_MUNICIPIO','FL_VINCULO_ATIVO_3112')
windowSpec  = Window.partitionBy('ANO',"CD_CNAE20_DIVISAO",'CD_MUNICIPIO')

#rais_vinculo_TOTAL_TRAB_MUNI = #spark.read.parquet(rais_vinculo_path).groupby('ANO','CD_MUNICIPIO').agg(f.count(f.col('FL_VINCULO_ATIVO_3112')).alias('TOTAL_TRABALHADORES_MUN')) 

rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN = rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN.withColumn('TOTAL_TRABALHADORES_MUN', f.count(f.col('FL_VINCULO_ATIVO_3112')).over(windowSpec))


rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN = rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN.groupby('ANO','CD_CNAE20_DIVISAO','CD_MUNICIPIO','TOTAL_TRABALHADORES_MUN').agg(f.count(f.col('FL_VINCULO_ATIVO_3112')).alias('TOTAL_TRABALHADORES_CNAE_MUN')).withColumn('INDICE_CNAE_MUN', f.round(f.col('TOTAL_TRABALHADORES_CNAE_MUN')/f.col('TOTAL_TRABALHADORES_MUN')*100,2))
            
#          
#          ,
#          f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('TOTAL_DIA_TRABALHADO'))\
#     .withColumn('INDICE_ABSENT', f.round(f.col('TOTAL_DIA_AFASTAMENTO')/f.col('TOTAL_DIA_TRABALHADO')*100,2))
#     .orderBy('ANO'))

# COMMAND ----------

rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN.display()

# COMMAND ----------

rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN.display()

# COMMAND ----------

windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()

# COMMAND ----------

windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()

# COMMAND ----------



rais_vinculo_CNAE_MUN = spark.read.parquet(rais_vinculo_path).groupby('ANO','CD_CNAE20_DIVISAO','CD_MUNICIPIO').agg(f.count(f.col('FL_VINCULO_ATIVO_3112')).alias('TOTAL_TRABALHADORES_CNAE_MUN')) 


rais_vinculo_CNAE_MUN_TOTAL_TRAB_MUN = (spark.read.parquet(rais_vinculo_path)
       .groupby('ANO','CD_CBO4')
       .agg(f.sum(f.col('VL_DIAS_AFASTAMENTO_TOT_CALC')).alias('TOTAL_DIA_AFASTAMENTO'),
            f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('TOTAL_DIA_TRABALHADO'))\
       .withColumn('INDICE_ABSENT', f.round(f.col('TOTAL_DIA_AFASTAMENTO')/f.col('TOTAL_DIA_TRABALHADO')*100,2))
       .orderBy('ANO'))

# COMMAND ----------


"""
## IMPORTAÇÃO DA RAIS VINCULOS NO PERIODO. 
rais_vinculo_path_2019 = '{uri}/trs/me/rais_vinculo/ANO=2019'.format(uri=var_adls_uri)
rais_vinculo_2019 = (spark.read.parquet(rais_vinculo_path_2019)
                      .withColumn('ANO' , f.lit('2019')))
"""

#ANAELY: FAZER FILTRO DAS VARIÁVEIS JÁ AQUI E CALCULAR O TOTAL DE TRABLAHADORES

rais_vinculo_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)

rais_vinculo_TOTAL_TRAB_MUNI = spark.read.parquet(rais_vinculo_path).groupby('ANO','CD_MUNICIPIO').agg(f.count(f.col('FL_VINCULO_ATIVO_3112')).alias('TOTAL_TRABALHADORES_MUN')) 

#for col in rais_vinculo.columns:
#     rais_vinculo = rais_vinculo.withColumn(col, rais_vinculo[col].cast('String'))



# COMMAND ----------

rais_vinculo_TOTAL_TRAB_MUNI.display()

# COMMAND ----------

[16:50] Rafael Silva e Sousa
o primeiro é que o considera a Cnae logo é saber por ano: (N° de trabalhadores por CNAE e município)/Nº de trabalhadores por município. 

[16:51] Rafael Silva e Sousa
o segundo é a Intensidade Tecnológica por ano: (N° de trabalhadores por Intensidade Tecnológica)/N° de trabalhadores por município 

[16:51] Zhang Yuan
okay

[16:51] Rafael Silva e Sousa
mais a frente trabalharemos com os estabelecimentos 

[16:51] Zhang Yuan
é o suficiente

[16:51] Rafael Silva e Sousa
mas a lógica vai ser a mesma... 

[16:51] Zhang Yuan
vou tentar okay, rafel

[16:51] Zhang Yuan
Deixa comigo

[16:51] Rafael Silva e Sousa
nesse script atual é só os vínculos.. 

[16:51] Rafael Silva e Sousa
Valeu demais!! 
 like 1

# COMMAND ----------

o primeiro é que o considera a Cnae logo é saber por ano: (N° de trabalhadores por CNAE e município)/Nº de trabalhadores por município.

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

for col in rais_vinculo.columns:
     rais_vinculo = rais_vinculo.withColumn(col, rais_vinculo[col].cast('String'))

# COMMAND ----------

rais_vinculo_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)

rais_vinculo_CNAE_MUN = spark.read.parquet(rais_vinculo_path).groupby('ANO','CD_CNAE20_DIVISAO','CD_MUNICIPIO').agg(f.count(f.col('FL_VINCULO_ATIVO_3112')).alias('TOTAL_TRABALHADORES_CNAE_MUN')) 

#for col in rais_vinculo.columns:
#     rais_vinculo = rais_vinculo.withColumn(col, rais_vinculo[col].cast('String'))

# COMMAND ----------

rais_vinculo_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
 
df = spark.read.parquet(rais_vinculo_path)

# COMMAND ----------

df.filter(f.col('ANO') == 2008).display()

# COMMAND ----------

#
#TOTAL_TRAB_MUNI = rais_vinculo_TOTAL_TRAB_MUNI.join(rais_vinculo_CNAE_MUN, (rais_vinculo_TOTAL_TRAB_MUNI.ANO == rais_vinculo_CNAE_MUN.ANO)
#               & (rais_vinculo_TOTAL_TRAB_MUNI.CD_MUNICIPIO == rais_vinculo_CNAE_MUN.CD_MUNICIPIO) & (rais_vinculo_TOTAL_TRAB_MUNI.CD_CNAE20_DIVISAO == rais_vinculo_CNAE_MUN.CD_CNAE20_DIVISAO)).show()

rais_vinculo_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)

rais_vinculo_TOTAL_TRAB_MUNI = spark.read.parquet(rais_vinculo_path).groupby('ANO','CD_MUNICIPIO').agg(f.count(f.col('FL_VINCULO_ATIVO_3112')).alias('TOTAL_TRABALHADORES_MUN')) 

rais_vinculo_CNAE_MUN = spark.read.parquet(rais_vinculo_path).groupby('ANO','CD_CNAE20_DIVISAO','CD_MUNICIPIO').agg(f.count(f.col('FL_VINCULO_ATIVO_3112')).alias('TOTAL_TRABALHADORES_CNAE_MUN')) 


TOTAL_TRAB_MUNI_CNAE = rais_vinculo_TOTAL_TRAB_MUNI.join(rais_vinculo_CNAE_MUN, (rais_vinculo_TOTAL_TRAB_MUNI.ANO == rais_vinculo_CNAE_MUN.ANO)
               & (rais_vinculo_TOTAL_TRAB_MUNI.CD_MUNICIPIO == rais_vinculo_CNAE_MUN.CD_MUNICIPIO))

#for col in rais_vinculo.columns:
#     rais_vinculo = rais_vinculo.withColumn(col, rais_vinculo[col].cast('String'))

# COMMAND ----------

TOTAL_TRAB_MUNI_CNAE = TOTAL_TRAB_MUNI_CNAE.withColumn('INDICE_CNAE_MUN', f.col('TOTAL_TRABALHADORES_CNAE_MUN')/f.col('TOTAL_TRABALHADORES_MUN'))

# COMMAND ----------

TOTAL_TRAB_MUNI_CNAE_DF = TOTAL_TRAB_MUNI_CNAE.drop('TOTAL_TRABALHADORES_MUN','CD_MUNICIPIO',"ANO")

# COMMAND ----------

# 2008 problema
TOTAL_TRAB_MUNI_CNAE.filter(f.col('CD_CNAE20_DIVISAO') == 'CL').display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

## IMPORTAR DADOS DA OCDE - INTENSIDADE TECNOLOGICA 
uds_ocde_int_tec_path = '{uri}/uds/uniepro/OCDE_inten_tec/Classificação_OCDE_Intensidade_tecnologica.csv'.format(uri = var_adls_uri)
df_ocde_int_tec_path = (spark.read.csv(uds_ocde_int_tec_path, sep = ";", header = True))

#for col in df_ocde_int_tec_path.columns:
#     df_ocde_int_tec_path = df_ocde_int_tec_path.withColumn(col, df_ocde_int_tec_path[col].cast('String'))

# COMMAND ----------

df_ocde_int_tec_path.display()   

# COMMAND ----------

#rais_vinculo_df_ocde_int_tec_path = rais_vinculo.join(df_ocde_int_tec_path, ['CD_CNAE20_DIVISAO'], how='right')

#rais_vinculo_df_ocde_int_tec_path = rais_vinculo.join(df_ocde_int_tec_path, df_ocde_int_tec_path.DS_CNAE20_DIV == #rais_vinculo.CD_CNAE20_DIVISAO).drop(df_ocde_int_tec_path.CD_CNAE20_DIVISAO)

#rais_vinculo_df_ocde_int_tec_path = rais_vinculo.alias("a").join(df_ocde_int_tec_path.alias("b"), rais_vinculo['CD_CNAE20_DIVISAO'] == #df_ocde_int_tec_path.alias['CD_CNAE20_DIVISAO']).select("a.id", "a.val1", "b.val2")

# COMMAND ----------

rais_vinculo_df_ocde_int_tec_path = rais_vinculo.join(df_ocde_int_tec_path, df_ocde_int_tec_path.CD_CNAE20_DIVISAO == rais_vinculo.CD_CNAE20_DIVISAO)

# COMMAND ----------

rais_vinculo_df_ocde_int_tec_path = rais_vinculo_df_ocde_int_tec_path.groupby('ANO','DS_CNAE20_DIV','INT_TEC_OCDE').agg(f.count(f.col('FL_VINCULO_ATIVO_3112').alias('TOTAL_TRABALHADORES_int_tec')))

# COMMAND ----------

rais_vinculo_df_ocde_int_tec_path.display()

# COMMAND ----------

## ## IMPORTAÇÃO DA RAIS VINCULOS NO PERIODO. 
## rais_vinculo_path_2019 = '{uri}/trs/me/rais_vinculo/ANO=2019'.format(uri=var_adls_uri)
## rais_vinculo_2019 = (spark.read.parquet(rais_vinculo_path_2019)
##                       .withColumn('ANO' , f.lit('2019')))
## 
## 
## 
## for col in rais_vinculo_2019.columns:
##      rais_vinculo_2019 = rais_vinculo_2019.withColumn(col, rais_vinculo_2019[col].cast('String'))
##     


#
#for col in rais_Mapa.columns:
#     rais_Mapa = rais_Mapa.withColumn(col, rais_Mapa[col].cast('String'))
#    
#rais_Mapa 
#

## rais_vinculo_2019

# COMMAND ----------



# COMMAND ----------

### IMPORTAR DADOS DO BANCO - UFU
uds_ind_cresc_eco_path = '{uri}/uds/uniepro/df_Ind_cresc_eco/raw/Dados_IND_Cresc.csv'.format(uri = var_adls_uri)

df_ind_cresc_eco = (spark.read.csv(uds_ind_cresc_eco_path, sep = ",", header = True)
                   .withColumnRenamed('ano', 'ANO')
                   .withColumnRenamed('cod_mun', 'cod_mun_7dg')
                   .withColumnRenamed('codig', 'CD_MUNICIPIO')
                   .withColumnRenamed('cod_uf', 'CD_UF')
                   .withColumnRenamed('uf', 'UF')
                   .select('ANO','munic','UF', 'pibpc_med', 'CD_MUNICIPIO', 'd_rgmt')
                   )


for col in df_ind_cresc_eco.columns:
     df_ind_cresc_eco = df_ind_cresc_eco.withColumn(col, df_ind_cresc_eco[col].cast('String'))
    
df_ind_cresc_eco    

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

Banco_unido = rais_vinculo.join(df_ocde_int_tec_path, ['CD_CNAE20_DIVISAO'], how='left')

# COMMAND ----------

Banco_unido.display()

# COMMAND ----------

###
Banco_unido_IT = (Banco_unido
                  .groupby('ANO','CD_MUNICIPIO','INT_TEC_OCDE')
                  .agg(f.count(f.lit(1)).alias('TOTAL_TRABALHADORES_IT_MUN')))

#Banco_unido_IT.display()



# COMMAND ----------

Banco_unido_IT = Banco_unido_IT.join(Banco_unido, ['ANO','CD_MUNICIPIO','INT_TEC_OCDE'], how='left')

# COMMAND ----------

##### JOIN ENTRE AS BASES 
###anco_unido = rais_vinculo_selected.join(df_ocde_int_tec_path, rais_vinculo_selected.CD_CNAE20_DIVISAO == df_ocde_int_tec_path.CD_CNAE20_DIVISAO, how='left')

###anco_unido = rais_vinculo_selected.join(df_ocde_int_tec_path, rais_vinculo_selected.CD_CNAE20_DIVISAO == df_ocde_int_tec_path.CD_CNAE20_DIVISAO, how='left')


###ataframe.join(dataframe1, (dataframe.ID1 == dataframe1.ID2)
###              & (dataframe.NAME1 == dataframe1.NAME2)).show()

# COMMAND ----------

Banco_unido = rais_vinculo.join(df_ocde_int_tec_path, ['CD_CNAE20_DIVISAO'], how='left')

# COMMAND ----------

Banco_unido_IT = (Banco_unido
                  .groupby('ANO','CD_MUNICIPIO','INT_TEC_OCDE')
                  .agg(f.count(f.lit(1)).alias('TOTAL_TRABALHADORES_IT_MUN')))

# COMMAND ----------

Banco_unido_IT = (Banco_unido
                  .groupby('ANO','CD_MUNICIPIO','INT_TEC_OCDE')
                  .agg(f.count(f.lit(1)).alias('TOTAL_TRABALHADORES_IT_MUN')))

# COMMAND ----------

Banco_unido_TOTAL = Banco_unido_IT.join(Banco_unido, ['ANO','CD_MUNICIPIO','INT_TEC_OCDE'], how='left')

# COMMAND ----------

Banco_unido_TOTAL.display()

# COMMAND ----------

Banco_unido_CNAE = (Banco_unido
                    .groupby('ANO','CD_MUNICIPIO','CD_CNAE20_DIVISAO')
                    .agg(f.count(f.lit(1)).alias('TOTAL_TRABALHADORES_CNAE_MUN')))

Banco_unido_CNAE = Banco_unido.join(Banco_unido_CNAE, ['ANO','CD_MUNICIPIO','CD_CNAE20_DIVISAO'], how='left')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

## JUNTAR AS INFORMAÇÕES COM O BANCO ENVIADO PELOS PROFESSORES - MANTENDO APENAS AS COLUNAS PRINCIPAIS
Banco_unido_IT_CNAE = Banco_unido_TOTAL.join(Banco_unido_CNAE, ['ANO','CD_MUNICIPIO', 'CD_UF','CD_CNAE20_DIVISAO', 'DS_CNAE20_DIV', 'INT_TEC_OCDE', 'FL_VINCULO_ATIVO_3112'], how='full')

# COMMAND ----------

Banco_unido_IT_CNAE = Banco_unido_IT_CNAE.distinct()
Banco_unido_IT_CNAE = Banco_unido_IT_CNAE.withColumnRenamed('DS_CNAE20_DIV', 'DS_CNAE20_DIV_Banco_unido_IT_CNAE')

# COMMAND ----------

Banco_unido_IT_CNAE = Banco_unido_IT_CNAE.distinct()
Banco_unido_IT_CNAE = Banco_unido_IT_CNAE.withColumnRenamed('DS_CNAE20_DIV', 'DS_CNAE20_DIV_Banco_unido_IT_CNAE')

# COMMAND ----------

Dados = Dados.distinct()

# COMMAND ----------

display(Dados)

# COMMAND ----------

(Banco_unido_IT
 .write
 .format('csv')
 .save(var_adls_uri + '/uds/uniepro/df_Ind_cresc_eco/raw/IT.csv', sep=";", header = True, mode='overwrite', encoding='latin1'))

# COMMAND ----------

(Banco_unido_IT_CNAE
 .write
 .format('csv')
 .save(var_adls_uri + '/uds/uniepro/df_Ind_cresc_eco/raw/Banco_unido_IT_CNAE_2008_2019.csv', sep=";", header = True, mode='overwrite', encoding='latin1'))

# COMMAND ----------

Dados.coalesce(1).write.format('csv').save(var_adls_uri + '/uds/uniepro/df_Ind_cresc_eco/raw/Dados', sep=";", header = True, mode='overwrite', encoding='latin1')

# COMMAND ----------

Dados.coalesce(1).write.format('parquet').save(var_adls_uri + '/uds/uniepro/df_Ind_cresc_eco/raw/Dados', sep=";", header = True, encoding='latin1')

# COMMAND ----------

(Dados.coalesce(1).write.format("csv")
    .option("header", True)
    .option('encoding', 'latin1')
    .save(var_adls_uri + '/uds/uniepro/df_Ind_cresc_eco/raw/Dados'))

# COMMAND ----------

