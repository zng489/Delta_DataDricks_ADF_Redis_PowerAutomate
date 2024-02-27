# Databricks notebook source
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/ms_sinan/ment/'.format(uri=var_adls_uri)
df_rais_ = spark.read.format("parquet").option("header","true").load(path)
df_rais.display()

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/ms_sinan/acgr/'.format(uri=var_adls_uri)
df_rais_ = spark.read.format("parquet").option("header","true").load(path)
df_rais.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Amigos, desculpem o horário.
# MAGIC A Presidência me.pediu o dado de estabalecimentos.industriais do Amapá. Mas.pediu CNPJ e Nome.
# MAGIC
# MAGIC Pelo site da.CNI, que usa RAIS 2020 existem 561 estabelecimentos.
# MAGIC
# MAGIC https://perfildaindustria.portaldaindustria.com.br/ranking?cat=3&id=3560
# MAGIC
# MAGIC Como não temos RAIS Identificada para 2020, avisei ao demandante que só temos a 2018.
# MAGIC
# MAGIC Assim, peço que pela manhã tentem tirar a solução. Não sei se já fizemos algo parecido(RO OU MT ou MS)
# MAGIC
# MAGIC Podemos usar o conceito IBGE de grupo indústria extrativa e transformação. Checar o que a CNI usou para chegar aos 561. Talvez ver com o Edson.

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/rais_estabelecimento/'.format(uri=var_adls_uri)
df_rais_ = spark.read.format("parquet").option("header","true").load(path)
df_rais.display()

# df_rais.select('CD_UF').distinct().collect()
# Nao ha valores de CD_UF

# COMMAND ----------

df_rais__ = (
  (
  df_rais_.filter(col('ANO') == 2018)
           ).filter(col('FL_IND_RAIS_NEGAT') == 0)
)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType

df_rais___ = (
  (
  ((((df_rais__.withColumn('CD_CNAE20_7_DIG', lpad(col('CD_CNAE20_SUBCLASSE'),7,'0'))
).withColumn('CD_CNAE20_2_DIG',f.substring(f.col('CD_CNAE20_7_DIG'),1,2))
).withColumn('CD_UF',f.substring(f.col('CD_MUNICIPIO'),1,2))).filter(col('CD_UF') == 16)).filter(col('QT_VINC_ATIV') >= 1)
).filter(   (col('CD_CNAE20_2_DIG') >= '05') & (col('CD_CNAE20_2_DIG') <= '43')  ).select( 'ANO', 'CD_UF', 'CD_CNAE20_2_DIG', 'ID_CNPJ_CEI', 'ID_CNPJ_RAIZ', 'ID_RAZAO_SOCIAL')
)


# COMMAND ----------

df_rais____ = (
  df_rais___.withColumn("ID_CNPJ_CEI",col("ID_CNPJ_CEI").cast(StringType()))
).withColumn("ID_CNPJ_RAIZ",col("ID_CNPJ_RAIZ").cast(StringType()))

# COMMAND ----------

df_rais____.display()

# COMMAND ----------

df_rais____DF = df_rais____.withColumn("ID_CNPJ_RAIZ",concat(lit('"'),col('ID_CNPJ_RAIZ'),lit('"')))
df_rais____DF.display()

# COMMAND ----------

(df_rais____.coalesce(1).write.format('csv').save(var_adls_uri + '/uds/uniepro/Estabelecimentos_Amapa_2018_621/', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1'))

# COMMAND ----------

df_rais___.count()

# COMMAND ----------

# CRUZAMENTO COM RAIS VINCULO PELO CNPJ PARA PEGAR CD_UF




# COMMAND ----------



# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo = spark.read.format("parquet").option("header","true").load(trs_rais_vinculo2008a2018_path)
df_rais_vinculo.display()

# COMMAND ----------

df_2008_2018_SEM_DUP_CPF_cbo_classificacoes = df_2008_2018_SEM_DUP_CPF.join(cbo_classificacoes, df_2008_2018_SEM_DUP_CPF.CD_CBO4 == cbo_classificacoes.COD_CBO4, how='left')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

trs/me/rais_estabelecimento/

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


#---------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------
###
# ABRINDO DADOS cbo_classificacoes
###

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
cbo_classificacoes_path = '{uri}/uds/uniepro/cbo_classificacoes/'.format(uri=var_adls_uri)
cbo_classificacoes = spark.read.format("csv").option("header","true").option('sep',';').load(cbo_classificacoes_path)
cbo_classificacoes = cbo_classificacoes.withColumn('COD_CBO4', lpad(col('COD_CBO4'),4,'0'))
cbo_classificacoes = cbo_classificacoes.dropDuplicates(['COD_CBO4'])
cbo_classificacoes = cbo_classificacoes.select(['COD_CBO4','DESC_CBO4'])


###
# ABRINDO DADOS raw_cnae_industrial
###

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
raw_cnae_industrial_path = '{uri}/raw/usr/uniepro/cnae_industrial/'.format(uri=var_adls_uri)
raw_cnae_industrial = spark.read.format("parquet").option("header","true").load(raw_cnae_industrial_path)
raw_cnae_industrial = raw_cnae_industrial.select('divisao','denominacao','agrupamento') 
raw_cnae_industrial  = raw_cnae_industrial.withColumn('divisao', lpad(col('divisao'),2,'0'))
#---------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------



#---------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------
###
# ABRINDO DADOS /trs/me/rais_vinculo
###

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo2008a2018 = spark.read.format("parquet").option("header","true").load(trs_rais_vinculo2008a2018_path)
#display(df_rais_vinculo2008a2018.limit(3))
# df = spark.read.parquet()
# df = spark.read.parquet.option('sep', ';')

# 2008,2009,2010,2011,2012,2013,2014,
ANOS = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018] 
 
df_2008_2018 = df_rais_vinculo2008a2018
df_2008_2018 = df_2008_2018.where(col('ANO').isin(ANOS))
 
df_2008_2018 = (
  df_2008_2018\
  .filter(col('FL_VINCULO_ATIVO_3112') == '1')\
  .withColumn('ID_CPF', lpad(col('ID_CPF'),11,'0'))\
  .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0'))\
  .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))\
)
 
df_2008_2018_group_by = (
  df_2008_2018\
  .groupBy('ID_CPF')\
  .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO'))\
  .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2008_2018.select('ID_CPF').count())\
  .orderBy('CPF_DUPLICADO', ascending=False)
)
 
  
# JOIN ENTRE A TABELA 'df_2008_2018' COM 'df_2008_2018_group_by'
# df_2008_2018_SEM_DUP_CPF
df_2008_2018_SEM_DUP_CPF = df_2008_2018.join(df_2008_2018_group_by, 'ID_CPF', how='right')


# JOIN ENTRE A TABELA 'df_2008_2018_SEM_DUP_CPF' COM 'cbo_classificacoes'
# df_2008_2018_SEM_DUP_CPF_cbo_classificacoes
df_2008_2018_SEM_DUP_CPF_cbo_classificacoes = df_2008_2018_SEM_DUP_CPF.join(cbo_classificacoes, df_2008_2018_SEM_DUP_CPF.CD_CBO4 == cbo_classificacoes.COD_CBO4, how='left')


# JOIN ENTRE A TABELA 'df_2008_2018_SEM_DUP_CPF_cbo_classificacoes' COM 'raw_cnae_industrial'
# df_2008_2018_SEM_DUP_CPF_cbo_classificacoes
df_2008_2018_SEM_DUP_CPF_cbo_classificacoes = df_2008_2018_SEM_DUP_CPF_cbo_classificacoes.join(raw_cnae_industrial, df_2008_2018_SEM_DUP_CPF_cbo_classificacoes.CD_CNAE20_DIVISAO == raw_cnae_industrial.divisao, how='left')
#---------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------

# COMMAND ----------

DF = (
  (
  df_2008_2018_SEM_DUP_CPF_cbo_classificacoes.filter(col('agrupamento') == 'Indústria')
     ).filter(col('CD_UF') == '16')
).filter(col('ANO') == 2018)

# COMMAND ----------

DF.display()

# COMMAND ----------



# COMMAND ----------

