# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



DF = spark.read.format("parquet").option("header","true").option('sep', ',').option('encoding', 'ISO-8859-1').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/me/rais_vinculo/')
DF.createOrReplaceTempView('RAIS_TABLE')
#display(DF.limit(5))

# COMMAND ----------

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM RAIS_TABLE 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create Table MyTempTable_1 
# MAGIC (
# MAGIC   SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)
# MAGIC )

# COMMAND ----------

df_2008_2018 = spark.sql(" SELECT * FROM (SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)) WHERE FL_VINCULO_ATIVO_3112='1' ")

# COMMAND ----------


df_2008_2018 = spark.sql("SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)")
df_2008_2018.createOrReplaceTempView('df_2008_2018')

# COMMAND ----------


from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



DF = spark.read.format("parquet").option("header","true").option('sep', ',').option('encoding', 'ISO-8859-1').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/me/rais_vinculo/')
DF.createOrReplaceTempView('RAIS_TABLE')

df_2008_2018 = spark.sql(" SELECT * FROM (SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)) WHERE FL_VINCULO_ATIVO_3112='1' ")

df_2008_2018 = spark.sql("SELECT * FROM RAIS_TABLE WHERE ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)")
df_2008_2018.createOrReplaceTempView('df_2008_2018')

df_2008_2018 = spark.sql("SELECT * FROM df_2008_2018 WHERE FL_VINCULO_ATIVO_3112='1'")
df_2008_2018.createOrReplaceTempView('df_2008_2018')

df_2008_2018 = spark.sql("SELECT *, LPAD(ID_CPF, 11, '0') AS LPAD_ID_CPF FROM df_2008_2018")
df_2008_2018.createOrReplaceTempView('df_2008_2018')

df_2008_2018 = spark.sql("SELECT *, LPAD(ID_CNPJ_CEI, 14, '0') AS LPAD_ID_CNPJ_CEI FROM df_2008_2018")
df_2008_2018.createOrReplaceTempView('df_2008_2018')

df_2008_2018 = spark.sql("SELECT *, SUBSTRING(CD_CNAE20_SUBCLASSE, 1, 2) AS SUBSTRING_CD_CNAE20_SUBCLASSE FROM df_2008_2018")
df_2008_2018.createOrReplaceTempView('df_2008_2018')

# COMMAND ----------

df_2008_2018_ = spark.sql("SELECT ID_CPF, COUNT(DISTINCT LPAD_ID_CNPJ_CEI) AS CPF_DUPLICADO FROM df_2008_2018 GROUP BY ID_CPF")
df_2008_2018_.createOrReplaceTempView('df_2008_2018_')

df_2008_2018_.display()

# COMMAND ----------

df_2008_2018_ = spark.sql("SELECT ID_CPF, COUNT(DISTINCT LPAD_ID_CNPJ_CEI) AS CPF_DUPLICADO FROM df_2008_2018 GROUP BY ID_CPF")
df_2008_2018_.createOrReplaceTempView('df_2008_2018_')

df_2008_2018_.display()

# COMMAND ----------

spark.sql("SELECT *, ID_CPF AS CPF_DUPLICADO FROM df_2008_2018_").display()



# COMMAND ----------

#>> SELECT (SELECT COUNT(*) FROM MyTable WHERE ID = 1 AND Flag = 1) * 100 / 
#>>       (SELECT COUNT(*) FROM MyTable WHERE ID = 1)

# COMMAND ----------

spark.sql("SELECT ID_CPF FROM df_2008_2018") 

# COMMAND ----------

df_2008_2018_ = spark.sql("SELECT ID_CPF, COUNT(DISTINCT LPAD_ID_CNPJ_CEI) AS CPF_DUPLICADO FROM df_2008_2018 GROUP BY ID_CPF")

df_2008_2018_.display()

# COMMAND ----------

# Importando arquivo cbo_classificacoes.csv
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
cbo_classificacoes_path = '{uri}/uds/uniepro/cbo_classificacoes/'.format(uri=var_adls_uri)
cbo_classificacoes = spark.read.format("csv").option("header","true").option('sep',';').load(cbo_classificacoes_path)
cbo_classificacoes = cbo_classificacoes.withColumn('COD_CBO4', lpad(col('COD_CBO4'),4,'0'))
cbo_classificacoes = cbo_classificacoes.dropDuplicates(['COD_CBO4'])
cbo_classificacoes = cbo_classificacoes.select(['COD_CBO4','DESC_CBO4'])

# Importando arquivo cnae_industrial.parquet
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
raw_cnae_industrial_path = '{uri}/raw/usr/uniepro/cnae_industrial/'.format(uri=var_adls_uri)
raw_cnae_industrial = spark.read.format("parquet").option("header","true").load(raw_cnae_industrial_path)
raw_cnae_industrial = raw_cnae_industrial.select('divisao','denominacao','agrupamento') 
raw_cnae_industrial  = raw_cnae_industrial.withColumn('divisao', lpad(col('divisao'),2,'0'))

# Importando arquivo rais_vinculo.parquet
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo2008a2018 = spark.read.format("parquet").option("header","true").load(trs_rais_vinculo2008a2018_path)

# Selecionando anos 2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018 da Rais 
ANOS = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018] 
df_2008_2018 = df_rais_vinculo2008a2018
df_2008_2018 = df_2008_2018.where(col('ANO').isin(ANOS))

# Condições estabelecidos:
#### .filter(col('FL_VINCULO_ATIVO_3112') == '1') ======>  Selecioando apenas vinculos 1
#### .withColumn('ID_CPF', lpad(col('ID_CPF'),11,'0')) ======> Corrigindo os zeros (11) a esquerda nos CPF's
#### .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0')) ======> Corrigindo os zeros (14) a esquerda nos CPF's
#### .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2)) ======> Selecionado os dois primeiros digítos da CNAE
df_2008_2018 = (
  df_2008_2018\
  .filter(col('FL_VINCULO_ATIVO_3112') == '1')\
  .withColumn('ID_CPF', lpad(col('ID_CPF'),11,'0'))\
  .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0'))\
  .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))\
)
  

# Condições estabelecidos:
#### .groupby('ID_CPF')
#### .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO')) =====> Contando CPF`s duplicados
#### .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2008_2018.select('ID_CPF').count()) =====> Porcentagem de CPF`s duplicados
df_2008_2018_group_by = (
  df_2008_2018\
  .groupBy('ID_CPF')\
  .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO'))\
  .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2008_2018.select('ID_CPF').count())
)

# COMMAND ----------

df_2008_2018_group_by.display()

# COMMAND ----------

df_2008_2018_group_by = (
    df_2008_2018\
    .groupBy('ID_CPF')\
    .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO'))\
    .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2008_2018.select('ID_CPF').count())
  )

# COMMAND ----------

df_2008_2018.display()

# COMMAND ----------

