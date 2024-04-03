# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/uds/uniepro/planilha_catalogo/'.format(uri=var_adls_uri)
Q = spark.read.format("csv").option("header","true").option("encoding", "ISO-8859-1").option('sep',';').load(path)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ano, ID_CPF, count(ID_CPF) as QTDE from observatorio_fiesc
# MAGIC where ano = 2012
# MAGIC group by ano, ID_CPF
# MAGIC having count(ID_CPF) > 1
# MAGIC order by ano asc, QTDE desc 

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/biz/uniepro/fta_rfb_cno/cno_biz/'.format(uri=var_adls_uri)
Q = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").option('sep',';').load(path)
Q.display()

# COMMAND ----------

biz/uniepro/fta_rfb_cno/cno_biz/

# COMMAND ----------

Q.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/rais_vinculo/'.format(uri=var_adls_uri)
observatorio_fiesc = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)

# COMMAND ----------

observatorio_fiesc.display()


# COMMAND ----------

(observatorio_fiesc.filter(col('ANO') == 2018)).select('DT_DIA_MES_ANO_DIA_DESLIGAMENTO').distinct().collect()

# COMMAND ----------

observatorio_fiesc.select()

# COMMAND ----------

# MAGIC %md 
# MAGIC # when and otherwise

# COMMAND ----------

observatorio_fiesc.withColumn('VL_REMUN_MEDIA_NOM_2010',
                              f.when( (f.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') < 510.00), 'Menor que R$510')
                              .otherwise(0)
                             )

# COMMAND ----------

observatorio_fiesc.withColumn('VL_REMUN_MEDIA_NOM_2010',
                              f.when( (f.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') < 510.00), 'Menor que R$510')
                              .otherwise(f.when( (f.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') <= 510.00) & (f.col('VL_REMUN_MEDIA_NOM') >= 1275 ), 'Entre R$ 510 e 1275')
                              .otherwise(0))
                             )

# COMMAND ----------

0 = f.when( (F.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') <= 510.00) & (f.col('VL_REMUN_MEDIA_NOM') >= 1275 ), 'Entre R$ 510 e 1275')
                              .otherwise(0)
  
  
  

# COMMAND ----------

observatorio_fiesc.withColumn('VL_REMUN_MEDIA_NOM_2010',
                              f.when( (F.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') < 510.00), 'Menor que R$510')
                              .otherwise(f.when( (F.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') <= 510.00) & (f.col('VL_REMUN_MEDIA_NOM') >= 1275 ), 'Entre R$ 510 e 1275')
                              .otherwise(f.when( (F.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') <= 510.00) & (f.col('VL_REMUN_MEDIA_NOM') >= 1275 ), 'Entre R$ 510 e 1275')
                              .otherwise(f.when( (F.col('ANO') == 2010) & (f.col('VL_REMUN_MEDIA_NOM') <= 510.00) & (f.col('VL_REMUN_MEDIA_NOM') >= 1275 ), 'Entre R$ 510 e 1275')
                              .otherwise(0))))
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC # ----------

# COMMAND ----------

# MAGIC %md
# MAGIC # Apllying UDF function with more than one parameters

# COMMAND ----------


from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

def func(ano, x, vl_remun):
    if ano == x and vl_remun < 510.00:
        return 'Menor que R$510'
    if ano == x and 510.00 <= vl_remun <= 1275:
        return 'Entre R$510 e R$1275'
      
    if ano == x and 1275.01 <= vl_remun <= 2550:
        return 'Entre R$1275.01 e R$2550'
      
    if ano == x and 2550.01 <= vl_remun <= 5100:
        return 'Entre R$1275.01 e R$2550'
      
    if ano == x and vl_remun > 5100.01:
        return 'Entre R$1275.01 e R$2550'
    return 0

func_udf = udf(func, IntegerType())
df = observatorio_fiesc.withColumn('new_column',func_udf(observatorio_fiesc['ANO'], lit(2008), observatorio_fiesc['VL_REMUN_MEDIA_NOM']))

# COMMAND ----------

# MAGIC %md
# MAGIC # Rais_Vinculo

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/rais_vinculo/'.format(uri=var_adls_uri)
observatorio_fiesc = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)


@udf
def return_string(VL_IDADE):
    if VL_IDADE >= 14 and VL_IDADE <= 17:
        return 'FAIXA 1'
      
    elif VL_IDADE >= 18 and VL_IDADE <= 24:
        return 'FAIXA 2'
      
    elif VL_IDADE >= 25 and VL_IDADE <= 29:
        return 'FAIXA 3'
      
    elif VL_IDADE >= 30 and VL_IDADE <= 35:
        return 'FAIXA 4'
      
    elif VL_IDADE >= 36 and VL_IDADE <= 40:
        return 'FAIXA 5'
      
    elif VL_IDADE >= 41 and VL_IDADE <= 50:
        return 'FAIXA 6'
      
    elif VL_IDADE >= 51 and VL_IDADE <= 65:
        return 'FAIXA 7'
      
    elif VL_IDADE > 65:
        return 'FAIXA 8'
    else:
        return 'NDA'
## +---+---+---+------+

@udf
def return_VL_REMUN_MEDIA_SM(VL_REMUN_MEDIA_SM):
    if VL_REMUN_MEDIA_SM < 1:
        return 'Menor que 1 SM'
      
    elif VL_REMUN_MEDIA_SM >= 1 and VL_REMUN_MEDIA_SM <= 2.59:
        return 'Entre 1 SM e 2.5 SM'
      
    elif VL_REMUN_MEDIA_SM >= 2.6 and VL_REMUN_MEDIA_SM <= 5:
        return 'Entre 2.6 SM e 5 SM'
      
    elif VL_REMUN_MEDIA_SM >= 5.1 and VL_REMUN_MEDIA_SM <= 10:
        return 'Entre 5.1 SM e 10 SM'
      
    elif VL_REMUN_MEDIA_SM > 10:
        return 'Acima de 10 SM'
      
    else:
        return 'NDA'
## +---+---+---+------+

@udf
def return_VL_REMUN_DEZEMBRO_SM(VL_REMUN_DEZEMBRO_SM):
    if VL_REMUN_DEZEMBRO_SM < 1:
        return 'Menor que 1 SM'
      
    elif VL_REMUN_DEZEMBRO_SM >= 1 and VL_REMUN_DEZEMBRO_SM <= 2.59:
        return 'Entre 1 SM e 2.5 SM'
      
    elif VL_REMUN_DEZEMBRO_SM >= 2.6 and VL_REMUN_DEZEMBRO_SM <= 5:
        return 'Entre 2.6 SM até 5 SM'
      
    elif VL_REMUN_DEZEMBRO_SM >= 5.1 and VL_REMUN_DEZEMBRO_SM <= 10:
        return 'Entre 5.1 SM e 10 SM'
      
    elif VL_REMUN_DEZEMBRO_SM > 10:
        return 'Acima de 10 SM'
      
    else:
        return 'NDA'
## +---+---+---+------+


@udf
def func(ano, x,y,a,b,c,d,e,f,g,vl_remun):
    if ano == x and vl_remun < 510.00:
        return 'Menor que R$510.00'
    if ano == x and 510.00 <= vl_remun <= 1275:
        return 'Entre R$510.00 e R$1275.00'
    if ano == x and 1275.01 <= vl_remun <= 2550:
        return 'Entre R$1275.01 e R$2550.00'
    if ano == x and 2550.01 <= vl_remun <= 5100.00:
        return 'Entre R$2550.01 e R$5100.00'
    if ano == x and vl_remun > 5100.01:
        return 'Acima de R$5100.01'
      
    if ano == y and vl_remun < 545.00:
        return 'Menor que R$545.00'
    if ano == y and 545.00 <= vl_remun <= 1362.50:
        return 'Entre R$545.00 e R$1362.50'
    if ano == y and 1362.51 <= vl_remun <= 2725.00:
        return 'Entre R$1362.51 e R$2725.00'
    if ano == y and 2725.01 <= vl_remun <= 5450:
        return 'Entre R$2725.01 e R$5450.00'
    if ano == y and vl_remun > 5450.01:
        return 'Acima de R$5450.01'
      
    if ano == a and vl_remun < 622.00:
        return 'Menor que R$622.00'
    if ano == a and 622.00 <= vl_remun <= 1555.00:
        return 'Entre R$545.00 e R$1362.50'
    if ano == a and 1555.01 <= vl_remun <= 3110.00:
        return 'Entre R$1362.51 e R$2725.00'
    if ano == a and 3110.01 <= vl_remun <= 6220.00:
        return 'Entre R$3110.00.01 e R$6220.00'
    if ano == a and vl_remun > 6220.01:
        return 'Acima de R$6220.01'
      
    if ano == b and vl_remun < 678.00:
        return 'Menor que R$678.00'
    if ano == b and 678.00 <= vl_remun <= 1695.00:
        return 'Entre R$545.00 e R$1695.00'
    if ano == b and 1695.01 <= vl_remun <= 3390.00:
        return 'Entre R$1695.01 e R$3390.00'
    if ano == b and 3390.01 <= vl_remun <= 6780.00:
        return 'Entre R$3390.01 e R$6780.00'
    if ano == b and vl_remun > 6780.01:
        return 'Acima de R$6780.01'
      
    if ano == c and vl_remun < 724.00:
        return 'Menor que R$724.00'
    if ano == c and 724.01 <= vl_remun <= 1810.00:
        return 'Entre R$724.01 e R$1810.00'
    if ano == c and 1810.01 <= vl_remun <= 3620.00:
        return 'Entre R$1810.01 e R$3620.00'
    if ano == c and 3620.01 <= vl_remun <= 7240.00:
        return 'Entre R$3620.01 e R$7240.00'
    if ano == c and vl_remun > 7240.01:
        return 'Acima de R$7240.01'
      
    if ano == d and vl_remun < 788.00:
        return 'Menor que R$788.00'
    if ano == d and 788.01 <= vl_remun <= 1970.00:
        return 'Entre R$788.01 e R$1970.00'
    if ano == d and 1970.01 <= vl_remun <= 3940.00:
        return 'Entre R$1970.01 e R$3940.00'
    if ano == d and 3940.01 <= vl_remun <= 7880.00:
        return 'Entre R$3940.01 e R$7880.00'
    if ano == d and vl_remun > 7880.01:
        return 'Acima de R$7880.01'
      
    if ano == e and vl_remun < 880.00:
        return 'Menor que R$880.00'
    if ano == e and 880.01 <= vl_remun <= 2200.00:
        return 'Entre R$880.01 e R$2200.00'
    if ano == e and 2200.01 <= vl_remun <= 4400.00:
        return 'Entre R$2200.01 e R$4400.00'
    if ano == e and 4400.01 <= vl_remun <= 8800.00:
        return 'Entre R$4400.01 e R$8800.00'
    if ano == e and vl_remun > 8800.01:
        return 'Acima de R$8800.01'
      
    if ano == f and vl_remun < 937.00:
        return 'Menor que R$937.00'
    if ano == f and 937.01 <= vl_remun <= 2342.50:
        return 'Entre R$937.01 e R$2342.50'
    if ano == f and 2342.51 <= vl_remun <= 4685.00:
        return 'Entre R$2342.51 e R$4685.00'
    if ano == f and 4685.01 <= vl_remun <= 9370.00:
        return 'Entre R$4685.01 e R$9370.00'
    if ano == f and vl_remun > 9370.01:
        return 'Acima de R$9370.01'
      
    if ano == g and vl_remun < 954.00:
        return 'Menor que R$954.00'
    if ano == g and 954.00 <= vl_remun <= 2385.00:
        return 'Entre R$954.00 e R$2385.00'
    if ano == g and 2385.01 <= vl_remun <= 4770.00:
        return 'Entre R$2385.01 e R$4770.00'
    if ano == g and 4770.01 <= vl_remun <= 9540.00:
        return 'Entre R$4770.01 e R$9540.00'
    if ano == g and vl_remun > 9540.01:
        return 'Acima de R$9540.01'
  
    return 'NDA'
  

df = (
  (
  observatorio_fiesc.filter(col("CD_UF").isin([41, 42, 43]))\
    .filter(col("ANO").isin([2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]))\
  #.withColumn('ID_CPF', sha2(observatorio_fiesc['ID_CPF'],256))\
  #.withColumn('ID_CNPJ_CEI', sha2(observatorio_fiesc['ID_CNPJ_CEI'],256))\
  .withColumn('FAIXA_VL_IDADE', return_string(col('VL_IDADE')))\
  .withColumn('FAIXA_VL_REMUN_MEDIA_SM', return_VL_REMUN_MEDIA_SM(col('VL_REMUN_MEDIA_SM')))\
  .withColumn('FAIXA_VL_REMUN_DEZEMBRO_SM', return_VL_REMUN_DEZEMBRO_SM(col('VL_REMUN_DEZEMBRO_SM')))\
  .withColumn('VL_REMUN_MEDIA_NOM_CATEGORIZAR',func(observatorio_fiesc['ANO'], lit(2010), lit(2011), lit(2012), lit(2013), lit(2014), lit(2015), lit(2016), lit(2017), lit(2018), observatorio_fiesc['VL_REMUN_MEDIA_NOM']))\
  .withColumn('VL_REMUN_DEZEMBRO_NOM_CATEGORIZAR',func(observatorio_fiesc['ANO'], lit(2010), lit(2011), lit(2012), lit(2013), lit(2014), lit(2015), lit(2016), lit(2017), lit(2018), observatorio_fiesc['VL_REMUN_DEZEMBRO_NOM']))
)).select('ANO','CD_UF','ID_CPF','ID_CNPJ_CEI',
          'CD_GRAU_INSTRUCAO','CD_CBO',
          'CD_CNAE20_SUBCLASSE','FAIXA_VL_IDADE','VL_IDADE','FAIXA_VL_REMUN_MEDIA_SM','VL_REMUN_MEDIA_SM',
          'FAIXA_VL_REMUN_DEZEMBRO_SM','VL_REMUN_DEZEMBRO_SM','CD_MUNICIPIO','CD_MUNICIPIO_TRAB',
          'CD_SEXO','CD_TAMANHO_ESTABELECIMENTO','NR_MES_TEMPO_EMPREGO', 'FL_VINCULO_ATIVO_3112', 'CD_MES_DESLIGAMENTO', 'CD_MOTIVO_DESLIGAMENTO', 'CD_TIPO_VINCULO', 'CD_TIPO_SALARIO','CD_TIPO_ADMISSAO','CD_NATUREZA_JURIDICA', 'VL_REMUN_MEDIA_NOM', 'VL_REMUN_MEDIA_NOM_CATEGORIZAR','VL_REMUN_DEZEMBRO_NOM', 'VL_REMUN_DEZEMBRO_NOM_CATEGORIZAR', 'DT_DIA_MES_ANO_DATA_ADMISSAO')


# COMMAND ----------

ID_CPF_HASH = (
  (
  observatorio_fiesc.select('ID_CPF')
).dropDuplicates(['ID_CPF'])
).withColumn('ID_CPF_HASH', sha2(observatorio_fiesc['ID_CPF'],256))


ID_CNPJ_CEI_HASH = (
  (
  observatorio_fiesc.select('ID_CNPJ_CEI')
).dropDuplicates(['ID_CNPJ_CEI'])
).withColumn('ID_CNPJ_CEI_HASH', sha2(observatorio_fiesc['ID_CNPJ_CEI'],256))

# COMMAND ----------

df_ = df.join(ID_CPF_HASH, df.ID_CPF == ID_CPF_HASH.ID_CPF, how='left').drop(ID_CPF_HASH.ID_CPF)

df__ = df_.join(ID_CNPJ_CEI_HASH, df_.ID_CNPJ_CEI == ID_CNPJ_CEI_HASH.ID_CNPJ_CEI, how='left').drop(ID_CNPJ_CEI_HASH.ID_CNPJ_CEI)

# COMMAND ----------

df__.createOrReplaceTempView('df__')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ano, ID_CPF, count(ID_CPF) as QTDE from df__
# MAGIC where ano = 2012
# MAGIC group by ano, ID_CPF
# MAGIC having count(ID_CPF) > 1
# MAGIC order by ano asc, QTDE desc 

# COMMAND ----------


df.createOrReplaceTempView('observatorio_fiesc')

%sql

select ano, ID_CPF, count(ID_CPF) as QTDE from observatorio_fiesc
where ano = 2012
group by ano, ID_CPF
having count(ID_CPF) > 1
order by ano asc, QTDE desc 

select * from observatorio_fiesc where ID_CPF = 00000414085;


# COMMAND ----------

#observatorio_fiesc.createOrReplaceTempView('observatorio_fiesc')

#%sql

#select DT_DIA_MES_ANO_DATA_ADMISSAO from observatorio_fiesc
#WHERE DT_DIA_MES_ANO_DATA_ADMISSAO LIKE '%//%'

# COMMAND ----------

#%sql

#select DT_DIA_MES_ANO_DATA_ADMISSAO from observatorio_fiesc
#WHERE DT_DIA_MES_ANO_DATA_ADMISSAO LIKE '%//%'

# COMMAND ----------

@udf
def return_string(VL_IDADE):
    if VL_IDADE >= 14 and VL_IDADE <= 17:
        return 'FAIXA 1'
      
    elif VL_IDADE >= 18 and VL_IDADE <= 24:
        return 'FAIXA 2'
      
    elif VL_IDADE >= 25 and VL_IDADE <= 29:
        return 'FAIXA 3'
      
    elif VL_IDADE >= 30 and VL_IDADE <= 35:
        return 'FAIXA 4'
      
    elif VL_IDADE >= 36 and VL_IDADE <= 40:
        return 'FAIXA 5'
      
    elif VL_IDADE >= 41 and VL_IDADE <= 50:
        return 'FAIXA 6'
      
    elif VL_IDADE >= 51 and VL_IDADE <= 65:
        return 'FAIXA 7'
      
    elif VL_IDADE > 65:
        return 'FAIXA 8'
    else:
        return 'NDA'
## +---+---+---+------+

@udf
def return_VL_REMUN_MEDIA_SM(VL_REMUN_MEDIA_SM):
    if VL_REMUN_MEDIA_SM < 1:
        return 'Menor que 1 SM'
      
    elif VL_REMUN_MEDIA_SM >= 1 and VL_REMUN_MEDIA_SM <= 2.59:
        return 'Entre 1 SM e 2.5 SM'
      
    elif VL_REMUN_MEDIA_SM >= 2.6 and VL_REMUN_MEDIA_SM <= 5:
        return 'Entre 2.6 SM e 5 SM'
      
    elif VL_REMUN_MEDIA_SM >= 5.1 and VL_REMUN_MEDIA_SM <= 10:
        return 'Entre 5.1 SM e 10 SM'
      
    elif VL_REMUN_MEDIA_SM > 10:
        return 'Acima de 10 SM'
      
    else:
        return 'NDA'
## +---+---+---+------+

@udf
def return_VL_REMUN_DEZEMBRO_SM(VL_REMUN_DEZEMBRO_SM):
    if VL_REMUN_DEZEMBRO_SM < 1:
        return 'Menor que 1 SM'
      
    elif VL_REMUN_DEZEMBRO_SM >= 1 and VL_REMUN_DEZEMBRO_SM <= 2.59:
        return 'Entre 1 SM e 2.5 SM'
      
    elif VL_REMUN_DEZEMBRO_SM >= 2.6 and VL_REMUN_DEZEMBRO_SM <= 5:
        return 'Entre 2.6 SM até 5 SM'
      
    elif VL_REMUN_DEZEMBRO_SM >= 5.1 and VL_REMUN_DEZEMBRO_SM <= 10:
        return 'Entre 5.1 SM e 10 SM'
      
    elif VL_REMUN_DEZEMBRO_SM > 10:
        return 'Acima de 10 SM'
      
    else:
        return 'NDA'
## +---+---+---+------+

# COMMAND ----------

@udf
def func(ano, x,y,a,b,c,d,e,f,g,vl_remun):
    if ano == x and vl_remun < 510.00:
        return 'Menor que R$510.00'
    if ano == x and 510.00 <= vl_remun <= 1275:
        return 'Entre R$510.00 e R$1275.00'
    if ano == x and 1275.01 <= vl_remun <= 2550:
        return 'Entre R$1275.01 e R$2550.00'
    if ano == x and 2550.01 <= vl_remun <= 5100.00:
        return 'Entre R$2550.01 e R$5100.00'
    if ano == x and vl_remun > 5100.01:
        return 'Acima de R$5100.01'
      
    if ano == y and vl_remun < 545.00:
        return 'Menor que R$545.00'
    if ano == y and 545.00 <= vl_remun <= 1362.50:
        return 'Entre R$545.00 e R$1362.50'
    if ano == y and 1362.51 <= vl_remun <= 2725.00:
        return 'Entre R$1362.51 e R$2725.00'
    if ano == y and 2725.01 <= vl_remun <= 5450:
        return 'Entre R$2725.01 e R$5450.00'
    if ano == y and vl_remun > 5450.01:
        return 'Acima de R$5450.01'
      
    if ano == a and vl_remun < 622.00:
        return 'Menor que R$622.00'
    if ano == a and 622.00 <= vl_remun <= 1555.00:
        return 'Entre R$545.00 e R$1362.50'
    if ano == a and 1555.01 <= vl_remun <= 3110.00:
        return 'Entre R$1362.51 e R$2725.00'
    if ano == a and 3110.01 <= vl_remun <= 6220.00:
        return 'Entre R$3110.00.01 e R$6220.00'
    if ano == a and vl_remun > 6220.01:
        return 'Acima de R$6220.01'
      
    if ano == b and vl_remun < 678.00:
        return 'Menor que R$678.00'
    if ano == b and 678.00 <= vl_remun <= 1695.00:
        return 'Entre R$545.00 e R$1695.00'
    if ano == b and 1695.01 <= vl_remun <= 3390.00:
        return 'Entre R$1695.01 e R$3390.00'
    if ano == b and 3390.01 <= vl_remun <= 6780.00:
        return 'Entre R$3390.01 e R$6780.00'
    if ano == b and vl_remun > 6780.01:
        return 'Acima de R$6780.01'
      
    if ano == c and vl_remun < 724.00:
        return 'Menor que R$724.00'
    if ano == c and 724.01 <= vl_remun <= 1810.00:
        return 'Entre R$724.01 e R$1810.00'
    if ano == c and 1810.01 <= vl_remun <= 3620.00:
        return 'Entre R$1810.01 e R$3620.00'
    if ano == c and 3620.01 <= vl_remun <= 7240.00:
        return 'Entre R$3620.01 e R$7240.00'
    if ano == c and vl_remun > 7240.01:
        return 'Acima de R$7240.01'
      
    if ano == d and vl_remun < 788.00:
        return 'Menor que R$788.00'
    if ano == d and 788.01 <= vl_remun <= 1970.00:
        return 'Entre R$788.01 e R$1970.00'
    if ano == d and 1970.01 <= vl_remun <= 3940.00:
        return 'Entre R$1970.01 e R$3940.00'
    if ano == d and 3940.01 <= vl_remun <= 7880.00:
        return 'Entre R$3940.01 e R$7880.00'
    if ano == d and vl_remun > 7880.01:
        return 'Acima de R$7880.01'
      
    if ano == e and vl_remun < 880.00:
        return 'Menor que R$880.00'
    if ano == e and 880.01 <= vl_remun <= 2200.00:
        return 'Entre R$880.01 e R$2200.00'
    if ano == e and 2200.01 <= vl_remun <= 4400.00:
        return 'Entre R$2200.01 e R$4400.00'
    if ano == e and 4400.01 <= vl_remun <= 8800.00:
        return 'Entre R$4400.01 e R$8800.00'
    if ano == e and vl_remun > 8800.01:
        return 'Acima de R$8800.01'
      
    if ano == f and vl_remun < 937.00:
        return 'Menor que R$937.00'
    if ano == f and 937.01 <= vl_remun <= 2342.50:
        return 'Entre R$937.01 e R$2342.50'
    if ano == f and 2342.51 <= vl_remun <= 4685.00:
        return 'Entre R$2342.51 e R$4685.00'
    if ano == f and 4685.01 <= vl_remun <= 9370.00:
        return 'Entre R$4685.01 e R$9370.00'
    if ano == f and vl_remun > 9370.01:
        return 'Acima de R$9370.01'
      
    if ano == g and vl_remun < 954.00:
        return 'Menor que R$954.00'
    if ano == g and 954.00 <= vl_remun <= 2385.00:
        return 'Entre R$954.00 e R$2385.00'
    if ano == g and 2385.01 <= vl_remun <= 4770.00:
        return 'Entre R$2385.01 e R$4770.00'
    if ano == g and 4770.01 <= vl_remun <= 9540.00:
        return 'Entre R$4770.01 e R$9540.00'
    if ano == g and vl_remun > 9540.01:
        return 'Acima de R$9540.01'
  
    return 'NDA'
  
# Example 
# df = observatorio_fiesc.withColumn('new_column',func(observatorio_fiesc['ANO'], lit(2010), lit(2011), lit(2012), lit(2013), lit(2014), lit(2015), lit(2016), lit(2017), lit(2018), observatorio_fiesc['VL_REMUN_MEDIA_NOM']))

# COMMAND ----------

df = (
  (
  observatorio_fiesc.filter(col("CD_UF").isin([41, 42, 43]))\
    .filter(col("ANO").isin([2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]))\
  #.withColumn('ID_CPF', sha2(observatorio_fiesc['ID_CPF'],256))\
  #.withColumn('ID_CNPJ_CEI', sha2(observatorio_fiesc['ID_CNPJ_CEI'],256))\
  .withColumn('FAIXA_VL_IDADE', return_string(col('VL_IDADE')))\
  .withColumn('FAIXA_VL_REMUN_MEDIA_SM', return_VL_REMUN_MEDIA_SM(col('VL_REMUN_MEDIA_SM')))\
  .withColumn('FAIXA_VL_REMUN_DEZEMBRO_SM', return_VL_REMUN_DEZEMBRO_SM(col('VL_REMUN_DEZEMBRO_SM')))\
  .withColumn('VL_REMUN_MEDIA_NOM_CATEGORIZAR',func(observatorio_fiesc['ANO'], lit(2010), lit(2011), lit(2012), lit(2013), lit(2014), lit(2015), lit(2016), lit(2017), lit(2018), observatorio_fiesc['VL_REMUN_MEDIA_NOM']))\
  .withColumn('VL_REMUN_DEZEMBRO_NOM_CATEGORIZAR',func(observatorio_fiesc['ANO'], lit(2010), lit(2011), lit(2012), lit(2013), lit(2014), lit(2015), lit(2016), lit(2017), lit(2018), observatorio_fiesc['VL_REMUN_DEZEMBRO_NOM']))
)).select('ANO','CD_UF','ID_CPF','ID_CNPJ_CEI',
          'CD_GRAU_INSTRUCAO','CD_CBO',
          'CD_CNAE20_SUBCLASSE','FAIXA_VL_IDADE','VL_IDADE','FAIXA_VL_REMUN_MEDIA_SM','VL_REMUN_MEDIA_SM',
          'FAIXA_VL_REMUN_DEZEMBRO_SM','VL_REMUN_DEZEMBRO_SM','CD_MUNICIPIO','CD_MUNICIPIO_TRAB',
          'CD_SEXO','CD_TAMANHO_ESTABELECIMENTO','NR_MES_TEMPO_EMPREGO', 'FL_VINCULO_ATIVO_3112', 'CD_MES_DESLIGAMENTO', 'CD_MOTIVO_DESLIGAMENTO', 'CD_TIPO_VINCULO', 'CD_TIPO_SALARIO','CD_TIPO_ADMISSAO','CD_NATUREZA_JURIDICA', 'VL_REMUN_MEDIA_NOM', 'VL_REMUN_MEDIA_NOM_CATEGORIZAR','VL_REMUN_DEZEMBRO_NOM', 'VL_REMUN_DEZEMBRO_NOM_CATEGORIZAR', 'DT_DIA_MES_ANO_DATA_ADMISSAO')

# COMMAND ----------

df_ = df.join(ID_CPF_HASH, df.ID_CPF == ID_CPF_HASH.ID_CPF, how='left').drop(ID_CPF_HASH.ID_CPF)

# COMMAND ----------

df__ = df_.join(ID_CNPJ_CEI_HASH, df_.ID_CNPJ_CEI == ID_CNPJ_CEI_HASH.ID_CNPJ_CEI, how='left').drop(ID_CNPJ_CEI_HASH.ID_CNPJ_CEI)

# COMMAND ----------

df___ = df__.select('ANO','CD_UF',
 'CD_GRAU_INSTRUCAO',
 'CD_CBO',
 'CD_CNAE20_SUBCLASSE',
 'FAIXA_VL_IDADE',
 'FAIXA_VL_REMUN_MEDIA_SM',
 'FAIXA_VL_REMUN_DEZEMBRO_SM',
 'CD_MUNICIPIO',
 'CD_MUNICIPIO_TRAB',
 'CD_SEXO',
 'CD_TAMANHO_ESTABELECIMENTO',
 'NR_MES_TEMPO_EMPREGO',
 'FL_VINCULO_ATIVO_3112',
 'CD_MES_DESLIGAMENTO',
 'CD_MOTIVO_DESLIGAMENTO',
 'CD_TIPO_VINCULO',
 'CD_TIPO_SALARIO',
 'CD_TIPO_ADMISSAO',
 'CD_NATUREZA_JURIDICA',
 'VL_REMUN_MEDIA_NOM_CATEGORIZAR',
 'VL_REMUN_DEZEMBRO_NOM_CATEGORIZAR',
 'ID_CPF_HASH',
 'ID_CNPJ_CEI_HASH',
 'DT_DIA_MES_ANO_DATA_ADMISSAO')

# COMMAND ----------

df___.display()
# ANO

# COMMAND ----------

df___.count()

# COMMAND ----------

(df___.select('ID_CPF_HASH')).count()

# COMMAND ----------

(df___.select('ID_CPF_HASH')).distinct().count()

# COMMAND ----------



# COMMAND ----------

  ''

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


# COMMAND ----------

#df___.coalesce(2).write.format('csv').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/danilo_22_05_2023/', header=True, mode='overwrite', encoding='ISO-8859-1')
df___.write.format('parquet').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/rede/observatorio_fiesc/rais_regiao_sul/', header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


Q = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/oni/cnaes_contribuintes/')


Q.display()
Q.printSchema()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest
from pyspark.sql import functions


path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/observatorio_fiesc'
DF_ESTABELECIMENTO = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)

# COMMAND ----------

DF_ESTABELECIMENTO = df__

# COMMAND ----------

DF_ESTABELECIMENTO.select("*")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions

# COMMAND ----------

for each_columns in DF_ESTABELECIMENTO.columns:
  DF_ESTABELECIMENTO = DF_ESTABELECIMENTO.withColumn(f'{each_columns}', functions.regexp_replace(f'{each_columns}',r'[.]',","))

# COMMAND ----------

DF_ESTABELECIMENTO.display()

# COMMAND ----------

#from pyspark.sql import functions

#df = df.withColumn("longitude", functions.regexp_replace('longitude',r'[.]',","))
#df = df.withColumn("latitude", functions.regexp_replace('latitude',r'[.]',","))
#df.show()

# COMMAND ----------

#ANOS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
#
#for each_year in ANOS:
#  (
#    DF_ESTABELECIMENTO.filter(col('ANO') == f'{each_year}')
#  ).display()

# COMMAND ----------

DF_ESTABELECIMENTO.columns

# COMMAND ----------

DF_ESTAB = (
  DF_ESTABELECIMENTO.filter( (col('ANO') >= 2010) & (col('ANO')  <= 2018) )
).select('ANO',
 'CD_UF',
 'CD_CNAE20_SUBCLASSE',
 'FAIXA_VL_IDADE',
 'FAIXA_VL_REMUN_MEDIA_SM',
 'FAIXA_VL_REMUN_DEZEMBRO_SM',
 'CD_MUNICIPIO',
 'CD_MUNICIPIO_TRAB',
 'CD_SEXO',
 'CD_TAMANHO_ESTABELECIMENTO',
 'NR_MES_TEMPO_EMPREGO',
 'FL_VINCULO_ATIVO_3112',
 'CD_MES_DESLIGAMENTO',
 'CD_MOTIVO_DESLIGAMENTO',
 'CD_TIPO_VINCULO',
 'CD_TIPO_SALARIO',
 'CD_TIPO_ADMISSAO',
 'CD_NATUREZA_JURIDICA',
 'VL_REMUN_MEDIA_NOM_CATEGORIZAR',
 'VL_REMUN_DEZEMBRO_NOM_CATEGORIZAR',
 'ID_CPF_HASH',
 'ID_CNPJ_CEI_HASH',
 'DT_DIA_MES_ANO_DATA_ADMISSAO')

# COMMAND ----------

DF_ESTAB.display()

# COMMAND ----------

DF_ESTAB.write.format('parquet').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_fiesc_campos_e_critérios_para_seleção_da_RAIS/', header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

DF_ESTAB.coalesce(1).write.format('csv').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/danilo/', header=True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/biz/uniepro/fta_rfb_cno/cno_biz/'.format(uri=var_adls_uri)
Q = spark.read.format("csv").option("header","true").option("encoding", "ISO-8859-1").option('sep',',').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/danilo/')
Q.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rais_Establecimento 
# MAGIC ##### Demanda FIEC

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/rais_estabelecimento/'.format(uri=var_adls_uri)
rais_vincu = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)

# COMMAND ----------

rais_vincu.display()

# COMMAND ----------

rais_vincu.columns

# COMMAND ----------

 Rais_Estabelecimento = (
  (rais_vincu.withColumn('COD_UF', f.substring(f.col('CD_MUNICIPIO'),1,2))).select('ANO','CD_TAMANHO_ESTABELECIMENTO','COD_UF','ID_CEPAO_ESTAB','CD_MUNICIPIO','NM_LOGRADOURO','NR_LOGRADOURO','NM_BAIRRO','CD_IBGE_SUBSETOR','CD_CNAE20_CLASSE', 'CD_CNAE20_SUBCLASSE','ID_CNPJ_CEI','ID_RAZAO_SOCIAL','FL_IND_SIMPLES','FL_IND_ATIV_ANO','CD_TIPO_ESTAB_ID', 'CD_NATUREZA_JURIDICA')
).filter(col('ANO') == 2018)\
.filter( (col('COD_UF') >= 17) & (col('COD_UF')  <= 29) )

# COMMAND ----------

 Rais_Estabelecimento.display()

# COMMAND ----------

# Rais_Estabelecimento.select(countDistinct("COD_UF")).show()
(
  Rais_Estabelecimento.select('COD_UF')
).distinct().show()

# COMMAND ----------

# MAGIC %md 
# MAGIC # filtering between values

# COMMAND ----------

# t.filter( (col('COD_UF') >= 17) & (col('COD_UF')  <= 29) ).display()
# .where(t.COD_UF.between(17, 29))

# COMMAND ----------

 Rais_Estabelecimento.write.format('parquet').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_fiec/', header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------



# COMMAND ----------

  from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


FIEC = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_fiec/')
FIEC

# COMMAND ----------

FIEC.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest
 
 
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/rais_vinculo/'.format(uri=var_adls_uri)
observatorio_fiesc = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)


FIESC = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_fiesc/')
FIESC

# COMMAND ----------

FIESC.display()

# COMMAND ----------

FIESC.write.format('csv').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/danilo/', header=True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------


