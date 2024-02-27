# Databricks notebook source
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/ibge/piaemp_aquisicoes_f/'.format(uri=var_adls_uri)
df_= spark.read.format("parquet").option("header","true").load(path)
df_.display()

# COMMAND ----------

df_ = df_.filter(df_.nm_variavel == "máquinas")

# COMMAND ----------

df_.filter(df_.NM_VARIAVEL > 3)

# COMMAND ----------

filter(df_.NM_VARIAVEL("máquinas")) 

# COMMAND ----------

df_.filter(df_.NM_VARIAVEL.contains("equipamentos")).display()

# COMMAND ----------

|#df_.groupBy("department").count()

# COMMAND ----------

df_.dropDuplicates(["NM_VARIAVEL"]).display()

# COMMAND ----------

items = dbutils.fs.ls('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw/oni/ibge/pintec/bio_nano/bio_nano_po/')

# COMMAND ----------

items


# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/biz/oni/prospectiva_mundo_do_trabalho/msp_cnae//'.format(uri=var_adls_uri)
df_= spark.read.format("parquet").option("header","true").load(path)
df_.display()

# COMMAND ----------

df_.coalesce(1).write.format('csv').save(var_adls_uri + '/uds/uniepro/msp_unificada/', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/rfb_cnpj/cadastro_empresa_f'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").option('mergeschema', 'false').load(path)
df

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/rfb_cnpj/cadastro_simples_f'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").option('mergeschema', 'false').load(path)
df

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/lnd/crw/rfb_cno__cadastro_nacional_de_obras/cno/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").option('mergeschema', 'false').load(path)

df.display()


# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/raw/crw/rfb_cnpj/cadastro_estbl/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.count()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

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
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------

df.display()

# COMMAND ----------

