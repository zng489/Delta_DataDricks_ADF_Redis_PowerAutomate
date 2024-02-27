# Databricks notebook source
# Importing functions
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

# COMMAND ----------

# Importando arquivo rais_vinculo.parquet
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo2008a2018 = spark.read.format("parquet").option("header","true").load(trs_rais_vinculo2008a2018_path)

# COMMAND ----------

df_rais_vinculo2008a2018.createOrReplaceTempView("table_rais_2018")

# COMMAND ----------

sql(" select DISTINCT ANO from table_rais_2018 ").show()

# COMMAND ----------

df.createOrReplaceTempView("people")
df2 = df.filter(df.age > 3)
df2.createOrReplaceTempView("people")
df3 = spark.sql("select * from people")
sorted(df3.collect()) == sorted(df2.collect())
True
spark.catalog.dropTempView("people")

# COMMAND ----------

# sql("select * from table_rais_2018 where ANO in (select DISTINCT ANO from table_rais_2018)")
df = spark.sql(" select * from table_rais_2018 where ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018) ")

# COMMAND ----------

sql(" select * from (select * from table_rais_2018 where ANO in (2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018)) where FL_VINCULO_ATIVO_3112 == 1")

# COMMAND ----------



# COMMAND ----------

df.createOrReplaceTempView("df_0")

df_1 = spark.sql(" select * from df_0 where FL_VINCULO_ATIVO_3112 == 1")
df_1.createOrReplaceTempView("df_2")

# COMMAND ----------

SELECT column_name(s)
FROM table_name
WHERE condition
GROUP BY column_name(s)
ORDER BY column_name(s);

# COMMAND ----------

SELECT COUNT(DISTINCT program_name) AS Count, program_type AS [Type] 
FROM cm_production 
WHERE push_number=@push_number 
GROUP BY program_type

# COMMAND ----------

sql(" select * from df_2 group by ID_CPF; ")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

