# Databricks notebook source
data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

# COMMAND ----------

df.display()

# COMMAND ----------

df_rdd = df.rdd

# COMMAND ----------

df_rdd.map(f=>{f.split(',')})

# COMMAND ----------

zhang = df.rdd.map(lambda x: (x[0]+","+x[1],x[2],x[3]*2)) 
zhang

# COMMAND ----------

zhang.toDF

# COMMAND ----------

rdd2 = df.map(lambda x: (x[0]+","+x[1],x[2],x[3]*2))  

# COMMAND ----------

zhang.display()

# COMMAND ----------

x = lambda a : a + 10
print(x(5))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo2008a2018 = spark.read.parquet(trs_rais_vinculo2008a2018_path)
display(df_rais_vinculo2008a2018.limit(2))

# COMMAND ----------

from pyspark.sql.functions import *

df.filter( col('FL_VINCULO_ATIVO_3112') == True)

# COMMAND ----------

from pyspark.sql.functions import *

df.filter(lambda x: x.happy == True)

# COMMAND ----------

def rename_columns(rename_df):
  for column in rename_df.columns:
    new_column = "Col_ " + column
    rename_df = rename_df.withColumnRenamed(column, new_column)
  return rename_df

df = rename_columns(df_rais_vinculo2008a2018)
df.display()

# COMMAND ----------

def rename_columns(rename_df):
  for column in rename_df.columns:
    new_column = "Col_ " + column
  return new_column

for x in df_rais_vinculo2008a2018.columns:
  rename_df = df_rais_vinculo2008a2018.withColumnRenamed(x, rename_columns(df_rais_vinculo2008a2018))
rename_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType, StringType

df_ind_cesc_eco = df_ind_cesc_eco.withColumnRenamed('tx_pib1...60', 'tx_pib160').withColumnRenamed('tx_pib1...61', 'tx_pib161')
df_ind_cesc_eco

def cast(dataframe_cast):
  for column in dataframe_cast.columns:
    dataframe_cast = dataframe_cast.withColumn(
  column, f.when(col(column).contains('.'), col(column).cast(DoubleType())).otherwise(col(column).cast(StringType())
))
  return dataframe_cast

df = cast(df_ind_cesc_eco)
df

            

# COMMAND ----------

df_rais_vinculo2008a2018

# COMMAND ----------

'''
DT_DIA_MES_ANO_DATA_ADMISSAO | DT_DIA_MES_ANO_DIA_DESLIGAMENTO
           01092005                         {ñ
'''

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo2008a2018 = spark.read.parquet(trs_rais_vinculo2008a2018_path)
display(df_rais_vinculo2008a2018.limit(5))

# COMMAND ----------

trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo2008a2018 = (spark.read.parquet(trs_rais_vinculo2008a2018_path)
                            .where((f.col('ANO')<f.lit(2019)))
                            .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))
                            .withColumn('DATA_INICIO_AFAST_1', f.when((f.col('NR_DIA_INI_AF1').isNotNull()) & 
                                         (f.col('NR_MES_INI_AF1').isNotNull()) &
                                         (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_INI_AF1", 2, '0'),
                                                                                  f.lpad("NR_DIA_INI_AF1", 2, '0')).cast('Date')))
                           .withColumn('DATA_FIM_AFAST_1', f.when((f.col('NR_DIA_FIM_AF1').isNotNull()) & 
                                         (f.col('NR_MES_FIM_AF1').isNotNull()) &
                                         (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_FIM_AF1", 2, '0'),
                                                                                  f.lpad("NR_DIA_FIM_AF1", 2, '0')).cast('Date')))
                           .withColumn('DATA_INICIO_AFAST_2', f.when((f.col('NR_DIA_INI_AF2').isNotNull()) & 
                                         (f.col('NR_MES_INI_AF2').isNotNull()) &
                                         (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_INI_AF2", 2, '0'),
                                                                                  f.lpad("NR_DIA_INI_AF2", 2, '0')).cast('Date')))
                           .withColumn('DATA_FIM_AFAST_2', f.when((f.col('NR_DIA_FIM_AF2').isNotNull()) & 
                                         (f.col('NR_MES_FIM_AF2').isNotNull()) &
                                         (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_FIM_AF2", 2, '0'),
                                                                                  f.lpad("NR_DIA_FIM_AF2", 2, '0')).cast('Date')))
                           .withColumn('DATA_INICIO_AFAST_3', f.when((f.col('NR_DIA_INI_AF3').isNotNull()) & 
                                         (f.col('NR_MES_INI_AF3').isNotNull()) &
                                         (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_INI_AF3", 2, '0'),
                                                                                  f.lpad("NR_DIA_INI_AF3", 2, '0')).cast('Date')))
                           .withColumn('DATA_FIM_AFAST_3', f.when((f.col('NR_DIA_FIM_AF3').isNotNull()) & 
                                         (f.col('NR_MES_FIM_AF3').isNotNull()) &
                                         (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_FIM_AF3", 2, '0'),
                                                                                  f.lpad("NR_DIA_FIM_AF3", 2, '0')).cast('Date'))))

# COMMAND ----------

def VERIF_VALORES_ANOS_INICIO_FIM(ANO):

  VERIF = df_rais_vinculo2008a2018.repartition(10).filter(f.col('ANO') == ANO ).select('ANO','DATA_INICIO_AFAST_1','DATA_FIM_AFAST_1','DATA_INICIO_AFAST_2','DATA_FIM_AFAST_2','DATA_INICIO_AFAST_3','DATA_FIM_AFAST_3','NR_DIA_INI_AF1','NR_MES_INI_AF1','NR_DIA_FIM_AF1','NR_MES_FIM_AF1','NR_DIA_INI_AF2','NR_MES_INI_AF2','NR_DIA_FIM_AF2','NR_MES_FIM_AF2','NR_DIA_INI_AF3','NR_MES_INI_AF3','NR_DIA_FIM_AF3','NR_MES_FIM_AF3')
  
  print('Quantidade de rows = ',VERIF.count())
  
  print('''  1  ##########    ''')
  print(''' --------------------------------------- ''')
  print(''' Tabela NR_DIA_INI_AF1 ''')
  VERIF.repartition(10).select(f.col('NR_DIA_INI_AF1')).dropDuplicates(['NR_DIA_INI_AF1']).show()  
  
  print(''' Tabela NR_MES_INI_AF1 ''')
  VERIF.repartition(10).select(f.col('NR_MES_INI_AF1')).dropDuplicates(['NR_MES_INI_AF1']).show()
  
  print(''' Contagem de valores Null e NotNull INICIO_1''')
  print('DATA_INICIO_AFAST_1 contagem de valores NAO Null = ',VERIF.repartition(8).filter(f.col('DATA_INICIO_AFAST_1').isNotNull()).count())
  print('DATA_INICIO_AFAST_1 contagem de valores Null = ',VERIF.repartition(8).filter(f.col('DATA_INICIO_AFAST_1').isNull()).count())

    
  print(''' --------------------------------------- ''')
  print(''' Tabela NR_DIA_FIM_AF1 ''')
  VERIF.repartition(10).select(f.col('NR_DIA_FIM_AF1')).dropDuplicates(['NR_DIA_FIM_AF1']).show()  
  
  print(''' Tabela NR_MES_INI_AF1 ''')
  VERIF.repartition(10).select(f.col('NR_MES_FIM_AF1')).dropDuplicates(['NR_MES_FIM_AF1']).show()
  
  print(''' Contagem de valores Null e NotNull FIM_1 ''')
  print('DATA_INICIO_AFAST_1 contagem de valores NAO Null = ',VERIF.repartition(8).filter(f.col('DATA_FIM_AFAST_1').isNotNull()).count())
  print('DATA_INICIO_AFAST_1 contagem de valores Null = ',VERIF.repartition(8).filter(f.col('DATA_FIM_AFAST_1').isNull()).count())

        
  print('''  2  ##########    ''')

  print(''' --------------------------------------- ''')
  print(''' Tabela NR_DIA_INI_AF2 ''')
  VERIF.repartition(10).select(f.col('NR_DIA_INI_AF2')).dropDuplicates(['NR_DIA_INI_AF2']).show()  
  
  print(''' Tabela NR_MES_INI_AF2 ''')
  VERIF.repartition(10).select(f.col('NR_MES_INI_AF2')).dropDuplicates(['NR_MES_INI_AF2']).show()
  
  print(''' Contagem de valores Null e NotNull INICIO_2''')
  print('DATA_INICIO_AFAST_2 contagem de valores NAO Null = ',VERIF.repartition(8).filter(f.col('DATA_INICIO_AFAST_2').isNotNull()).count())
  print('DATA_INICIO_AFAST_2 contagem de valores Null = ',VERIF.repartition(8).filter(f.col('DATA_INICIO_AFAST_2').isNull()).count())

    
  print(''' --------------------------------------- ''')
  print(''' Tabela NR_DIA_FIM_AF2 ''')
  VERIF.repartition(10).select(f.col('NR_DIA_FIM_AF2')).dropDuplicates(['NR_DIA_FIM_AF2']).show()  
  
  print(''' Tabela NR_MES_INI_AF2 ''')
  VERIF.repartition(10).select(f.col('NR_MES_FIM_AF2')).dropDuplicates(['NR_MES_FIM_AF2']).show()
  
  print(''' Contagem de valores Null e NotNull FIM_2 ''')
  print('DATA_INICIO_AFAST_2 contagem de valores NAO Null = ',VERIF.repartition(8).filter(f.col('DATA_FIM_AFAST_2').isNotNull()).count())
  print('DATA_INICIO_AFAST_2 contagem de valores Null = ',VERIF.repartition(8).filter(f.col('DATA_FIM_AFAST_2').isNull()).count())       
        
  print('''  3  ##########    ''')

  print(''' --------------------------------------- ''')
  print(''' Tabela NR_DIA_INI_AF3 ''')
  VERIF.repartition(10).select(f.col('NR_DIA_INI_AF3')).dropDuplicates(['NR_DIA_INI_AF3']).show()  
  
  print(''' Tabela NR_MES_INI_AF3 ''')
  VERIF.repartition(10).select(f.col('NR_MES_INI_AF3')).dropDuplicates(['NR_MES_INI_AF3']).show()
  
  print(''' Contagem de valores Null e NotNull INICIO_3''')
  print('DATA_INICIO_AFAST_3 contagem de valores NAO Null = ',VERIF.repartition(8).filter(f.col('DATA_INICIO_AFAST_3').isNotNull()).count())
  print('DATA_INICIO_AFAST_3 contagem de valores Null = ',VERIF.repartition(8).filter(f.col('DATA_INICIO_AFAST_3').isNull()).count())

    
  print(''' --------------------------------------- ''')
  print(''' Tabela NR_DIA_FIM_AF3 ''')
  VERIF.repartition(10).select(f.col('NR_DIA_FIM_AF3')).dropDuplicates(['NR_DIA_FIM_AF3']).show()  
  
  print(''' Tabela NR_MES_INI_AF3 ''')
  VERIF.repartition(10).select(f.col('NR_MES_FIM_AF3')).dropDuplicates(['NR_MES_FIM_AF3']).show()
  
  print(''' Contagem de valores Null e NotNull FIM_3 ''')
  print('DATA_INICIO_AFAST_3 contagem de valores NAO Null = ',VERIF.repartition(8).filter(f.col('DATA_FIM_AFAST_3').isNotNull()).count())
  print('DATA_INICIO_AFAST_3 contagem de valores Null = ',VERIF.repartition(8).filter(f.col('DATA_FIM_AFAST_3').isNull()).count())        
        

def display_function(ANO):
  VERIF = df_rais_vinculo2008a2018.filter(f.col('ANO') == ANO ).select('ANO','DATA_INICIO_AFAST_1','DATA_FIM_AFAST_1','DATA_INICIO_AFAST_2','DATA_FIM_AFAST_2','DATA_INICIO_AFAST_3','DATA_FIM_AFAST_3','NR_DIA_INI_AF1','NR_MES_INI_AF1','NR_DIA_FIM_AF1','NR_MES_FIM_AF1','NR_DIA_INI_AF2','NR_MES_INI_AF2','NR_DIA_FIM_AF2','NR_MES_FIM_AF2','NR_DIA_INI_AF3','NR_MES_INI_AF3','NR_DIA_FIM_AF3','NR_MES_FIM_AF3').cache()
  
  return VERIF.display()

#if __name__ == '__main__':
#  ANO = 
#  
#  VERIF_VALORES_ANOS_INICIO_FIM(ANO)
#  display_function(ANO)

# COMMAND ----------

VERIF_VALORES_ANOS_INICIO_FIM(2008)

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC df_rais_vinculo2008a2018.filter( col('ANO') == 2011).select('DT_DIA_MES_ANO_DATA_ADMISSAO','DT_DIA_MES_ANO_DIA_DESLIGAMENTO','CD_MES_DESLIGAMENTO','datafinal','NR_MES_TEMPO_EMPREGO').display()

# COMMAND ----------

# MAGIC %md
# MAGIC df_rais_vinculo2008a2018 = (df_rais_vinculo2008a2018.withColumn('ID_CPF', lpad(col('ID_CPF'), 11, '0'))\
# MAGIC                             .withColumn('ID_CNPJS_CEI', lpad(col('ID_CNPJ_CEI'), 14, '0'))\
# MAGIC                             .withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',lpad(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
# MAGIC                             .withColumn('datafinal', concat_ws('-', col('ANO'), col('CD_MES_DESLIGAMENTO'), col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
# MAGIC                             .withColumn('data_inicio',concat_ws('-', col('ANO'), lit('01'), lit('01')))\
# MAGIC                             .withColumn('data_fim', concat_ws('-', col('ANO'), lit('12'), lit('31'))))

# COMMAND ----------

from pyspark.sql.functions import *

df_rais_vinculo2008a2018 = (df_rais_vinculo2008a2018.withColumn('ID_CPF', lpad(col('ID_CPF'), 11, '0'))\
                            .withColumn('ID_CNPJS_CEI', lpad(col('ID_CNPJ_CEI'), 14, '0'))\
                            .withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',lpad(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
                            .withColumn('datafinal', concat_ws('-', col('ANO'), col('CD_MES_DESLIGAMENTO'), col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
                            .withColumn('data_inicio',concat_ws('-', col('ANO'), lit('01'), lit('01')))\
                            .withColumn('data_fim', concat_ws('-', col('ANO'), lit('12'), lit('31')))\
                            .withColumn('dt_adm_corrigido', when(col('ANO')== lit(2011), concat_ws('-', substring(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,4),
                                                                                       substring( col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,2),
                                                                                       substring( col('DT_DIA_MES_ANO_DATA_ADMISSAO'),7,2)))
                                        .otherwise( when((( 
                                          col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains("/")) | 
                                          (col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains("*"))), 
                                          expr("add_months(datafinal,-NR_MES_TEMPO_EMPREGO)"))    
                                        .otherwise(f.concat_ws('-', substring( col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                                               substring(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2),
                                                               substring(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2)))))
                           )
df_rais_vinculo2008a2018.display()

# COMMAND ----------

df_rais_vinculo2008a2018.select('ID_CPF','ID_CNPJ_CEI','DT_DIA_MES_ANO_DATA_ADMISSAO','datafinal','DT_DIA_MES_ANO_DIA_DESLIGAMENTO','data_inicio','data_fim','dt_adm_corrigido').display()

# COMMAND ----------

df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
df.show()

# COMMAND ----------

df.select(concat_ws('-', df.s, df.d).alias('kkkkkkkkk')).collect()

# COMMAND ----------

def soma(x):
  return 1 + x

# COMMAND ----------

e = soma(4)

# COMMAND ----------

e

# COMMAND ----------

#Concatenate columns using || (sql like)
data=[("James","Bond"),("Scott","Varsa")] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.display()

# COMMAND ----------

df.withColumn("Name",expr("col1, col2")).show()

# COMMAND ----------

df.withColumn("Name",expr("add_months(datafinal,-NR_MES_TEMPO_EMPREGO)")).show()

+-----+-----+-----------+
| col1| col2|       Name|
+-----+-----+-----------+
|James| Bond| James,Bond|
|Scott|Varsa|Scott,Varsa|
+-----+-----+-----------+

# COMMAND ----------


from pyspark.sql.functions import expr
data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)] 
df=spark.createDataFrame(data).toDF("date","increment") 

df.display()

# COMMAND ----------


#Add Month value from another column
df.select(df.date,df.increment, expr("add_months(date,increment)").alias("inc_date")).show()


# COMMAND ----------

