# Databricks notebook source
from pyspark.sql.types import StringType, IntegerType, LongType
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as f
import pandas as pd
import re


# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path_catho = '{uri}/raw/crw/catho/vagas/'.format(uri=var_adls_uri)
data_catho = spark.read.parquet(path_catho)
data_catho

# COMMAND ----------

def test_of_udf(x):
  a = x.replace('<li>', '').replace('</li>', '').replace(';', '').replace('\r','').replace('\n','')
  #b = a.replace('</li>', '')
  #c = b.replace('"', '')
  return a
function_test_of_udf = udf(lambda x: test_of_udf(x), StringType())
#if __name__ == "__main__":
#  test_of_udf(3)
df = data_catho.withColumn('PYSPARK_descricao', function_test_of_udf(col('descricao')))

# COMMAND ----------

df = df.select('id','PYSPARK_descricao')

# COMMAND ----------



# COMMAND ----------

df.show()

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'  
PATH_TARGET="/uds/uniepro/export/rais_vinculo/monitor_de_emprego_arrumado.csv"  


target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)     
#df = (spark.read.parquet(target_path)).sample(fraction=0.1)
df.repartition(1).write.mode("overwrite").option('header', True).csv(target_path)       


# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'  
PATH_TARGET="/uds/uniepro/export/rais_vinculo/monitor_de_emprego_arrumado.csv"  


target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)     
#df = (spark.read.parquet(target_path)).sample(fraction=0.1)
df.repartition(1).write.mode("overwrite").option('header', True).csv(target_path)       



data_lake_access = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
ENDERECO = '/uds/uniepro/export/rais_vinculo/monitor_de_emprego_arrumado.csv'

file_path = '{address}{SEU_ARQUIVO}'.format(address=data_lake_access, SEU_ARQUIVO=ENDERECO)
data = spark.read.option('delimiter', ',').csv(file_path, header=True)

# COMMAND ----------

data.display()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd.set_option('display.max_columns',None)

# COMMAND ----------

data.toPandas()

# COMMAND ----------



# COMMAND ----------

data_catho.select('descricao').display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# COMMAND ----------

df = data_catho.select(regexp_replace(col("descricao"), "<li>", ""))
df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import split, regexp_extract

# COMMAND ----------

data_catho.select(regexp_extract('descricao', r'(\d+)-(\d+)', 1).alias('host')).collect()

# COMMAND ----------

df = spark.createDataFrame([('100-200',)], ['str'])
df.display()

# COMMAND ----------

df_1 = df.select(regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d'))

# COMMAND ----------

df_1.display()

# COMMAND ----------



# COMMAND ----------

split_df.display()


# COMMAND ----------

data_catho.select('descricao').display()

# COMMAND ----------

#[A-Za-z \d]
df.select(regexp_replace('descricao', r'(\d+)', '--').alias('d')).collect()

# COMMAND ----------

df = spark.createDataFrame([('100-200',)], ['str'])
df.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect()

# COMMAND ----------

def test_of_udf(x):
  a = x + 'PYSPARK'
  return a
function_test_of_udf = udf(lambda x: test_of_udf(x), StringType())
#if __name__ == "__main__":
#  test_of_udf(3)
df = data_catho.withColumn('PYSPARK_descricao', function_test_of_udf(col('descricao')))

# COMMAND ----------

@udf(returnType=StringType())
def test_of_udf(x):
  a = x + 'PYSPARK'
  return a
#function_test_of_udf = udf(lambda x: test_of_udf(x), StringType())

#if __name__ == "__main__":
#  test_of_udf(3)
df = data_catho.withColumn('PYSPARK_DECORATOR_descricao', function_test_of_udf(col('descricao')))

# COMMAND ----------

class py_or_udf():
  def __init__(self, variable_A, variable_B):
    self.A = variable_A
    self.B = variable_B
    #return
  def __str__s(self):
    return self.A
  
def main():
  variable_A = " Zhang"
  variable_B = "Yuan"
  
  main_class = py_or_udf(variable_A,variable_B)
  print(main_class)

if __name__ == "__main__":
  main()
  display(main())

# COMMAND ----------

class py_or_udf():
  def __init__(self, variable_A, variable_B):
    self.A = variable_A
    self.B = variable_B
    #return
  def __str__s(self):
    return self.A

# COMMAND ----------

import py_or_udf

py_or_udf('zhang','yuan')

# COMMAND ----------

main()

# COMMAND ----------



# COMMAND ----------

df.select('descricao').display()

# COMMAND ----------

df.select('PYSPARK_DECORATOR_descricao').display()

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re

# CATHO
#def data_inification():
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path_catho = '{uri}/raw/crw/catho/vagas/'.format(uri=var_adls_uri)
data_catho = spark.read.parquet(path_catho)
data_catho = data_catho.select(['data','dataRemovido','id','titulo','faixaSalarial','salarioACombinar','companySize','descricao','horario','infoAdicional','regimeContrato','benef','empContratou','ppdInfo','anunciante','contratante','vagas','url_vaga'])
data_catho = data_catho.drop('empContratou','salarioACombinar','companySize','ppdInfo','data', 'dataRemovido')

#data_catho_joined = data_catho.withColumn('new_regime', f.concat(f.col('regimeContrato'),f.lit('\t'),f.col('infoAdicional')))
data_catho = data_catho.withColumn('descricao', f.concat(f.col('descricao'),f.lit('\t'),f.col('InfoAdicional')))
data_catho = data_catho.drop('InfoAdicional','contratante')

data_catho = data_catho.withColumn('reg_jorn_benef', f.concat(f.col('benef'),f.lit('\t'),f.col('regimeContrato'),f.lit('\t'),f.col('horario')))
data_catho = data_catho.drop('benef','regimeContrato','horario')

data_catho = data_catho.withColumnRenamed('faixaSalarial', 'salario')
data_catho = data_catho.withColumnRenamed('titulo', 'tipo_de_vaga')
data_catho = data_catho.withColumnRenamed('vagas', 'numero_de_vagas')


data_catho = data_catho.withColumn('descricao', f.concat(f.col('descricao'),f.lit('\t'),f.col('anunciante')))
data_catho = data_catho.drop('anunciante')

lower_case = [f.lower(f.col(x)).alias(x) for x in data_catho.columns]
data_catho = data_catho.select(*lower_case)

data_catho = data_catho.toPandas()

data_catho['numero_de_vagas'] = data_catho['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')

data_catho['descricao'] = data_catho['descricao'].str.replace('<li>','').str.replace('</li>','').str.replace('\t','').str.replace("{'nome':",'').str.replace("'confidencial':",'').str.replace("True}",'').str.replace("False}",'')



data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace('\t','').str.replace('[','').str.replace(']','').str.replace("'",'')

data_catho['salario'] = data_catho['salario'].str.extract(r'(\d\.\d\d\d,\d\d\s+[a-zA-Z]\s+[a-zA-Z]\$ \d\.\d\d\d,\d\d)')
#data_catho['salario'] = data_catho['salario'].replace('[a-zA-Z]\$','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('$','').str.replace('R','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('r','')

data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace(',',' ').str.replace('/','')

data_catho.head(3)

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re

# CATHO
#def data_inification():
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path_catho = '{uri}/raw/crw/catho/vagas/'.format(uri=var_adls_uri)
data_catho = spark.read.parquet(path_catho)
data_catho = data_catho.select(['data','dataRemovido','id','titulo','faixaSalarial','salarioACombinar','companySize','descricao','horario','infoAdicional','regimeContrato','benef','empContratou','ppdInfo','anunciante','contratante','vagas','url_vaga'])
data_catho = data_catho.drop('empContratou','salarioACombinar','companySize','ppdInfo','data', 'dataRemovido')

#data_catho_joined = data_catho.withColumn('new_regime', f.concat(f.col('regimeContrato'),f.lit('\t'),f.col('infoAdicional')))
data_catho = data_catho.withColumn('descricao', f.concat(f.col('descricao'),f.lit('\t'),f.col('InfoAdicional')))
data_catho = data_catho.drop('InfoAdicional','contratante')

data_catho = data_catho.withColumn('reg_jorn_benef', f.concat(f.col('benef'),f.lit('\t'),f.col('regimeContrato'),f.lit('\t'),f.col('horario')))
data_catho = data_catho.drop('benef','regimeContrato','horario')

data_catho = data_catho.withColumnRenamed('faixaSalarial', 'salario')
data_catho = data_catho.withColumnRenamed('titulo', 'tipo_de_vaga')
data_catho = data_catho.withColumnRenamed('vagas', 'numero_de_vagas')


data_catho = data_catho.withColumn('descricao', f.concat(f.col('descricao'),f.lit('\t'),f.col('anunciante')))
data_catho = data_catho.drop('anunciante')

lower_case = [f.lower(f.col(x)).alias(x) for x in data_catho.columns]
data_catho = data_catho.select(*lower_case)

data_catho = data_catho.toPandas()

data_catho['numero_de_vagas'] = data_catho['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')

data_catho['descricao'] = data_catho['descricao'].str.replace('<li>','').str.replace('</li>','').str.replace('\t','').str.replace("{'nome':",'').str.replace("'confidencial':",'').str.replace("True}",'').str.replace("False}",'')



data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace('\t','').str.replace('[','').str.replace(']','').str.replace("'",'')

data_catho['salario'] = data_catho['salario'].str.extract(r'(\d\.\d\d\d,\d\d\s+[a-zA-Z]\s+[a-zA-Z]\$ \d\.\d\d\d,\d\d)')
#data_catho['salario'] = data_catho['salario'].replace('[a-zA-Z]\$','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('$','').str.replace('R','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('r','')

data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace(',',' ').str.replace('/','')

data_catho



# VAGAS CERTAS

import pyspark.sql.functions as f

azure_adress = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
dataframe_vagas_certas = f"{azure_adress}/raw/crw/vagascertas/vagas/"
data_vagas_certas = spark.read.parquet(dataframe_vagas_certas)
data_vagas_certas = data_vagas_certas.drop('nm_arq_in','nr_reg', 'dh_insercao_raw', 'dh_arq_in', 'dataRemovido','salario')

list_of_columns_to_concat = ['conteudo','requisito','escolaridade','area_atuacao']
data_vagas_certas = data_vagas_certas.withColumn('descricao', f.concat(*list_of_columns_to_concat))
data_vagas_certas = data_vagas_certas.drop('conteudo','requisito','escolaridade')


list_of_columns_to_concat = ['observacao','beneficio']
data_vagas_certas = data_vagas_certas.withColumn('salario', f.concat(*list_of_columns_to_concat))
data_vagas_certas = data_vagas_certas.drop('observacao','beneficio')


data_vagas_certas = data_vagas_certas.withColumn('tipo_de_vaga', f.concat(f.col('titulo'),f.lit('\t'),f.col('area_atuacao')))
data_vagas_certas = data_vagas_certas.drop('titulo','area_atuacao')

data_vagas_certas = data_vagas_certas.withColumnRenamed('localidade', 'local')
data_vagas_certas = data_vagas_certas.withColumnRenamed('data', 'data_public')
data_vagas_certas = data_vagas_certas.withColumnRenamed('regime', 'reg_jorn_benef')


lower_case = [f.lower(f.col(x)).alias(x) for x in data_vagas_certas.columns]
data_vagas_certas = data_vagas_certas.select(*lower_case)

#display(data_vagas_certas)

data_vagas_certas = data_vagas_certas.toPandas()
data_vagas_certas['local'] = data_vagas_certas['tipo_de_vaga'].str.extract(r'\–( [a-z \–\s\ç\ã]+)')
data_vagas_certas['local'] = data_vagas_certas['local'].str.replace("\t",' ').str.replace("administração",' ').str.replace("–",' ')

data_vagas_certas['tipo_de_vaga'] = data_vagas_certas['tipo_de_vaga'].str.extract(r'([a-z\s\ç\ã\õ\á\ó\ú\í\é\â]+)')
data_vagas_certas['tipo_de_vaga'] = data_vagas_certas['tipo_de_vaga'].str.replace("\t",' ')

data_vagas_certas


# VAGAS COM

azure_adress = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
dataframe_vagas_com = f"{azure_adress}/raw/crw/vagascom/vagas/"
data_vagas_com = spark.read.parquet(dataframe_vagas_com)
data_vagas_com = data_vagas_com.drop('nm_arq_in','nr_reg','dh_insercao_raw','dh_arq_in','dataRemovido')

data_vagas_com = data_vagas_com.withColumnRenamed('titulo', 'tipo_de_vaga')
data_vagas_com = data_vagas_com.withColumnRenamed('qtdVagas', 'numero_de_vagas')
data_vagas_com = data_vagas_com.withColumnRenamed('localidade', 'local')
data_vagas_com = data_vagas_com.withColumnRenamed('conteudo', 'descricao')
data_vagas_com = data_vagas_com.withColumnRenamed('dadosEmpresa', 'empresa')
data_vagas_com = data_vagas_com.withColumnRenamed('dataPublicacao', 'data_public')
data_vagas_com = data_vagas_com.withColumnRenamed('beneficios', 'reg_jorn_benef')


lower_case = [f.lower(f.col(x)).alias(x) for x in data_vagas_com.columns]
data_vagas_com = data_vagas_com.select(*lower_case)

#display(vagas_com)
data_vagas_com = data_vagas_com.toPandas()

data_vagas_com['numero_de_vagas'] = data_vagas_com['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')
data_vagas_com['descricao'] = data_vagas_com['descricao'].str.replace("\r",' ').str.replace(";",' ').str.replace(",",' ').str.replace("●",' ').str.replace("-",' ')

data_vagas_com['reg_jorn_benef'] = data_vagas_com['reg_jorn_benef'].str.replace("[",' ').str.replace("]",' ').str.replace("'",' ').str.replace(",",'')

data_vagas_com['salario1'] = data_vagas_com['salario'].str.extract(r'([0-9\,\.]+)')
data_vagas_com['salario2'] = data_vagas_com['salario'].str.extract(r'(a r\$ [0-9\.]+)')
data_vagas_com['salario2'] = data_vagas_com['salario2'].str.replace("a r",' ').str.replace("$",' ')

data_vagas_com['salario3'] = data_vagas_com['salario1'] + data_vagas_com['salario2'] 
data_vagas_com = data_vagas_com.drop(['salario1','salario2','salario'], axis=1)
data_vagas_com = data_vagas_com.rename(columns={'salario3':'salario'})

data_vagas_com


# INFOJOBS
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path_infojobs = '{uri}/raw/crw/infojobs/vagas/'.format(uri=var_adls_uri)
data_infojobs = spark.read.parquet(path_infojobs)

# Dropping few columns
data_infojobs = data_infojobs.drop('nm_arq_in','nr_reg','dh_insercao_raw','dh_arq_in','dataRemovido')

# Joinning
data_infojobs = data_infojobs.withColumn('reg_jorn_benef', f.concat(f.col('beneficio'),f.lit('\t'),f.col('regime'),f.lit('\t'),f.col('jornada')))

# Joinning
# list_of_columns_to_concat = ['beneficio','regime','jornada']
# data_i = data_infojobs.withColumn('reg_jorn_benef', f.concat(*list_of_columns_to_concat))

# Dropping
data_infojobs = data_infojobs.drop('beneficio','regime','dh_arq_in','jornada')

# Renamed
data_infojobs = data_infojobs.withColumnRenamed('titulo', 'tipo_de_vaga')
data_infojobs = data_infojobs.withColumnRenamed('vagas', 'numero_de_vagas')
data_infojobs = data_infojobs.withColumnRenamed('localidade', 'local')
data_infojobs = data_infojobs.withColumnRenamed('conteudo', 'descricao')
data_infojobs = data_infojobs.withColumnRenamed('dadosEmpresa', 'empresa')

# Lower_case
lower_case = [f.lower(f.col(x)).alias(x) for x in data_infojobs.columns]
data_infojobs = data_infojobs.select(*lower_case)
#display(data_infojobs)


data_infojobs = data_infojobs.toPandas()
data_infojobs['reg_jorn_benef'] = data_infojobs['reg_jorn_benef'].str.replace('\t',' ').str.replace('–',' ').str.replace('-',' ')
data_infojobs['tipo_de_vaga'] = data_infojobs['tipo_de_vaga'].str.replace('–',' ').str.replace('-',' ').str.replace('+',' ').str.replace('/',' ').str.replace('|',' ')
data_infojobs['numero_de_vagas'] = data_infojobs['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')
data_infojobs['local'] = data_infojobs['local'].str.replace(',',' ')
data_infojobs['salario1'] = data_infojobs['salario'].str.extract(r'([0-9\,\.]+)')
data_infojobs['salario2'] = data_infojobs['salario'].str.extract(r'(!? [0-9\,\.]+)')
data_infojobs['salario3'] = data_infojobs['salario1'] + data_infojobs['salario2'] 
data_infojobs = data_infojobs.drop(['salario1','salario2','salario'], axis=1)
data_infojobs = data_infojobs.rename(columns={'salario3':'salario'})
data_infojobs = data_infojobs.rename(columns={'data':'data_public'})
data_infojobs


df = pd.concat([data_catho,data_vagas_certas,data_vagas_com,data_infojobs])

from pyspark.sql.types import (StructField, StringType, IntegerType, StructType)

data_schema = [StructField('id', StringType(), True),
              StructField('tipo_de_vaga', StringType(), True),
              StructField('salario', StringType(), True),
              StructField('descricao', StringType(), True),
              StructField('numero_de_vagas', StringType(), True),
              StructField('url_vaga', StringType(), True),
              StructField('reg_jorn_benef', StringType(), True),
              StructField('local', StringType(), True),
              StructField('data_public', StringType(), True),
              StructField('empresa', StringType(), True)]

final_struct = StructType(fields = data_schema)

df_v1 = spark.createDataFrame(df, schema=final_struct)

df_v1.show()

# COMMAND ----------

