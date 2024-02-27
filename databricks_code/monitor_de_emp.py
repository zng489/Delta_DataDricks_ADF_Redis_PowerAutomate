# Databricks notebook source
# MAGIC %md
# MAGIC # Importando libraries

# COMMAND ----------

from pyspark.sql.types import (StructField, StringType, IntegerType, StructType)
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as f
import pandas as pd
import numpy as np
import re

# COMMAND ----------

CATHO = (
  spark.read.format('parquet').option('header','true')\
  .load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/catho/vagas/')
)

# COMMAND ----------

CATHO = (
  spark.read.format('parquet').option('header','true')\
  .load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/catho/vagas/')\
  .drop('faixaSalarialId','jobQuantity','entradaEmp','dataAtualizacao','nm_arq_in','salarioACombinar','empId',
      'companySize','tipoEmp','localPreenchimento','perfilId','ppdPerfilId','hrenova','empContratou','grupoMidia',
      'habilidades','ppdInfo','anunciante','nr_reg','ppdFiltro','dh_insercao_raw','dh_arq_in','dataRemovido','salario')\
  .withColumn('reg_jorn_benef', 
              f.concat(f.col('regimeContrato'),f.lit(' '),f.col('InfoAdicional'),f.lit(' '),f.col('benef'),f.lit(' '),f.col('horario')))
  .drop('regimeContrato','InfoAdicional','benef','horario')\
  .withColumnRenamed('data', 'data_public')\
  .withColumnRenamed('faixaSalarial', 'salario')\
  .withColumnRenamed('contratante', 'empresa')\
  .withColumnRenamed('vagas', 'numero_de_vagas')\
  .withColumnRenamed('titulo', 'tipo_de_vaga')\
)

LOWER_CASE = [f.lower(f.col(x)).alias(x) for x in CATHO.columns]
CATHO_lower = CATHO.select(*LOWER_CASE)



#salario
CATHO_lower = CATHO_lower.withColumn('salario',regexp_extract(col('salario'), r'(\d\.\d\d\d,\d\d\s+[a-zA-Z]\s+[a-zA-Z]\$ \d\.\d\d\d,\d\d)',1))

def function_salario(col_name):
  from pyspark.sql.functions import regexp_replace,regexp_extract
  regexp = ("a|r|\$")
  return regexp_replace(col_name, regexp, "") 
CATHO_lower = CATHO_lower.withColumn('salario', function_salario(col('salario')))
CATHO_lower = CATHO_lower.withColumn('salario', regexp_replace(col('salario'), "   ", " "))



#numeros de vagas
def function_numero_de_vagas(col_name):
  from pyspark.sql.functions import regexp_replace,regexp_extract
  regexp = ("a|r|\$")
  return regexp_extract(col_name, r'(\d+)(?!.*\d)',1)
CATHO_lower = CATHO_lower.withColumn('numero_de_vagas', function_numero_de_vagas(col('numero_de_vagas')))



#descricao
from pyspark.sql.functions import regexp_replace

def function_descricao(col_name):
  removed_chars = ("<li>","</li>",";","\r",'\n','\t','"',"-",".","?")
  regexp = "|".join('{0}'.format(i) for i in removed_chars)
  return regexp_replace(col_name, regexp, "")
CATHO_lower = CATHO_lower.withColumn('descricao', remove_some_chars(col('descricao')))


#empresa
def function_empresa(column):
  return column.replace("{'nome':","").replace("'confidencial':","").replace("true}","").replace("false}","").replace(",","").replace("'","")
convertUDF = udf(lambda z: function_empresa(z),StringType())
CATHO_lower = CATHO_lower.withColumn('empresa', convertUDF(col('empresa')))

#reg_jorn_benef
#def function_reg_jorn_benef(col_name):
#  removed_chars = ('[',']','\t','\n','\n',"'",',','/','   ')
#  regexp = "|".join('\{0}'.format(i) for i in removed_chars)
#  return regexp_replace(col_name, regexp, "")
#
#CATHO_lower = CATHO_lower.withColumn('reg_jorn_benef', function_reg_jorn_benef(col('reg_jorn_benef')))




from pyspark.sql.functions import regexp_replace


def remove_some_chars(col_name):
  removed_chars = ("<li>","</li>",";","\r",'\n','\t','"',"-",".","?")
  regexp = "|".join('\{0}'.format(i) for i in removed_chars)
  return regexp_replace(col_name, regexp, "")

actual_df = CATHO_lower

for col_name in CATHO_lower.columns:
  actual_df = actual_df.withColumn(col_name, remove_some_chars(col_name))























#CATHO_lower = CATHO_lower.withColumn('reg_jorn_benef',regexp_replace(col('reg_jorn_benef'), '\[|\]|\,|\/,\\r|\\n|\\t|\;|\?', ""))

#',','').str.replace('/','').str.replace('\r','').str.replace('\n','').str.replace('\t','')


##reg_jorn_benef
#from pyspark.sql.functions import regexp_replace
#
#def function_descricao(col_name):
#  removed_chars = ("\'|disponibilidade")
#  return regexp_replace(col_name, removed_chars, "")
#CATHO_lower = CATHO_lower.withColumn('reg_jorn_benef', remove_some_chars(col('reg_jorn_benef')))


#'\t','[',']',"'",',','/','\r','\n','\t'
actual_df.display()


#CATHO_df = CATHO_lower


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Certas

# COMMAND ----------

from pyspark.sql.types import (StructField, StringType, IntegerType, StructType)
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as f
import pandas as pd
import numpy as np
import re



# Acessando datalake
azure_adress = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# Endereço do arquivo
dataframe_vagas_certas = f"{azure_adress}/raw/crw/vagascertas/vagas/"

data_vagas_certas = spark.read.parquet(dataframe_vagas_certas)


# dropping nas colunas
data_vagas_certas = data_vagas_certas.drop('nm_arq_in','nr_reg', 'dh_insercao_raw', 'dh_arq_in', 'dataRemovido','salario')

# concatenar
list_of_columns_to_concat = ['conteudo','requisito','escolaridade','area_atuacao']

# Criando columns from concat_list 
# Droppping colunas
data_vagas_certas = data_vagas_certas.withColumn('descricao', f.concat(*list_of_columns_to_concat)).drop('conteudo','requisito','escolaridade')


# Concatenar
# Criando columns from concat_list 
# Droppping colunas
list_of_columns_to_concat = ['observacao','beneficio']
data_vagas_certas = data_vagas_certas.withColumn('salario', f.concat(*list_of_columns_to_concat)).drop('observacao','beneficio')
data_vagas_certas = data_vagas_certas.drop('observacao','beneficio')
data_vagas_certas = data_vagas_certas.withColumn('tipo_de_vaga', f.concat(f.col('titulo'),f.lit('\t'),f.col('area_atuacao'))).drop('titulo','area_atuacao')

# Renomeando colunas
data_vagas_certas = data_vagas_certas.withColumnRenamed('localidade', 'local')
data_vagas_certas = data_vagas_certas.withColumnRenamed('data', 'data_public')
data_vagas_certas = data_vagas_certas.withColumnRenamed('regime', 'reg_jorn_benef')

# Transformando em lower_case
lower_case = [f.lower(f.col(x)).alias(x) for x in data_vagas_certas.columns]
data_vagas_certas = data_vagas_certas.select(*lower_case)
 

# COMMAND ----------

data_vagas_certas.display()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType  
# Declare the function and create the UDF

@pandas_udf("string")
#def mean_udf(v):
#    return v.mean()
def mean_udf(v):
    return v.replace("vagascertas"," ")

# COMMAND ----------

dfdf = data_vagas_certas.withColumn('url_vaga', mean_udf(col('url_vaga')))

# COMMAND ----------

dfdf.display()

# COMMAND ----------

# Transformando em Pandas para as extrações de termos
data_vagas_certas = data_vagas_certas.toPandas()

data_vagas_certas['local'] = data_vagas_certas['tipo_de_vaga'].str.extract(r'\–( [a-z \–\s\ç\ã]+)')
data_vagas_certas['local'] = data_vagas_certas['local'].str.replace("\t",' ').str.replace("administração",' ').str.replace("–",' ')

data_vagas_certas['tipo_de_vaga'] = data_vagas_certas['tipo_de_vaga'].str.extract(r'([a-z\s\ç\ã\õ\á\ó\ú\í\é\â]+)')
data_vagas_certas['tipo_de_vaga'] = data_vagas_certas['tipo_de_vaga'].str.replace("\t",' ')
data_vagas_certas['numero_de_vagas'] = np.nan
data_vagas_certas['empresa'] = np.nan
data_vagas_certas = data_vagas_certas[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]

data_vagas_certas

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

data_catho['empresa'] = data_catho['empresa'].str.replace("{'nome':","").str.replace("'","").str.replace("confidencial:","").str.replace("true}","").str.replace("false}","").str.replace(",","")
#

def function_empresa(column):
  column.str.replace("{'nome':","")
  return column.str.replace("{'nome':","")
convertUDF = udf(lambda z: function_empresa(z),StringType())
                 
CATHO.withColumn(each_column, convertUDF(col(each_column)))




# COMMAND ----------

from pyspark.sql.functions import regexp_replace


def remove_some_chars(col_name):
  removed_chars = ("<li>","</li>",";","\r",'\n','\t','"',"-",".","?")
  regexp = "|".join('\{0}'.format(i) for i in removed_chars)
  return regexp_replace(col_name, regexp, "")

actual_df = CATHO_lower

for col_name in CATHO_lower.columns:
  actual_df = actual_df.withColumn(col_name, remove_some_chars(col_name))

# COMMAND ----------

removed_chars = ("<li>", "?")
regexp = "|".join('\{0}'.format(i) for i in removed_chars)

print(regexp)
# \<li>|\?


# COMMAND ----------

from pyspark.sql.functions import regexp_replace


def function_descricao(col_name):
  removed_chars = ('<li>','</li>',';','\r','\n','"','\t',"-",".","?")
  regexp = "|".join('\{0}'.format(i) for i in removed_chars)
  return regexp_replace(col_name, regexp, "") 
'''
myTuple = ("John", "Peter", "Vicky")
x = "#".join(myTuple)
print(x)
John#Peter#Vicky
''' 
CATHO_df  = CATHO_lower.withColumn('descricao', function_descricao(col('descricao')))



# COMMAND ----------

from pyspark.sql.functions import regexp_replace


def remove_some_chars(col_name):
  removed_chars = ("<li>","</li>",";","\r",'\n','\t','"',"{'nome':","'confidencial':","True}","False}",

  regexp = "|".join('\{0}'.format(i) for i in removed_chars)
  return regexp_replace(col_name, regexp, "")
  
'''
myTuple = ("John", "Peter", "Vicky")
x = "#".join(myTuple)
print(x)
John#Peter#Vicky
'''
actual_df = CATHO_lower


for col_name in CATHO_lower.columns:
  actual_df = actual_df.withColumn(col_name, remove_some_chars(col_name))

# COMMAND ----------

removed_chars = ("!", "?")
regexp = "|".join('\{0}'.format(i) for i in removed_chars)

print(regexp)

# COMMAND ----------

  for col in df.columns:
      col_renamed = regex.sub('', normalize('NFKD', col.strip())
                             .encode('ASCII', 'ignore')        
                             .decode('ASCII')       
                              
                             .replace(' ', '_')                    
                             .replace('-', '_')
                             .replace('/', '_')
                             .replace('$', 'S')
                             .upper())
      df = df.withColumnRenamed(col, col_renamed)
  return df

# COMMAND ----------

def remove_some_chars(col_name):
  #newDf = testDF.withColumn('d_id', regexp_replace('d_id', 'a', '0'))
  x = "'<li>', ''"
  #return regexp_replace(col_name, x)
  return x


# COMMAND ----------

remove_some_chars(col_name)

# COMMAND ----------

def remove_some_chars(col_name):
    removed_chars = ("<li>", "</li>")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")
  
actual_df = source_df


for col_name in CATHO_lower.columns:
  actual_df = actual_df.withColumn(col_name, remove_some_chars(col_name))

# COMMAND ----------

column_lists = ['id','tipo_de_vaga','data_public','salario','descricao','empresa','numero_de_vagas','url_vaga','reg_jorn_benef']


def remove_some_chars(col_name):
    removed_chars = ("!", "?")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")
  
  actual_df = source_df

for col_name in ["sport", "team"]:
    actual_df = actual_df.withColumn(col_name, remove_some_chars(col_name))

# COMMAND ----------

def rename_columns(df):
  from unicodedata import normalize  
  import re
  
  regex = re.compile(r'[.,;{}()\n\t=]')
  for col in df.columns:
      col_renamed = regex.sub('', normalize('NFKD', col.strip())
                             .encode('ASCII', 'ignore')        
                             .decode('ASCII')       
                              
                             .replace(' ', '_')                    
                             .replace('-', '_')
                             .replace('/', '_')
                             .replace('$', 'S')
                             .upper())
      df = df.withColumnRenamed(col, col_renamed)
  return df

.str.replace('<li>', '').str.replace('</li>','')

# COMMAND ----------

# Extraindo termos, regex, e replacing
data_catho['numero_de_vagas'] = data_catho['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')
 
data_catho['descricao'] = data_catho['descricao'].str.replace('<li>', '').str.replace('</li>','').str.replace(';','').str.replace('\r','').str.replace('\n','').str.replace('"', '').str.replace('\t','').str.replace("{'nome':",'').str.replace("'confidencial':",'').str.replace("True}",'').str.replace("False}",'').str.replace("-",'').str.replace(".","").str.replace("?","")
 
 
data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace('\t','').str.replace('[','').str.replace(']','').str.replace("'",'')
 
data_catho['salario'] = data_catho['salario'].str.extract(r'(\d\.\d\d\d,\d\d\s+[a-zA-Z]\s+[a-zA-Z]\$ \d\.\d\d\d,\d\d)')
data_catho['salario'] = data_catho['salario'].replace('[a-zA-Z]\$','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('$','').str.replace('R','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('r','')
 
data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace(',','').str.replace('/','').str.replace('\r','').str.replace('\n','').str.replace('\t','')
 
data_catho['empresa'] = data_catho['empresa'].str.replace("{'nome':","").str.replace("'","").str.replace("confidencial:","").str.replace("true}","").str.replace("false}","").str.replace(",","")
#
data_catho['data_public'] = data_catho['data_public'].str.extract(r'(..........)')
data_catho['local'] = np.nan
 
data_catho = data_catho[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]
 
data_catho
#data_catho.head(3)

# COMMAND ----------

def lower_case(X):
  return f.lower(X)

'''- Try to use preferably pyspark functions:
Use this one f.lower, instead of .lower()'''

convertUDF = udf(lambda z: lower_case(z),StringType())
                 
for each_column in CATHO.columns:
  CATHO_1 = CATHO.withColumn(each_column, convertUDF(col(each_column)))

# COMMAND ----------

teste = ['ZHANG', 'YUAN']

exit = []
for each_column in teste:
  column = each_column.lower()
  exit.append(column)
print(exit)

exit_1 = [each_column + ' words' for each_column in teste] 
print(exit_1)

# COMMAND ----------

c

# COMMAND ----------

def lower_case(X):
  for each_column in X:
    column = each_column.lower()
  return list(column)

# COMMAND ----------

lower_case(teste)

# COMMAND ----------

# MAGIC %md
# MAGIC # Vagas do Catho

# COMMAND ----------

# Endereço do data lake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# Caminho do arquivo da qual será lido
path_catho = '{uri}/raw/crw/catho/vagas/'.format(uri=var_adls_uri)

# Run
data_catho = spark.read.parquet(path_catho)
#display(data_catho)

# COMMAND ----------

data_catho.display()

# COMMAND ----------

#def test_of_udf(x):
#  a = x.replace('<li>', '').replace('</li>', '').replace(';', '').replace('\r','').replace('\n','')
#  #b = a.replace('</li>', '')
#  #c = b.replace('"', '')
#  return a
#function_test_of_udf = udf(lambda x: test_of_udf(x), StringType())
##if __name__ == "__main__":
##  test_of_udf(3)
#
#data_catho= data_catho.withColumn('PYSPARK_descricao', function_test_of_udf(col('descricao')))

# COMMAND ----------

# Transformando em lower_case
lower_case = [f.lower(f.col(x)).alias(x) for x in data_catho.columns]
data_catho = data_catho.select(*lower_case)

# dropping em colunas
data_catho = data_catho.drop('faixaSalarialId',
                             'jobQuantity',
                             'entradaEmp',
                             'dataAtualizacao',
                             'nm_arq_in',
                             'salarioACombinar',
                             'empId',
                             'companySize',
                             'tipoEmp',
                             'localPreenchimento',
                             'perfilId',
                             'ppdPerfilId',
                             'hrenova',
                             'empContratou',
                             'grupoMidia',
                             'habilidades',
                             'ppdInfo',
                             'anunciante',
                             'nr_reg',
                             'ppdFiltro',
                             'dh_insercao_raw',
                             'dh_arq_in',
                             'dataRemovido',
                             'salario')

# Preenchendo " " nas colunas infoAdicional, regimeContrato
data_catho = data_catho.na.fill(" ",["infoAdicional"]).na.fill(" ",["regimeContrato"])

# concat colunas regimeContrato, InfoAdicional, benef, horario
# dropping nas colunas 'regimeContrato','InfoAdicional','benef','horario'
data_catho = data_catho.withColumn('reg_jorn_benef', f.concat(f.col('regimeContrato'),f.lit(' '),f.col('InfoAdicional'),f.lit(' '),f.col('benef'),f.lit(' '),f.col('horario'))).drop('regimeContrato','InfoAdicional','benef','horario')


# Renomeando
data_catho = data_catho.withColumnRenamed('data', 'data_public').withColumnRenamed('faixaSalarial', 'salario').withColumnRenamed('contratante', 'empresa').withColumnRenamed('vagas', 'numero_de_vagas').withColumnRenamed('titulo', 'tipo_de_vaga')

data_catho = data_catho.toPandas()
#display(data_catho)

# COMMAND ----------



# Extraindo termos, regex, e replacing
data_catho['numero_de_vagas'] = data_catho['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')

data_catho['descricao'] = data_catho['descricao'].str.replace('<li>', '').str.replace('</li>','').str.replace(';','').str.replace('\r','').str.replace('\n','').str.replace('"', '').str.replace('\t','').str.replace("{'nome':",'').str.replace("'confidencial':",'').str.replace("True}",'').str.replace("False}",'').str.replace("-",'').str.replace(".","").str.replace("?","")


data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace('\t','').str.replace('[','').str.replace(']','').str.replace("'",'')

data_catho['salario'] = data_catho['salario'].str.extract(r'(\d\.\d\d\d,\d\d\s+[a-zA-Z]\s+[a-zA-Z]\$ \d\.\d\d\d,\d\d)')
data_catho['salario'] = data_catho['salario'].replace('[a-zA-Z]\$','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('$','').str.replace('R','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('r','')

data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace(',','').str.replace('/','').str.replace('\r','').str.replace('\n','').str.replace('\t','')

data_catho['empresa'] = data_catho['empresa'].str.replace("{'nome':","").str.replace("'","").str.replace("confidencial:","").str.replace("true}","").str.replace("false}","").str.replace(",","")
#
data_catho['data_public'] = data_catho['data_public'].str.extract(r'(..........)')
data_catho['local'] = np.nan

data_catho = data_catho[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]

data_catho
#data_catho.head(3)

# COMMAND ----------

#df = data_catho
#df = df.astype(str)
#df[["id","descricao"]].head(50)

# COMMAND ----------

#df = data_catho
#df = df.astype(str)
#df
#
#df = spark.createDataFrame(df)
#display(df)

# COMMAND ----------

#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'  
#PATH_TARGET="/uds/uniepro/export/rais_vinculo/monitor_de_emprego_arrumado.csv"  
#
#
#target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)     
##df = (spark.read.parquet(target_path)).sample(fraction=0.1)
#df.repartition(1).write.mode("overwrite").option('header', True).csv(target_path)       
#
#
#
#data_lake_access = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#ENDERECO = '/uds/uniepro/export/rais_vinculo/monitor_de_emprego_arrumado.csv'
#
#file_path = '{address}{SEU_ARQUIVO}'.format(address=data_lake_access, SEU_ARQUIVO=ENDERECO)
#data = spark.read.option('delimiter', ',').csv(file_path, header=True)

# COMMAND ----------

#data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Vagas Certas

# COMMAND ----------

# Acessando datalake
azure_adress = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# Endereço do arquivo
dataframe_vagas_certas = f"{azure_adress}/raw/crw/vagascertas/vagas/"

data_vagas_certas = spark.read.parquet(dataframe_vagas_certas)




# COMMAND ----------

# dropping nas colunas
data_vagas_certas = data_vagas_certas.drop('nm_arq_in','nr_reg', 'dh_insercao_raw', 'dh_arq_in', 'dataRemovido','salario')

# concatenar
list_of_columns_to_concat = ['conteudo','requisito','escolaridade','area_atuacao']

# Criando columns from concat_list 
# Droppping colunas
data_vagas_certas = data_vagas_certas.withColumn('descricao', f.concat(*list_of_columns_to_concat)).drop('conteudo','requisito','escolaridade')


# Concatenar
# Criando columns from concat_list 
# Droppping colunas
list_of_columns_to_concat = ['observacao','beneficio']
data_vagas_certas = data_vagas_certas.withColumn('salario', f.concat(*list_of_columns_to_concat)).drop('observacao','beneficio')
data_vagas_certas = data_vagas_certas.drop('observacao','beneficio')
data_vagas_certas = data_vagas_certas.withColumn('tipo_de_vaga', f.concat(f.col('titulo'),f.lit('\t'),f.col('area_atuacao'))).drop('titulo','area_atuacao')

# Renomeando colunas
data_vagas_certas = data_vagas_certas.withColumnRenamed('localidade', 'local')
data_vagas_certas = data_vagas_certas.withColumnRenamed('data', 'data_public')
data_vagas_certas = data_vagas_certas.withColumnRenamed('regime', 'reg_jorn_benef')

# Transformando em lower_case
lower_case = [f.lower(f.col(x)).alias(x) for x in data_vagas_certas.columns]
data_vagas_certas = data_vagas_certas.select(*lower_case)


# COMMAND ----------

# Transformando em Pandas para as extrações de termos
data_vagas_certas = data_vagas_certas.toPandas()

data_vagas_certas['local'] = data_vagas_certas['tipo_de_vaga'].str.extract(r'\–( [a-z \–\s\ç\ã]+)')
data_vagas_certas['local'] = data_vagas_certas['local'].str.replace("\t",' ').str.replace("administração",' ').str.replace("–",' ')

data_vagas_certas['tipo_de_vaga'] = data_vagas_certas['tipo_de_vaga'].str.extract(r'([a-z\s\ç\ã\õ\á\ó\ú\í\é\â]+)')
data_vagas_certas['tipo_de_vaga'] = data_vagas_certas['tipo_de_vaga'].str.replace("\t",' ')
data_vagas_certas['numero_de_vagas'] = np.nan
data_vagas_certas['empresa'] = np.nan
data_vagas_certas = data_vagas_certas[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]

data_vagas_certas

# COMMAND ----------

# MAGIC %md 
# MAGIC # Vagas COM

# COMMAND ----------

# Acessando Data Lake
azure_adress = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# Endereço do arquivo
dataframe_vagas_com = f"{azure_adress}/raw/crw/vagascom/vagas/"
data_vagas_com = spark.read.parquet(dataframe_vagas_com)

# Dropping colunas
data_vagas_com = data_vagas_com.drop('nm_arq_in','nr_reg','dh_insercao_raw','dh_arq_in','dataRemovido')

# Renomeando
data_vagas_com = data_vagas_com.withColumnRenamed('titulo', 'tipo_de_vaga').withColumnRenamed('qtdVagas', 'numero_de_vagas').withColumnRenamed('localidade', 'local').withColumnRenamed('conteudo', 'descricao').withColumnRenamed('dadosEmpresa', 'empresa').withColumnRenamed('dataPublicacao', 'data_public').withColumnRenamed('beneficios', 'reg_jorn_benef')

# Transformando em lower case
lower_case = [f.lower(f.col(x)).alias(x) for x in data_vagas_com.columns]
data_vagas_com = data_vagas_com.select(*lower_case)

# Transformando me Pandas
data_vagas_com = data_vagas_com.toPandas()

# Extração de termos
data_vagas_com['numero_de_vagas'] = data_vagas_com['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')
data_vagas_com['descricao'] = data_vagas_com['descricao'].str.replace("\r",' ').str.replace(";",' ').str.replace(",",' ').str.replace("●",' ').str.replace("-",' ')

data_vagas_com['reg_jorn_benef'] = data_vagas_com['reg_jorn_benef'].str.replace("[",' ').str.replace("]",' ').str.replace("'",' ').str.replace(",",'')

data_vagas_com['salario1'] = data_vagas_com['salario'].str.extract(r'([0-9\,\.]+)')
data_vagas_com['salario2'] = data_vagas_com['salario'].str.extract(r'(a r\$ [0-9\.]+)')
data_vagas_com['salario2'] = data_vagas_com['salario2'].str.replace("a r",' ').str.replace("$",' ')

data_vagas_com['salario3'] = data_vagas_com['salario1'] + data_vagas_com['salario2'] 
data_vagas_com = data_vagas_com.drop(['salario1','salario2','salario'], axis=1)
data_vagas_com = data_vagas_com.rename(columns={'salario3':'salario'})

data_vagas_com['data_public'] = data_vagas_com['data_public'].str.replace("publicada",'').str.replace("há",'').str.replace(" em",'')

data_vagas_com = data_vagas_com[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]

data_vagas_com

# COMMAND ----------

# MAGIC %md
# MAGIC # Vagas Infojobs

# COMMAND ----------

# Acessando datalake e o endereço do arquivo
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path_infojobs = '{uri}/raw/crw/infojobs/vagas/'.format(uri=var_adls_uri)
data_infojobs = spark.read.parquet(path_infojobs)

# Dropping few columns
data_infojobs = data_infojobs.drop('nm_arq_in','nr_reg','dh_insercao_raw','dh_arq_in','dataRemovido')

# Joinning
data_infojobs = data_infojobs.withColumn('reg_jorn_benef', f.concat(f.col('beneficio'),f.lit('\t'),f.col('regime'),f.lit('\t'),f.col('jornada')))

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

# Transformando em Pandas
data_infojobs = data_infojobs.toPandas()

# Extração de termos
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
data_infojobs = data_infojobs[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]

data_infojobs

# COMMAND ----------

# MAGIC %md
# MAGIC # Concat nos DataFrames

# COMMAND ----------

df = pd.concat([data_catho,data_vagas_certas,data_vagas_com,data_infojobs])
df = df.astype(str)
df

#df = spark.createDataFrame(df)
#display(df)

# COMMAND ----------

df = spark.createDataFrame(df)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 SAVE #########################################################################################################################################################################

# COMMAND ----------

#PATH_FONTE ="/raw/usr/me/rais_vinculo/" #substitua o caminho para a sua fonte
#PATH_TARGET="/uds/uniepro/export/rais_vinculo/rais_vinculo.csv" #substitua o caminho para o destino
#origin.coalesce(1).write.option('header', True).csv(target_path)

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_TARGET="/uds/uniepro/inteligencia_ocupacional/monitor_de_emprego.csv"
target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)
#df = (spark.read.parquet(target_path)).sample(fraction=0.1)
df.repartition(1).write.mode("overwrite").option('header', True).csv(target_path)






var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data_lake_access = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
ENDERECO="/uds/uniepro/inteligencia_ocupacional/monitor_de_emprego.csv"


file_path = '{address}{SEU_ARQUIVO}'.format(address=data_lake_access, SEU_ARQUIVO=ENDERECO)
# Run
data = spark.read.csv(file_path, header=True)

display(data)

# COMMAND ----------

#########################################################################################################################################

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2 SAVE #########################################################################################################################################################################

# COMMAND ----------

monitor_corrigido = data.toPandas()

# COMMAND ----------

monitor_corrigido = monitor_corrigido.fillna('NONE')

# COMMAND ----------

monitor_corrigido.head(20)

# COMMAND ----------

monitor = monitor_corrigido[(monitor_corrigido['id']).str.contains("[0-9]+")]

# COMMAND ----------

monitor = spark.createDataFrame(monitor)
display(monitor)

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_TARGET="/uds/uniepro/inteligencia_ocupacional/monitor_de_emprego_parte_1.csv"
target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)
#df = (spark.read.parquet(target_path)).sample(fraction=0.1)
monitor.repartition(1).write.option('header', True).csv(target_path)

# COMMAND ----------

################################################################################################

# COMMAND ----------

monitor_de_emprego_parte_2_error = monitor_corrigido[~(monitor_corrigido['id']).str.contains("[0-9]+")]

# COMMAND ----------

monitor_de_emprego_parte_2_error = spark.createDataFrame(monitor_de_emprego_parte_2_error)
display(monitor_de_emprego_parte_2_error)

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_TARGET="/uds/uniepro/inteligencia_ocupacional/monitor_de_emprego_parte_2.csv"
target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)
#df = (spark.read.parquet(target_path)).sample(fraction=0.1)
monitor_de_emprego_parte_2_error.repartition(1).write.option('header', True).csv(target_path)

# COMMAND ----------

#####################################################################################################################################################################################################################

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_data_only_ = data_catho[(data_catho['id']).str.contains("[0-9]+")]
df_data_only_

# COMMAND ----------

df_descricao = df[['id', 'tipo_de_vaga', 'salario', 'numero_de_vagas', 'url_vaga', 'reg_jorn_benef', 'data_public', 'empresa']]
df_descricao = spark.createDataFrame(df_descricao)
#df_1.describe()
# id	tipo_de_vaga	salario	descricao	numero_de_vagas	url_vaga	reg_jorn_benef	local	data_public	empresa
display(df_descricao)

# COMMAND ----------



#PATH_FONTE ="/raw/usr/me/rais_vinculo/" #substitua o caminho para a sua fonte
#PATH_TARGET="/uds/uniepro/export/rais_vinculo/rais_vinculo.csv" #substitua o caminho para o destino
#origin.coalesce(1).write.option('header', True).csv(target_path)

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_TARGET="/uds/uniepro/inteligencia_ocupacional/1.csv"
target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)
#df = (spark.read.parquet(target_path)).sample(fraction=0.1)
zxc.repartition(1).write.option('header', True).csv(target_path)



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data_lake_access = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
ENDERECO="/uds/uniepro/inteligencia_ocupacional/1.csv"


file_path = '{address}{SEU_ARQUIVO}'.format(address=data_lake_access, SEU_ARQUIVO=ENDERECO)
# Run
data = spark.read.csv(file_path, header=True)

display(data)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_descricao = df[['descricao','id']]
df_descricao
df_descricao = spark.createDataFrame(df_descricao)
#df_1.describe()
display(df_descricao)

# COMMAND ----------

#df_2 = df[['tipo_de_vaga']]
#df_2 = spark.createDataFrame(df_2)

#df_2.describe()


#rty = pd.merge(product,customer,on='Product_ID',how='outer')

# qwe = df_1 + df_2


# COMMAND ----------

mySchema = StructType([StructField("id", StringType(), True)\
                       ,StructField("salario", StringType(), True)\
                       ,StructField("tipo_de_vaga", StringType(), True)\
                       ,StructField("descricao", StringType(), True)\
                       ,StructField("numero_de_vagas", StringType(), True)\
                       ,StructField("url_vaga", StringType(), True)\
                       ,StructField("reg_jorn_benef", StringType(), True)\
                       ,StructField("local", StringType(), True)\
                       ,StructField("data_public", StringType(), True)\
                       ,StructField("empresa", StringType(), True)])

# COMMAND ----------

df = spark.createDataFrame(df,schema=mySchema)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.toDF()
df.printSchema()

# COMMAND ----------

df.head(15)

# COMMAND ----------

df = spark.createDataFrame(df)
type(df)

# COMMAND ----------

display(data)

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------

#PATH_FONTE ="/raw/usr/me/rais_vinculo/" #substitua o caminho para a sua fonte
#PATH_TARGET="/uds/uniepro/export/rais_vinculo/rais_vinculo.csv" #substitua o caminho para o destino
#origin.coalesce(1).write.option('header', True).csv(target_path)

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_TARGET="/uds/uniepro/inteligencia_ocupacional/teste_1.csv"
target_path = '{uri}{target}'.format(uri=var_adls_uri, target= PATH_TARGET)
#df = (spark.read.parquet(target_path)).sample(fraction=0.1)
data_catho.repartition(1).write.option('header', True).csv(target_path)



# COMMAND ----------



# COMMAND ----------

# Save file to HDFS
df.write.format('csv').option('header', True).mode('overwrite').option('sep', '|').save('/output.csv')

# COMMAND ----------

df_1 = df[['id', 'tipo_de_vaga']]
df_1 = spark.createDataFrame(df_1)
#df_1.describe()
display(df_1)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Opening File
# MAGIC
# MAGIC

# COMMAND ----------



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data_lake_access = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
ENDERECO="/uds/uniepro/inteligencia_ocupacional/teste_1.csv"


file_path = '{address}{SEU_ARQUIVO}'.format(address=data_lake_access, SEU_ARQUIVO=ENDERECO)
# Run
data = spark.read.csv(file_path, header=True)

display(data)

# COMMAND ----------

data.show()

# COMMAND ----------

da

# COMMAND ----------



# COMMAND ----------

ENDERECO = '/uds/uniepro/empresas_Alimento_Bebidas/empresas_alim_beb.csv'



# Endereço do data lake
data_lake_access = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
# Caminho do arquivo da qual será lido
file_path = '{address}{SEU_ARQUIVO}'.format(address=data_lake_access, SEU_ARQUIVO=ENDERECO)
# Run
data = spark.read.option('delimiter',';').csv(file_path, header=True)

display(data)
# Número de empregados

# COMMAND ----------


"""

# CATHO
#def data_inification():
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path_catho = '{uri}/raw/crw/catho/vagas/'.format(uri=var_adls_uri)
data_catho = spark.read.parquet(path_catho)


# Lower_case

lower_case = [f.lower(f.col(x)).alias(x) for x in data_catho.columns]
data_catho = data_catho.select(*lower_case)


# Columns deleted salarioACombinar','empId','companySize','tipoEmp','localPreenchimento','perfilId','ppdPerfilId','hrenova','empContratou','grupoMidia','habilidades','ppdInfo','anunciante','nr_reg','dh_insercao_raw','dh_arq_in','dataRemovido'

data_catho = data_catho.drop('faixaSalarialId',
                             'jobQuantity',
                             'entradaEmp',
                             'dataAtualizacao',
                             'nm_arq_in',
                             'salarioACombinar',
                             'empId',
                             'companySize',
                             'tipoEmp',
                             'localPreenchimento',
                             'perfilId',
                             'ppdPerfilId',
                             'hrenova',
                             'empContratou',
                             'grupoMidia',
                             'habilidades',
                             'ppdInfo',
                             'anunciante',
                             'nr_reg',
                             'ppdFiltro',
                             'dh_insercao_raw',
                             'dh_arq_in',
                             'dataRemovido',
                             'salario')

# concat_we and collease doesn't work, i dunno
# data_catho = data_catho.withColumn('benef', regexp_replace('[]', ' '))

data_catho = data_catho.na.fill(" ",["infoAdicional"]).na.fill(" ",["regimeContrato"])
data_catho = data_catho.withColumn('reg_jorn_benef', f.concat(f.col('regimeContrato'),f.lit(' '),f.col('InfoAdicional'),f.lit(' '),f.col('benef'),f.lit(' '),f.col('horario')))

data_catho = data_catho.drop('regimeContrato','InfoAdicional','benef','horario')


############################################################################################################################################

data_catho = data_catho.withColumnRenamed('data', 'data_public')
data_catho = data_catho.withColumnRenamed('faixaSalarial', 'salario')
data_catho = data_catho.withColumnRenamed('contratante', 'empresa')
data_catho = data_catho.withColumnRenamed('vagas', 'numero_de_vagas')
data_catho = data_catho.withColumnRenamed('titulo', 'tipo_de_vaga')

data_catho = data_catho.toPandas()

data_catho['numero_de_vagas'] = data_catho['numero_de_vagas'].str.extract(r'(\d+)(?!.*\d)')

data_catho['descricao'] = data_catho['descricao'].str.replace('<li>','').str.replace('</li>','').str.replace('\t','').str.replace("{'nome':",'').str.replace("'confidencial':",'').str.replace("True}",'').str.replace("False}",'')

data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace('\t','').str.replace('[','').str.replace(']','').str.replace("'",'')

data_catho['salario'] = data_catho['salario'].str.extract(r'(\d\.\d\d\d,\d\d\s+[a-zA-Z]\s+[a-zA-Z]\$ \d\.\d\d\d,\d\d)')
data_catho['salario'] = data_catho['salario'].replace('[a-zA-Z]\$','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('$','').str.replace('R','').str.replace('a','')
data_catho['salario'] = data_catho['salario'].str.replace('r','')

data_catho['reg_jorn_benef'] = data_catho['reg_jorn_benef'].str.replace(',',' ').str.replace('/','')
data_catho['empresa'] = data_catho['empresa'].str.replace("{'nome':","").str.replace("'","").str.replace("confidencial:","").str.replace("true}","").str.replace("false}","").str.replace(",","")
#
data_catho['data_public'] = data_catho['data_public'].str.extract(r'(..........)')
data_catho['local'] = np.nan

data_catho = data_catho[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]
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
data_vagas_certas['numero_de_vagas'] = np.nan
data_vagas_certas['empresa'] = np.nan
data_vagas_certas = data_vagas_certas[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]
data_vagas_certas







import pyspark.sql.functions as f

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

data_vagas_com['data_public'] = data_vagas_com['data_public'].str.replace("publicada",'').str.replace("há",'').str.replace(" em",'')

data_vagas_com = data_vagas_com[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]
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
data_infojobs = data_infojobs[['id','tipo_de_vaga','salario','descricao','numero_de_vagas','url_vaga','reg_jorn_benef','local','data_public','empresa']]
data_infojobs

df = pd.concat([data_catho,data_vagas_certas,data_vagas_com,data_infojobs])
df = df.astype(str)

"""

# COMMAND ----------

"""
#df.to_csv('monitor_de_emprego_1000.csv')

#display = df.sample(1000)
#display
df = spark.createDataFrame(df)
type(df)

df.write.option("header").csv("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/infojobs/vagas/")

#display(df)
#abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net

#df.write.format("csv").save("/raw/crw/infojobs/vagas/datacsv")
origin.coalesce(1).write.option('header', True).csv(target_path

(df
.coalesce(1)
.write
.format('csv')
.save(var_adls_uri + '/uds/uniepro/monitor_emprego/monitor_emprego_1000.csv', sep = ";", header = True, mode = 'overwrite', encoding = 'latin1'))


(df
.coalesce(1)
.write
.format('csv')
.save(var_adls_uri + '/uds/uniepro/monitor_emprego/monitor_emprego_1000.csv', sep = ";", header = True, mode = 'overwrite', encoding = 'latin1'))

"""