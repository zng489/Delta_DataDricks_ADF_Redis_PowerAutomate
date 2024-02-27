# Databricks notebook source
#=====X====X====X====X====X====X====X
from unicodedata import normalize
import json
import re
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import datetime

#=====X====X====X====X====X====X====X
# importando functions e o file bruto da RAIS 2020
from pyspark.sql.functions import when,col,lit

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/2019'

path = var_adls_uri + de_para_path

df = spark.read\
.format("csv")\
.option("header","true")\
.option('sep', ';')\
.option("header","true")\
.option('sep', ';')\
.option('encoding','utf-8')\
.option('mode','FAILFAST')\
.option('ignoreLeadingWhiteSpace','true')\
.option('ignoreTrailingWhiteSpace','true')\
.option('encoding', 'ISO-8859-1')\
.option('encoding', 'latin1')\
.load(path)

# COMMAND ----------

import asyncio




async def get():
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/2019'
  
  path = var_adls_uri + de_para_path
  
  df = spark.read\
  .format("csv")\
  .option("header","true")\
  .option('sep', ';')\
  .option("header","true")\
  .option('sep', ';')\
  .option('encoding','utf-8')\
  .option('mode','FAILFAST')\
  .option('ignoreLeadingWhiteSpace','true')\
  .option('ignoreTrailingWhiteSpace','true')\
  .option('encoding', 'ISO-8859-1')\
  .option('encoding', 'latin1')\
  .load(path)
  return display(df)

await get()
  

# COMMAND ----------

display(df)

# COMMAND ----------

def transform(sef, f):
  a = 4 + f
  return f(self)

# COMMAND ----------

transform(4)

# COMMAND ----------

# MAGIC %md
# MAGIC # Applying UDF to create a new column in PySpark

# COMMAND ----------

#=====X====X====X====X====X====X====X
from unicodedata import normalize
import json
import re
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import datetime

#=====X====X====X====X====X====X====X
# importando functions e o file bruto da RAIS 2020
from pyspark.sql.functions import when,col,lit

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/2019'

path = var_adls_uri + de_para_path

df = spark.read\
.format("csv")\
.option("header","true")\
.option('sep', ';')\
.option("header","true")\
.option('sep', ';')\
.option('encoding','utf-8')\
.option('mode','FAILFAST')\
.option('ignoreLeadingWhiteSpace','true')\
.option('ignoreTrailingWhiteSpace','true')\
.option('encoding', 'ISO-8859-1')\
.option('encoding', 'latin1')\
.load(path)\
.withColumn("ANO", lit(2019)) # <----- ADICIONADO o ANO:integer
#display(df.limit(3))


#=====X====X====X====X====X====X====X
# __normalize_str(_str) function de padronização
def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())

# __normalize_str(_str) em todas as columns
for column in df.columns:
  df = df.withColumnRenamed(column, __normalize_str(column))

# replace de , to .  
from pyspark.sql.functions import *

for column in df.columns:
  df = df.withColumn(column , regexp_replace(column , ',', '.'))  
  
display(df.limit(3))

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Endereço do data lake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/KC2332_ME_RAIS_VINCULO_mapeamento_2020.csv'  # <----- O ARQUIVO ERA PARA SER 2019, NO ENTANTO ESQUECERAM DE COLOCAR NO DATA LAKE, LOGO FOI FEITO COM ARQUIVO 2020, E AS MUDANCAS FORAM CORRIGIDAS POSTERIORIOMENTE NO WITHCOLULMNRENAMED
path = var_adls_uri + de_para_path

#=====X====X====X====X====X====X====X
# importando o prm 
# prm em formato 'csv'
prm = spark.read\
.format("csv")\
.option("header","true")\
.option('sep', ';')\
.load(path)

prm_2019 = prm.toPandas()
prm_2019
#=====X====X====X====X====X====X====X

# COMMAND ----------

# Padronizando lista de cast()
lista_de_cast = ['string', 'string', 'integer', 'string', 'string']

lista = [cast.capitalize() for cast in lista_de_cast] 
print(lista)

lista_add_Type = [we +'Type()' for we in lista] 
print(lista_add_Type)

# COMMAND ----------

df2 = df.withColumn("age",col("age").cast(StringType())) \
    .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
    .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
df2.printSchema()

# COMMAND ----------

#=====X====X====X====X====X====X====X
df_prm_2019_SEM_N_A = prm_2019.loc[(
    (~((prm_2019['_c1'].isnull()) | (prm_2019['_c1'] == 'N/A')) & (prm_2019['Cliente:'] == 'RAIS_VINCULO')
)),['Cliente:','_c1','_c2','_c3','_c4','_c5']]

# Code when just there is only one column to represent as DataFrame
# df_prm_2019 = pd.DataFrame(df_prm_2019)
# df_prm_2019

len(df_prm_2019_SEM_N_A)
df_prm_2019_SEM_N_A.head(5)
#=====X====X====X====X====X====X====X

# COMMAND ----------

# DICT_ITEM
# dict(zip(KEYS, VALUES))
# Testing dict(zip(KEYS, VALUES))

KEYS = df_prm_2019_SEM_N_A['_c1']
VALUES = df_prm_2019_SEM_N_A['_c3']

dict_items_SEM_N_A = dict(zip(KEYS, VALUES))
dict_items_SEM_N_A
#dict_items_SEM_N_A.items()
# 60

#=====X====X=====X====X=====X====X=====X====X=====X====X
# Matching values

# COMMAND ----------

for columns in df.column:
  dw = df.withColumnRenamed(xx,w)
  zip

# COMMAND ----------

df2 = df.withColumn("age",col("age").cast(StringType())) \
    .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
    .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
df2.printSchema()

# COMMAND ----------

df.withColumn([c,col(c).cast(dict_cast.get(c, c)) for c in df.columns])

# COMMAND ----------


df = df.select([col(c).alias(dict_items_SEM_N_A.get(c, c)) for c in df.columns])


# COMMAND ----------

display(df)

# COMMAND ----------



[dict_items_SEM_N_A.get(c,c) for c in df.columns]
  dw = df.withColumnRenamed(xx,w)
dw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dados para derem match

# COMMAND ----------

# RUNNING RAIS RAW DATA
df.columns

# COMMAND ----------

# RUNNING PRM SEM N_A
dict_items_SEM_N_A.items()

import pandas as pd

df_df = pd.DataFrame(columns=[df.columns])
dt = df_df.rename(columns={k: v for k,v in dict_items_SEM_N_A.items() if k in df_df.columns})
# len(dt.columns) 61
#dt

dr = []
for each_values in list(dt.columns):
  a = list(each_values)
  #print(a)
  dr.append(a)
#print(dr)

newest = [str(i[0]) for i in dr]
#print(newest)

##############################
newest
#def replace_column_name(newest):
  #return newest



# COMMAND ----------

newest

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

df_replace_column_name = udf(lambda x: replace_column_name(x),StringType())
#df.withColumnRenamed(x,df_replace_column_name(newest))
#replace_column_name(newest)
for x,y in zip(df.columns,newest):
  dw = df.withColumnRenamed(x,y)
display(dw.limit(3))
  #rint(x,y)
# x = ('{x}'.format(x=x), '{y}'.format(y=y))
# print(x)
# dw = df.withColumnRenamed('{x}'.format(x=x), '{y}'.format(y=y))
# display(dw)

# COMMAND ----------

rename_dict = {
  'BAIRROS_SP':"BAIRROS_SPasdasdasda",
  'BAIRROS_FORTALEZA':"BAIRROS_SPasdasdasdaadsadasda"}

rename_dict.get(c) for c in df.columns]

# COMMAND ----------

x = ['BAIRROS_SP','BAIRROS_FORTALEZA']
y = ['BAIRROS_sdasdasdasdfffffffffffffasP','BAIRROS_sdasdasdasdffffffffffeeeeeeeeeeeeeeeeeeefffasP']

for q,w in zip(x,y):
  dw = df.withColumnRenamed(q,w)
  dw.display()

# COMMAND ----------



# COMMAND ----------

x = ['BAIRROS_SP','BAIRROS_FORTALEZA']
y = ['BAIRROS_sdasdasdasdfffffffffffffasP','BAP']

for i,w in enumerate(y):
  xx = x[i]
  dw = df.withColumnRenamed(xx,w)
dw.display()

# COMMAND ----------



for column in df.columns:
  print(column)


# COMMAND ----------


def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())
  
for column in df.columns:
  df = df.withColumnRenamed(column, __normalize_str(column))
  
df.display()

# COMMAND ----------

display(df)

# COMMAND ----------

for columns in df.columns:
  newest
  #print(columns)  
  df = df.withColumnRenamed(columns, newest)
df

# COMMAND ----------

# RUNNING RAIS RAW DATA

# COMMAND ----------

df = [[110652450], [5309154]]
newest = [i[0] for i in df]
print(newest)
>> [110652450,5309154]

# COMMAND ----------

NEW_NAMES = {k: v for k, v in dict_items_SEM_N_A.items() if k in LISTA_RAW_DATA}
NEW_NAMES

# COMMAND ----------

NEW_NAMES = {v:v for k, v in dict_items_SEM_N_A.items() if k not in LISTA_RAW_DATA}
NEW_NAMES

# COMMAND ----------

DF_RENAMING_RAW_DATA = df.withColumnRenamed(existingName, newNam)


# COMMAND ----------

A = [v if k in LISTA_RAW_DATA else v for k, v in dict_items_SEM_N_A.items()]
A

# COMMAND ----------



# COMMAND ----------

A = {v for k, v in dict_items_SEM_N_A.items() if k in LISTA_RAW_DATA if k not in LISTA_RAW_DATA}
A

# COMMAND ----------

columns={k: v for k, v in d.items() if k in df.columns}

# COMMAND ----------

dict_items_SEM_N_A = dict(zip(KEYS, VALUES))
dict_items_SEM_N_A

for columns in df.columns:
  dict_items_SEM_N_A = dict(zip(KEYS, VALUES))
  #print(columns)
  df = df.withColumnRenamed(columns, [v for k, v in dict_items_SEM_N_A.items() if k in df.columns])

# COMMAND ----------

# NAO DEU CERTO, TENTANDO DE OUTRA FORMAR

dict_items_SEM_N_A = dict(zip(KEYS, VALUES))
dict_items_SEM_N_A

for columns in df.columns:
  #print(columns)
  df = df.withColumnRenamed(columns,[k: v for k, v in dict_items_SEM_N_A.items() if k in df.columns])

# COMMAND ----------

A = [v if k in LISTA_RAW_DATA else v for k, v in dict_items_SEM_N_A.items()]
len(A)

# COMMAND ----------

for columns in df.columns:
  #print(columns)
  df = df.withColumnRenamed(columns,[v if k in LISTA_RAW_DATA else v for k, v in dict_items_SEM_N_A.items()])

# COMMAND ----------

def if_exist(DF_COLUMNS,DICT_KEYS_VALUES):
  DF_COLUMNS = DF_COLUMNS
  DICT_KEYS_VALUES = DICT_KEYS_VALUES
  z = [b if b in DF_COLUMNS else b for a,b in DICT_KEYS_VALUES ]
  return z

# COMMAND ----------

len(if_exist(LISTA_RAW_DATA,dict_items_SEM_N_A.items()))

# COMMAND ----------

if_exist(LISTA_RAW_DATA,dict_items_SEM_N_A.items())

# COMMAND ----------

LISTA_RAW_DATA
dict_items_SEM_N_A.items()

def if_exist(DF_COLUMNS,DICT_KEYS_VALUES):
  DF_COLUMNS = DF_COLUMNS
  DICT_KEYS_VALUES = DICT_KEYS_VALUES
  z = [b if b in DF_COLUMNS else b for a,b in DICT_KEYS_VALUES ]
  return z

for columns in df.columns:
  LISTA_RAW_DATA
  dict_items_SEM_N_A.items()
  #print(columns)
  df_TESTE = df.withColumnRenamed(columns,if_exist(LISTA_RAW_DATA,dict_items_SEM_N_A.items()))
  
  
  

# COMMAND ----------

#############################################################

# COMMAND ----------

for columns in df.columns:
  print(columns)

# COMMAND ----------

qw.columns

# COMMAND ----------

list(qw.columns)

# COMMAND ----------

list(list(qw.columns))

# COMMAND ----------

for qwer in list(qw.columns):
  print(list(qwer))

# COMMAND ----------

dict_items_SEM_N_A

# COMMAND ----------

import pandas as pd
LISTA_RAW_DATA
dfdf = pd.DataFrame(columns=[LISTA_RAW_DATA])
qw = dfdf.rename(columns={k: v for k,v in dict_items_SEM_N_A.items() if k in dfdf.columns})
len(qw.columns)

# COMMAND ----------

for columns in df.columns:
  #print(columns)
  df_TESTE = df.withColumnRenamed(columns,qw.columns)
  df_TESTE

# COMMAND ----------

dict_items_SEM_N_A.items()

# COMMAND ----------

type(dfdf)

# COMMAND ----------

er = []
for e in dfdf.columns:
  z = list(e)
  a = z
  
  er.append(a)
er

# COMMAND ----------

#     from pyspark.sql import DataFrame
#     
#     def transform(self, f):
#       return f(self)
#     
#     DataFrame.transform = transform


def renameColumn(df):
  rename_dict = {
    'FName':"FirstName",
    'LName':"LastName",
    'DOB':"BirthData"
  }
  return df.select([col(c).alias(rename_dict.get(c, c)) for c in df.columns])

df_renamed = spark.read.load('https').transform(renameColumns)

# COMMAND ----------



# COMMAND ----------

rename_dict = {
  'BAIRROS_SP':"BAIRROS_SPasdasdasda",
  'BAIRROS_FORTALEZA':"BAIRROS_SPasdasdasdaadsadasda"}

rename_dict.get(c) for c in df.columns]

# COMMAND ----------

rename_dict.get("BAIRROS_SPasdasdasda")

# COMMAND ----------

df

# COMMAND ----------

[rename_dict.get(c) for c in df.columns]

# COMMAND ----------

rename_dict.get(c, c)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2019

# COMMAND ----------

# Chamando das functions e as libraries

from unicodedata import normalize
import json
import re
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import datetime


#=====X====X====X====X====X====X====X
# Problemas de import

# from cni_connectors import adls_gen1_connector as adls_conn
# import crawler.functions as cf

#=====X====X====X====X====X====X====X

# COMMAND ----------

# MAGIC %md  
# MAGIC # Importando file PRM RAIS 2019 para a padronizção do RAIS Bruto

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Endereço do data lake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/KC2332_ME_RAIS_VINCULO_mapeamento_2020.csv'  # <----- O ARQUIVO ERA PARA SER 2019, NO ENTANTO ESQUECERAM DE COLOCAR NO DATA LAKE, LOGO FOI FEITO COM ARQUIVO 2020, E AS MUDANCAS FORAM CORRIGIDAS POSTERIORIOMENTE NO WITHCOLULMNRENAMED
path = var_adls_uri + de_para_path

#=====X====X====X====X====X====X====X
# importando o prm 
# prm em formato 'csv'
prm = spark.read\
.format("csv")\
.option("header","true")\
.option('sep', ';')\
.load(path)

#=====X====X====X====X====X====X====X
# Transformando para pandas e fazendo modificações
prm = prm.toPandas()
prm = prm.iloc[18:].reset_index(drop=True)

prm = prm.rename(columns={"Cliente:": "Tabela_Origem",
                                      "_c1": "Campo_Origem",
                                      "_c2":"Transformação",
                                      "_c3":"Campo_Destino",
                                      "_c4":"Tipo_tamanho",
                                      "_c5":"Descrição"})

prm = prm[["Tabela_Origem", "Campo_Origem", "Transformação", "Campo_Destino", "Tipo_tamanho", "Descrição"]]
prm['ANO'] = 2019 # integer
prm_COM_NA = prm[prm['Campo_Origem'] == 'N/A']
prm_COM_NA.head(3) #prm com values (valores N/A)

#=====X====X====X====X====X====X====X

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Obtendo lista do prm com valores NA e adicionado algumas variáveis
LIST_prm_COM_NA = prm_COM_NA['Campo_Destino'].tolist()

#=====X====X====X====X====X====X====X
# Lista obtida pelo LIST_prm_COM_NA
# Adicionando valores ["N/A"]
d = {'FL_IND_VINCULO_ALVARA': ["N/A"],
 'CD_TIPO_SALARIO': ["N/A"],
 'CD_CBO94': ["N/A"],
 'FL_IND_ESTAB_PARTICIPA_PAT': ["N/A"],
 'DT_DIA_MES_ANO_DATA_ADMISSAO': ["N/A"],
 'VL_REMUN_ULTIMA_ANO_NOM': ["N/A"],
 'VL_REMUN_CONTRATUAL_NOM': ["N/A"],
 'ID_PIS': ["N/A"],
 'DT_DIA_MES_ANO_DATA_NASCIMENTO': ["N/A"],
 'ID_CTPS': ["N/A"],
 'ID_CPF': ["N/A"],
 'ID_CEI_VINCULADO': ["N/A"],
 'ID_CNPJ_CEI': ["N/A"],
 'ID_CNPJ_RAIZ': ["N/A"],
 'ID_NOME_TRABALHADOR': ["N/A"],
 'DT_DIA_MES_ANO_DIA_DESLIGAMENTO': ["N/A"],
 'NR_DIA_INI_AF1': ["N/A"],
 'NR_MES_INI_AF1': ["N/A"],
 'NR_DIA_FIM_AF1': ["N/A"],
 'NR_MES_FIM_AF1': ["N/A"],
 'NR_DIA_INI_AF2': ["N/A"],
 'NR_MES_INI_AF2': ["N/A"],
 'NR_DIA_FIM_AF2': ["N/A"],
 'NR_MES_FIM_AF2': ["N/A"],
 'NR_DIA_INI_AF3': ["N/A"],
 'NR_MES_INI_AF3': ["N/A"],
 'NR_DIA_FIM_AF3': ["N/A"],
 'NR_MES_FIM_AF3': ["N/A"],
 'ID_CEPAO_ESTAB': ["N/A"],
 'ID_RAZAO_SOCIAL': ["N/A"],
 'FL_SINDICAL': ["N/A"],
 'VL_ANO_CHEGADA_BRASIL2': ["N/A"],
 'CD_CNAE20_DIVISAO': ["N/A"],
 'CD_CBO4': ["N/A"], 
  'ANO':2019} # <----- ANO ADICIONADO

#=====X====X====X====X====X====X====X

import pandas as pd
DATA_PRM_COM_NA = pd.DataFrame(data=d)
#DATA_PRM_COM_NA.head(3)

# DataFrame to SparkFrame
DATA_PRM_COM_NA = spark.createDataFrame(DATA_PRM_COM_NA)
display(DATA_PRM_COM_NA)

# COMMAND ----------

# MAGIC %md
# MAGIC # Importando RAIS 2019 BRUTO, padronizando-a

# COMMAND ----------

#=====X====X====X====X====X====X====X
# importando functions e o file bruto da RAIS 2020
from pyspark.sql.functions import when,col,lit

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/2019'

path = var_adls_uri + de_para_path

df = spark.read\
.format("csv")\
.option("header","true")\
.option('sep', ';')\
.option("header","true")\
.option('sep', ';')\
.option('encoding','utf-8')\
.option('mode','FAILFAST')\
.option('ignoreLeadingWhiteSpace','true')\
.option('ignoreTrailingWhiteSpace','true')\
.option('encoding', 'ISO-8859-1')\
.option('encoding', 'latin1')\
.load(path)\
.withColumn("ANO", lit(2019)) # <----- ADICIONADO o ANO:integer
#display(df.limit(3))


#=====X====X====X====X====X====X====X
# __normalize_str(_str) function de padronização
def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())

# __normalize_str(_str) em todas as columns
for column in df.columns:
  df = df.withColumnRenamed(column, __normalize_str(column))

# replace de , to .  
from pyspark.sql.functions import *

for column in df.columns:
  df = df.withColumn(column , regexp_replace(column , ',', '.'))  
  
display(df.limit(3))


# COMMAND ----------

#=====X====X====X====X====X====X====X
# Há um método melhor, modificações futuras
# Renomeando colunas de acordo com o PRM

df = df.withColumnRenamed("MUNICIPIO", "CD_MUNICIPIO")\
.withColumnRenamed("CNAE_95_CLASSE", "CD_CNAE10_CLASSE")\
.withColumnRenamed("VINCULO_ATIVO_31_12", "FL_VINCULO_ATIVO_3112")\
.withColumnRenamed("TIPO_VINCULO", "CD_TIPO_VINCULO")\
.withColumnRenamed("MOTIVO_DESLIGAMENTO", "CD_MOTIVO_DESLIGAMENTO")\
.withColumnRenamed("MES_DESLIGAMENTO", "CD_MES_DESLIGAMENTO")\
.withColumnRenamed("TIPO_ADMISSAO", "CD_TIPO_ADMISSAO")\
.withColumnRenamed("ESCOLARIDADE_APOS_2005", "CD_GRAU_INSTRUCAO")\
.withColumnRenamed("SEXO_TRABALHADOR", "CD_SEXO")\
.withColumnRenamed("NACIONALIDADE", "CD_NACIONALIDADE")\
.withColumnRenamed("RACA_COR", "CD_RACA_COR")\
.withColumnRenamed("IND_PORTADOR_DEFIC", "FL_IND_PORTADOR_DEFIC")\
.withColumnRenamed("TAMANHO_ESTABELECIMENTO", "CD_TAMANHO_ESTABELECIMENTO")\
.withColumnRenamed("NATUREZA_JURIDICA", "CD_NATUREZA_JURIDICA")\
.withColumnRenamed("IND_CEI_VINCULADO", "FL_IND_CEI_VINCULADO")\
.withColumnRenamed("TIPO_ESTAB41", "CD_TIPO_ESTAB")\
.withColumnRenamed("IND_SIMPLES", "FL_IND_SIMPLES")\
.withColumnRenamed("VL_REMUN_MEDIA_SM", "VL_REMUN_MEDIA_SM")\
.withColumnRenamed("VL_REMUN_MEDIA_NOM", "VL_REMUN_MEDIA_NOM")\
.withColumnRenamed("VL_REMUN_DEZEMBRO_SM", "VL_REMUN_DEZEMBRO_SM")\
.withColumnRenamed("VL_REMUN_DEZEMBRO_NOM", "VL_REMUN_DEZEMBRO_NOM")\
.withColumnRenamed("TEMPO_EMPREGO", "NR_MES_TEMPO_EMPREGO")\
.withColumnRenamed("QTD_HORA_CONTR", "QT_HORA_CONTRAT")\
.withColumnRenamed("TIPO_ESTAB42", "CD_TIPO_ESTAB_ID")\
.withColumnRenamed("CBO_OCUPACAO_2002", "CD_CBO")\
.withColumnRenamed("CNAE_2_0_CLASSE", "CD_CNAE20_CLASSE")\
.withColumnRenamed("CNAE_2_0_SUBCLASSE", "CD_CNAE20_SUBCLASSE")\
.withColumnRenamed("TIPO_DEFIC", "CD_TIPO_DEFIC")\
.withColumnRenamed("CAUSA_AFASTAMENTO_1", "CD_CAUSA_AFASTAMENTO1")\
.withColumnRenamed("CAUSA_AFASTAMENTO_2", "CD_CAUSA_AFASTAMENTO2")\
.withColumnRenamed("CAUSA_AFASTAMENTO_3", "CD_CAUSA_AFASTAMENTO3")\
.withColumnRenamed("QTD_DIAS_AFASTAMENTO", "VL_DIAS_AFASTAMENTO")\
.withColumnRenamed("IDADE", "VL_IDADE")\
.withColumnRenamed("IBGE_SUBSETOR", "CD_IBGE_SUBSETOR")\
.withColumnRenamed("ANO_CHEGADA_BRASIL", "VL_ANO_CHEGADA_BRASIL")\
.withColumnRenamed("MUN_TRAB", "CD_MUNICIPIO_TRAB")\
.withColumnRenamed("VL_REM_JANEIRO_CC", "VL_REMUN_JANEIRO_NOM")

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Há um método melhor, modificações futuras
# Renomeando colunas de acordo com o PRM

df = df.withColumnRenamed("VL_REM_FEVEREIRO_CC", "VL_REMUN_FEVEREIRO_NOM")\
.withColumnRenamed("VL_REM_MARCO_CC", "VL_REMUN_MARCO_NOM")\
.withColumnRenamed("VL_REM_ABRIL_CC", "VL_REMUN_ABRIL_NOM")\
.withColumnRenamed("VL_REM_MAIO_CC", "VL_REMUN_MAIO_NOM")\
.withColumnRenamed("VL_REM_JUNHO_CC", "VL_REMUN_JUNHO_NOM")\
.withColumnRenamed("VL_REM_JULHO_CC", "VL_REMUN_JULHO_NOM")\
.withColumnRenamed("VL_REM_AGOSTO_CC", "VL_REMUN_AGOSTO_NOM")\
.withColumnRenamed("VL_REM_SETEMBRO_CC", "VL_REMUN_SETEMBRO_NOM")\
.withColumnRenamed("VL_REM_OUTUBRO_CC", "VL_REMUN_OUTUBRO_NOM")\
.withColumnRenamed("VL_REM_NOVEMBRO_CC", "VL_REMUN_NOVEMBRO_NOM")\
.withColumnRenamed("IND_TRAB_INTERMITENTE", "FL_IND_TRAB_INTERMITENTE")\
.withColumnRenamed("IND_TRAB_PARCIAL", "FL_IND_TRAB_PARCIAL")\
.withColumnRenamed("BAIRROS_FORTALEZA", "CD_BAIRROS_FORTALEZA")\
.withColumnRenamed("BAIRROS_RJ", "CD_BAIRROS_RJ")\
.withColumnRenamed("BAIRROS_SP", "CD_BAIRROS_SP")\
.withColumnRenamed("DISTRITOS_SP", "CD_DISTRITOS_SP")\
.withColumnRenamed("FAIXA_ETARIA", "CD_FAIXA_ETARIA")\
.withColumnRenamed("FAIXA_HORA_CONTRAT", "CD_FAIXA_HORA_CONTRAT")\
.withColumnRenamed("FAIXA_REMUN_DEZEM_SM", "CD_FAIXA_REMUN_DEZEM_SM")\
.withColumnRenamed("FAIXA_REMUN_MEDIA_SM", "CD_FAIXA_REMUN_MEDIA_SM")\
.withColumnRenamed("FAIXA_TEMPO_EMPREGO", "CD_FAIXA_TEMPO_EMPREGO")\
.withColumnRenamed("REGIOES_ADM_DF", "CD_REGIOES_ADM_DF")\
.withColumnRenamed("MES_ADMISSAO", "DT_MES_ADMISSAO")

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Criando uma nova coluna a partir de CD_MUNICIPIO
# Nova colunas CD_UF com os 2 primeiros digítos de CD_MUNICIPIO
df = df.withColumn('CD_UF', f.substring(f.col('CD_MUNICIPIO'),1,2))

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Join nos arquivos df e DATA_PRM_COM_NA

# COMMAND ----------

RAIS_2019 = DATA_PRM_COM_NA.join(df, 'ANO', how='right')

display(RAIS_2019)

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Padronizando Type()
from pyspark.sql.types import IntegerType, DoubleType
__RAIS_2019__ = RAIS_2019.withColumn("FL_VINCULO_ATIVO_3112",col("FL_VINCULO_ATIVO_3112").cast(IntegerType()))\
.withColumn("FL_IND_VINCULO_ALVARA" ,col("FL_IND_VINCULO_ALVARA").cast(IntegerType()))\
.withColumn("FL_IND_PORTADOR_DEFIC" ,col("FL_IND_PORTADOR_DEFIC").cast(IntegerType()))\
.withColumn("FL_IND_CEI_VINCULADO" ,col("FL_IND_CEI_VINCULADO").cast(IntegerType()))\
.withColumn("FL_IND_ESTAB_PARTICIPA_PAT" ,col("FL_IND_ESTAB_PARTICIPA_PAT").cast(IntegerType()))\
.withColumn("FL_IND_SIMPLES" ,col("FL_IND_SIMPLES").cast(IntegerType()))\
.withColumn("VL_REMUN_MEDIA_SM" ,col("VL_REMUN_MEDIA_SM").cast(DoubleType()))\
.withColumn("VL_REMUN_MEDIA_NOM" ,col("VL_REMUN_MEDIA_NOM").cast(DoubleType()))\
.withColumn("VL_REMUN_DEZEMBRO_SM" ,col("VL_REMUN_DEZEMBRO_SM").cast(DoubleType()))\
.withColumn("VL_REMUN_DEZEMBRO_NOM" ,col("VL_REMUN_DEZEMBRO_NOM").cast(DoubleType()))\
.withColumn("NR_MES_TEMPO_EMPREGO" ,col("NR_MES_TEMPO_EMPREGO" ).cast(DoubleType()))\
.withColumn("QT_HORA_CONTRAT",col("QT_HORA_CONTRAT").cast(IntegerType()))\
.withColumn("VL_REMUN_ULTIMA_ANO_NOM" ,col("VL_REMUN_ULTIMA_ANO_NOM").cast(DoubleType()))\
.withColumn("VL_REMUN_CONTRATUAL_NOM" ,col("VL_REMUN_CONTRATUAL_NOM").cast(DoubleType()))\
.withColumn("NR_DIA_INI_AF1",col("NR_DIA_INI_AF1").cast(IntegerType()))\
.withColumn("NR_MES_INI_AF1",col("NR_MES_INI_AF1").cast(IntegerType()))\
.withColumn("NR_DIA_FIM_AF1",col("NR_DIA_FIM_AF1").cast(IntegerType()))\
.withColumn("NR_MES_FIM_AF1",col("NR_MES_FIM_AF1").cast(IntegerType()))\
.withColumn("NR_DIA_INI_AF2",col("NR_DIA_INI_AF2").cast(IntegerType()))\
.withColumn("NR_MES_INI_AF2",col("NR_MES_INI_AF2").cast(IntegerType()))\
.withColumn("NR_DIA_FIM_AF2",col("NR_DIA_FIM_AF2").cast(IntegerType()))\
.withColumn("NR_MES_FIM_AF2",col("NR_MES_FIM_AF2").cast(IntegerType()))\
.withColumn("NR_DIA_INI_AF3",col("NR_DIA_INI_AF3").cast(IntegerType()))\
.withColumn("NR_MES_INI_AF3",col("NR_MES_INI_AF3").cast(IntegerType()))\
.withColumn("NR_DIA_FIM_AF3",col("NR_DIA_FIM_AF3").cast(IntegerType()))\
.withColumn("NR_MES_FIM_AF3",col("NR_MES_FIM_AF3").cast(IntegerType()))\
.withColumn("VL_DIAS_AFASTAMENTO",col("VL_DIAS_AFASTAMENTO").cast(IntegerType()))\
.withColumn("VL_IDADE",col("VL_IDADE").cast(IntegerType()))\
.withColumn("VL_ANO_CHEGADA_BRASIL",col("VL_ANO_CHEGADA_BRASIL").cast(IntegerType()))\
.withColumn("VL_REMUN_JANEIRO_NOM" ,col("VL_REMUN_JANEIRO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_FEVEREIRO_NOM" ,col("VL_REMUN_FEVEREIRO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_MARCO_NOM" ,col("VL_REMUN_MARCO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_ABRIL_NOM" ,col("VL_REMUN_ABRIL_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_MAIO_NOM" ,col("VL_REMUN_MAIO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_JUNHO_NOM" ,col("VL_REMUN_JUNHO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_JULHO_NOM" ,col("VL_REMUN_JULHO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_AGOSTO_NOM" ,col("VL_REMUN_AGOSTO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_SETEMBRO_NOM" ,col("VL_REMUN_SETEMBRO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_OUTUBRO_NOM" ,col("VL_REMUN_OUTUBRO_NOM" ).cast(DoubleType()))\
.withColumn("VL_REMUN_NOVEMBRO_NOM" ,col("VL_REMUN_NOVEMBRO_NOM" ).cast(DoubleType()))\
.withColumn("FL_IND_TRAB_INTERMITENTE",col("FL_IND_TRAB_INTERMITENTE").cast(IntegerType()))\
.withColumn("FL_IND_TRAB_PARCIAL",col("FL_IND_TRAB_PARCIAL").cast(IntegerType()))\
.withColumn("FL_SINDICAL",col("FL_SINDICAL").cast(IntegerType()))\
.withColumn("VL_ANO_CHEGADA_BRASIL2",col("VL_ANO_CHEGADA_BRASIL2").cast(IntegerType()))

display(__RAIS_2019__)

# COMMAND ----------

__RAIS_2019__.repartition(40).write.partitionBy('ANO').parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/raw/', mode='overwrite')

# COMMAND ----------

