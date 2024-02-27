# Databricks notebook source
# from cni_connectors import adls_gen1_connector as adls_conn

# import crawler.functions as cf

from unicodedata import normalize
import json
import re

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import datetime

# COMMAND ----------



# COMMAND ----------

# import crawler.functions as cf

# COMMAND ----------

# MAGIC %md 
# MAGIC -----x-----x-----x-----x-----x-----x-----
# MAGIC       PRM
# MAGIC -----x-----x-----x-----x-----x-----x-----

# COMMAND ----------

# MAGIC %md
# MAGIC - Importando arquivo prm (declarando como "prm");
# MAGIC - Separando em prm do tipo com N/A e do tipo sem N/A, para fazer o coupling futuramente

# COMMAND ----------

### var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
### de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/KC2332_ME_RAIS_VINCULO_mapeamento_2019.csv'
### #uds/uniepro/me/rais_vinc_19_20/uld/KC2332_ME_RAIS_VINCULO_mapeamento_2019.csv
### 
### path = var_adls_uri + de_para_path
### # print(path)
### #    abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/prm/KC2332_ME_RAIS_VINCULO_mapeamento_raw.xlsx
### 
### prm = spark.read\
### .format("csv")\
### .option("header","true")\
### .option('sep', ';')\
### .load(path)
### 
### #display(prm)
### prm = prm.toPandas()
### prm = prm.iloc[18:113].reset_index(drop=True).drop(['_c0'], axis=1)  #df_pandas = df_pandas.reset_index(drop=True)
### prm
### 
### prm = prm.rename(columns={"Cliente:": "Tabela_Origem",
###                                       "_c2": "Campo_Origem",
###                                       "_c3":"Transformação",
###                                       "_c4":"Campo_Destino",
###                                       "_c5":"Tipo_tamanho",
###                                       "_c6":"Descrição"})
### 
### prm = prm[["Tabela Origem", "Campo Origem", "Transformação", "Campo Destino", "Tipo (tamanho)", "Descrição"]]
### prm['ANO'] = 2020
### 
### prm = spark.createDataFrame(prm)
### display(prm)

### #df_pandas_COM_NA = df_pandas.iloc[60:]
### #df_pandas_COM_NA
### com_NA_prm = prm.iloc[60:]
### 
### #df_pandas_SEM_NA = df_pandas.iloc[0:60]
### #df_pandas_SEM_NA
### prm = prm.iloc[0:60]

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/KC2332_ME_RAIS_VINCULO_mapeamento_2020.csv'
#uds/uniepro/me/rais_vinc_19_20/uld/KC2332_ME_RAIS_VINCULO_mapeamento_2019.csv

path = var_adls_uri + de_para_path
# print(path)
#    abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/prm/KC2332_ME_RAIS_VINCULO_mapeamento_raw.xlsx

prm = spark.read\
.format("csv")\
.option("header","true")\
.option('sep', ';')\
.load(path)
#display(prm)

prm = prm.toPandas()
 #prm = prm.iloc[18:113].reset_index(drop=True).drop(['_c0'], axis=1)  #df_pandas = df_pandas.reset_index(drop=True)
prm = prm.iloc[18:].reset_index(drop=True)

prm = prm.rename(columns={"Cliente:": "Tabela_Origem",
                                      "_c1": "Campo_Origem",
                                      "_c2":"Transformação",
                                      "_c3":"Campo_Destino",
                                      "_c4":"Tipo_tamanho",
                                      "_c5":"Descrição"})

prm = prm[["Tabela_Origem", "Campo_Origem", "Transformação", "Campo_Destino", "Tipo_tamanho", "Descrição"]]
prm['ANO'] = 2020 # integer
prm_COM_NA = prm[prm['Campo_Origem'] == 'N/A']
prm_COM_NA

#prm = spark.createDataFrame(prm)
#display(prm)

#prm_COM_NA = prm[prm['Campo_Origem'].str.contains('N/A')]

#prm_SEM_NA = prm[~prm['Campo_Origem'].str.contains('N/A')]
#pm_SEM_NA
#prm_SEM_NA = spark.createDataFrame(prm_SEM_NA)
#display(prm_SEM_NA)

# COMMAND ----------

lista_DE_VALORES_NA = prm_COM_NA['Campo_Destino'].tolist()
#lista_DE_VALORES_NA 
#import pandas as pd
#colunas_COM_NA =  pd.DataFrame(columns=lista_DE_VALORES_NA)
#colunas_COM_NA
lista_DE_VALORES_NA 

# COMMAND ----------

import pandas as pd

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
  'ANO':2020} 
  
df_NA = pd.DataFrame(data=d)
df_NA.columns

# COMMAND ----------

df_valores_tabela_NA = spark.createDataFrame(df_NA)
display(df_valores_tabela_NA)

# COMMAND ----------

#spark.createDataFrame(pd.DataFrame({'a': [], 'b': []}), 'c INT').dtypes

# COMMAND ----------

#import pyspark.sql.functions as f
#my_list = prm_SEM_NA.select(f.collect_list('Campo_Origem')).first()[0]
#my_list

# COMMAND ----------

# MAGIC %md 
# MAGIC x-----x-----x----x RAIS x-----x-----x----x
# MAGIC
# MAGIC - importando arquivo da RAIS(bruto);
# MAGIC - 'Padronizando' com a function def __normalize_str(_str);

# COMMAND ----------

from pyspark.sql.functions import when,col,lit

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/2020'

path = var_adls_uri + de_para_path
# print(path)
# abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/prm/KC2332_ME_RAIS_VINCULO_mapeamento_raw.xlsx

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


df = df.withColumn("ANO", lit(2020)) #ANO:integer
#display(df.sample(0.0008))
#df = df.toPandas()
#df.headers.uppercase()
#df
#df = df.select('*', lit(0.25).alias("Bonus Percent"))
#df = df.withColumn("Bonus Percent", lit(0.25))
#df.show()
#nome_que_serao_subst = dt.columns
#print(nome_que_serao_subst)
#len(nome_que_serao_subst)


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
#df = df.toPandas()

# COMMAND ----------

#################################################

df.columns


#######################################################

# COMMAND ----------

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
.withColumnRenamed("VL_REM_JANEIRO_SC", "VL_REMUN_JANEIRO_NOM")\
.withColumnRenamed("VL_REM_FEVEREIRO_SC", "VL_REMUN_FEVEREIRO_NOM")\
.withColumnRenamed("VL_REM_MARCO_SC", "VL_REMUN_MARCO_NOM")\
.withColumnRenamed("VL_REM_ABRIL_SC", "VL_REMUN_ABRIL_NOM")\
.withColumnRenamed("VL_REM_MAIO_SC", "VL_REMUN_MAIO_NOM")\
.withColumnRenamed("VL_REM_JUNHO_SC", "VL_REMUN_JUNHO_NOM")\
.withColumnRenamed("VL_REM_JULHO_SC", "VL_REMUN_JULHO_NOM")\
.withColumnRenamed("VL_REM_AGOSTO_SC", "VL_REMUN_AGOSTO_NOM")\
.withColumnRenamed("VL_REM_SETEMBRO_SC", "VL_REMUN_SETEMBRO_NOM")\
.withColumnRenamed("VL_REM_OUTUBRO_SC", "VL_REMUN_OUTUBRO_NOM")\
.withColumnRenamed("VL_REM_NOVEMBRO_SC", "VL_REMUN_NOVEMBRO_NOM")\
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

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# df = df.toPandas()
#org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 85 tasks (4.0 GiB) is bigger than spark.driver.maxResultSize 4.0 GiB.

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

display(df_valores_tabela_NA)

# COMMAND ----------

display(df)

# COMMAND ----------

RAIS_2020 = df_valores_tabela_NA.join(df, 'ANO', how='right')

# COMMAND ----------

display(RAIS_2020)

# COMMAND ----------

RAIS_2020 = RAIS_2020.withColumn('CD_UF', f.substring(f.col('CD_MUNICIPIO'),1,2))

# COMMAND ----------

####  df.repartition(40).write.partitionBy('ANO').parquet(path=var_sink, mode='overwrite')

# COMMAND ----------

# result = df.union(df_valores_tabela_NA)
# AnalysisException: Union can only be performed on tables with the same number of columns, but the first table has 61 columns and the second table has 34 columns;;

# COMMAND ----------

lista_RAIS = ['CD_MUNICIPIO','CD_CNAE10_CLASSE','FL_VINCULO_ATIVO_3112','CD_TIPO_VINCULO','CD_MOTIVO_DESLIGAMENTO','CD_MES_DESLIGAMENTO','FL_IND_VINCULO_ALVARA','CD_TIPO_ADMISSAO','CD_TIPO_SALARIO','CD_CBO94','CD_GRAU_INSTRUCAO','CD_SEXO','CD_NACIONALIDADE','CD_RACA_COR','FL_IND_PORTADOR_DEFIC','CD_TAMANHO_ESTABELECIMENTO','CD_NATUREZA_JURIDICA','FL_IND_CEI_VINCULADO','CD_TIPO_ESTAB','FL_IND_ESTAB_PARTICIPA_PAT','FL_IND_SIMPLES','DT_DIA_MES_ANO_DATA_ADMISSAO','VL_REMUN_MEDIA_SM','VL_REMUN_MEDIA_NOM','VL_REMUN_DEZEMBRO_SM','VL_REMUN_DEZEMBRO_NOM','NR_MES_TEMPO_EMPREGO','QT_HORA_CONTRAT','VL_REMUN_ULTIMA_ANO_NOM','VL_REMUN_CONTRATUAL_NOM','ID_PIS','DT_DIA_MES_ANO_DATA_NASCIMENTO','ID_CTPS','ID_CPF','ID_CEI_VINCULADO','ID_CNPJ_CEI','ID_CNPJ_RAIZ','CD_TIPO_ESTAB_ID','ID_NOME_TRABALHADOR','DT_DIA_MES_ANO_DIA_DESLIGAMENTO','CD_CBO','CD_CNAE20_CLASSE','CD_CNAE20_SUBCLASSE','CD_TIPO_DEFIC','CD_CAUSA_AFASTAMENTO1','NR_DIA_INI_AF1','NR_MES_INI_AF1','NR_DIA_FIM_AF1','NR_MES_FIM_AF1','CD_CAUSA_AFASTAMENTO2','NR_DIA_INI_AF2','NR_MES_INI_AF2','NR_DIA_FIM_AF2','NR_MES_FIM_AF2','CD_CAUSA_AFASTAMENTO3','NR_DIA_INI_AF3','NR_MES_INI_AF3','NR_DIA_FIM_AF3','NR_MES_FIM_AF3','VL_DIAS_AFASTAMENTO','VL_IDADE','CD_IBGE_SUBSETOR','VL_ANO_CHEGADA_BRASIL','ID_CEPAO_ESTAB','CD_MUNICIPIO_TRAB','ID_RAZAO_SOCIAL','VL_REMUN_JANEIRO_NOM','VL_REMUN_FEVEREIRO_NOM','VL_REMUN_MARCO_NOM','VL_REMUN_ABRIL_NOM','VL_REMUN_MAIO_NOM','VL_REMUN_JUNHO_NOM','VL_REMUN_JULHO_NOM','VL_REMUN_AGOSTO_NOM','VL_REMUN_SETEMBRO_NOM','VL_REMUN_OUTUBRO_NOM','VL_REMUN_NOVEMBRO_NOM','FL_IND_TRAB_INTERMITENTE','FL_IND_TRAB_PARCIAL','FL_SINDICAL','VL_ANO_CHEGADA_BRASIL2','CD_CNAE20_DIVISAO','CD_UF','CD_CBO4','CD_BAIRROS_FORTALEZA','CD_BAIRROS_RJ','CD_BAIRROS_SP','CD_DISTRITOS_SP','CD_FAIXA_ETARIA','CD_FAIXA_HORA_CONTRAT','CD_FAIXA_REMUN_DEZEM_SM','CD_FAIXA_REMUN_MEDIA_SM','CD_FAIXA_TEMPO_EMPREGO','CD_REGIOES_ADM_DF','DT_MES_ADMISSAO']

# COMMAND ----------

from pyspark.sql.functions import lit, col

# COMMAND ----------

R = RAIS_2020.select(*lista_RAIS)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

qwe = RAIS_2020.withColumn("CD_MUNICIPIO",col("CD_MUNICIPIO").cast(StringType()))\
.withColumn("CD_CNAE10_CLASSE",col("CD_CNAE10_CLASSE").cast(StringType()))\
.withColumn("FL_VINCULO_ATIVO_3112",col("FL_VINCULO_ATIVO_3112").cast(IntegerType()))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

RAIS_2020 = RAIS_2020.withColumn("FL_VINCULO_ATIVO_3112",col("FL_VINCULO_ATIVO_3112").cast(IntegerType()))\
.withColumn("FL_IND_VINCULO_ALVARA" ,col("FL_IND_VINCULO_ALVARA").cast(IntegerType()))\
.withColumn("FL_IND_PORTADOR_DEFIC" ,col("FL_IND_PORTADOR_DEFIC").cast(IntegerType()))\
.withColumn("FL_IND_CEI_VINCULADO" ,col("FL_IND_CEI_VINCULADO").cast(IntegerType()))\
.withColumn("FL_IND_ESTAB_PARTICIPA_PAT" ,col("FL_IND_ESTAB_PARTICIPA_PAT").cast(IntegerType()))\
.withColumn("FL_IND_SIMPLES" ,col("FL_IND_SIMPLES").cast(IntegerType()))\
.withColumn("VL_REMUN_MEDIA_SM" ,col("VL_REMUN_MEDIA_SM").cast(DoubleType()))\
.withColumn("VL_REMUN_MEDIA_NOM" ,col("VL_REMUN_MEDIA_NOM").cast(DoubleType()))\
.withColumn("VL_REMUN_DEZEMBRO_SM" ,col("VL_REMUN_DEZEMBRO_SM").cast(DoubleType()))\
.withColumn("VL_REMUN_DEZEMBRO_NOM" ,col("VL_REMUN_DEZEMBRO_NOM").cast(DoubleType()))\
.withColumn("NR_MES_TEMPO_EMPREGO" ,col("NR_MES_TEMPO_EMPREGO" ).cast(DoubleType()))

# COMMAND ----------

RAIS_2020 = RAIS_2020.withColumn("QT_HORA_CONTRAT",col("QT_HORA_CONTRAT").cast(IntegerType()))\
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
.withColumn("VL_IDADE",col("VL_IDADE").cast(IntegerType()))

# COMMAND ----------

RAIS_2020 = RAIS_2020.withColumn("VL_ANO_CHEGADA_BRASIL",col("VL_ANO_CHEGADA_BRASIL").cast(IntegerType()))\
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

# COMMAND ----------

RAIS_2020_ = RAIS_2020.withColumn("FL_VINCULO_ATIVO_3112",col("FL_VINCULO_ATIVO_3112").cast(IntegerType()))\
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


# COMMAND ----------

.cast(StringType()))\
.withColumn("CD_MUNICIPIO",col("CD_MUNICIPIO").cast(StringType()))\
.withColumn("CD_CNAE10_CLASSE",col("CD_CNAE10_CLASSE").cast(StringType()))\
.withColumn("FL_VINCULO_ATIVO_3112",col("FL_VINCULO_ATIVO_3112").cast(IntergerType()))\


.withColumn("CD_TIPO_VINCULO:StringType()
.withColumn("CD_MOTIVO_DESLIGAMENTO:StringType()
.withColumn("CD_MES_DESLIGAMENTO:StringType()
.withColumn("FL_IND_VINCULO_ALVARA: IntegerType()
.withColumn("CD_TIPO_ADMISSAO:StringType()
.withColumn("CD_TIPO_SALARIO:StringType()
.withColumn("CD_CBO94:StringType()
.withColumn("CD_GRAU_INSTRUCAO:StringType()
.withColumn("CD_SEXO:StringType()
.withColumn("CD_NACIONALIDADE:StringType()
.withColumn("CD_RACA_COR:StringType()
.withColumn("FL_IND_PORTADOR_DEFIC: IntegerType()
.withColumn("CD_TAMANHO_ESTABELECIMENTO:StringType()
.withColumn("CD_NATUREZA_JURIDICA:StringType()
.withColumn("FL_IND_CEI_VINCULADO: IntegerType()
.withColumn("CD_TIPO_ESTAB:StringType()
.withColumn("FL_IND_ESTAB_PARTICIPA_PAT:IntegerType()
.withColumn("FL_IND_SIMPLES:IntegerType()
.withColumn("DT_DIA_MES_ANO_DATA_ADMISSAO:sStringType()
.withColumn("VL_REMUN_MEDIA_SM: DoubleType()
.withColumn("VL_REMUN_MEDIA_NOM: DoubleType()
.withColumn("VL_REMUN_DEZEMBRO_SM: DoubleType()
.withColumn("VL_REMUN_DEZEMBRO_NOM: DoubleType()
.withColumn("NR_MES_TEMPO_EMPREGO: DoubleType()
.withColumn("QT_HORA_CONTRAT: IntegerType()
.withColumn("VL_REMUN_ULTIMA_ANO_NOM: DoubleType()
.withColumn("VL_REMUN_CONTRATUAL_NOM: DoubleType()
.withColumn("ID_PIS:StringType()
.withColumn("DT_DIA_MES_ANO_DATA_NASCIMENTO:StringType()
.withColumn("ID_CTPS:StringType()
.withColumn("ID_CPF:StringType()
.withColumn("ID_CEI_VINCULADO:StringType()
.withColumn("ID_CNPJ_CEI:StringType()
.withColumn("ID_CNPJ_RAIZ:StringType()
.withColumn("CD_TIPO_ESTAB_ID:StringType()
.withColumn("ID_NOME_TRABALHADOR:StringType()
.withColumn("DT_DIA_MES_ANO_DIA_DESLIGAMENTO:StringType()
.withColumn("CD_CBO:StringType()
.withColumn("CD_CNAE20_CLASSE:StringType()
.withColumn("CD_CNAE20_SUBCLASSE:StringType()
.withColumn("CD_TIPO_DEFIC:StringType()
.withColumn("CD_CAUSA_AFASTAMENTO1:StringType()
.withColumn("NR_DIA_INI_AF1:IntegerType()
.withColumn("NR_MES_INI_AF1:IntegerType()
.withColumn("NR_DIA_FIM_AF1:IntegerType()
.withColumn("NR_MES_FIM_AF1:IntegerType()
.withColumn("CD_CAUSA_AFASTAMENTO2: StringType()
.withColumn("NR_DIA_INI_AF2:IntegerType()
.withColumn("NR_MES_INI_AF2:IntegerType()
.withColumn("NR_DIA_FIM_AF2:IntegerType()
.withColumn("NR_MES_FIM_AF2:IntegerType()
.withColumn("CD_CAUSA_AFASTAMENTO3:StringType()
.withColumn("NR_DIA_INI_AF3:IntegerType()
.withColumn("NR_MES_INI_AF3:IntegerType()
.withColumn("NR_DIA_FIM_AF3:IntegerType()
.withColumn("NR_MES_FIM_AF3:IntegerType()
.withColumn("VL_DIAS_AFASTAMENTO:IntegerType()
.withColumn("VL_IDADE:IntegerType()
.withColumn("CD_IBGE_SUBSETOR:StringType()
.withColumn("VL_ANO_CHEGADA_BRASIL:IntegerType()
.withColumn("ID_CEPAO_ESTAB:StringType()
.withColumn("CD_MUNICIPIO_TRAB:StringType()
.withColumn("ID_RAZAO_SOCIAL:StringType()
.withColumn("VL_REMUN_JANEIRO_NOM:DoubleType()
.withColumn("VL_REMUN_FEVEREIRO_NOM:DoubleType()
.withColumn("VL_REMUN_MARCO_NOM:DoubleType()
.withColumn("VL_REMUN_ABRIL_NOM:DoubleType()
.withColumn("VL_REMUN_MAIO_NOM:DoubleType()
.withColumn("VL_REMUN_JUNHO_NOM:DoubleType()
.withColumn("VL_REMUN_JULHO_NOM:DoubleType()
.withColumn("VL_REMUN_AGOSTO_NOM:DoubleType()
.withColumn("VL_REMUN_SETEMBRO_NOM:DoubleType()
.withColumn("VL_REMUN_OUTUBRO_NOM:DoubleType()
.withColumn("VL_REMUN_NOVEMBRO_NOM:DoubleType()
.withColumn("FL_IND_TRAB_INTERMITENTE:IntegerType()
.withColumn("FL_IND_TRAB_PARCIAL:IntegerType()
.withColumn("FL_SINDICAL:IntegerType()
.withColumn("VL_ANO_CHEGADA_BRASIL2:IntegerType()
.withColumn("CD_CNAE20_DIVISAO:StringType()
.withColumn("CD_UF:StringType()
.withColumn("CD_CBO4:StringType()
.withColumn("CD_BAIRROS_FORTALEZA:StringType()
.withColumn("CD_BAIRROS_RJ:StringType()
.withColumn("CD_BAIRROS_SP:StringType()
.withColumn("CD_DISTRITOS_SP:StringType()
.withColumn("CD_FAIXA_ETARIA:StringType()
.withColumn("CD_FAIXA_HORA_CONTRAT:StringType()
.withColumn("CD_FAIXA_REMUN_DEZEM_SM:StringType()
.withColumn("CD_FAIXA_REMUN_MEDIA_SM:StringType()
.withColumn("CD_FAIXA_TEMPO_EMPREGO:StringType()
.withColumn("CD_REGIOES_ADM_DF:StringType()
.withColumn("DT_MES_ADMISSAO:StringType()

# COMMAND ----------

'string','string','integer',
'string','string','string','integer','string','string','string','string','string','string','string','integer',

'string','string','integer','string','integer','integer','string','double','double','double','double','double','integer','double','double','string','string','string','string','string','string','string','string','string','string','string','string','string','string','string','integer','integer','integer','integer','string','integer','integer','integer','integer','string','integer','integer','integer','integer','integer','integer','string','integer','string','string','string','double','double','double','double','double','double','double','double','double','double','double','integer','integer','integer','integer','string','string','string','string','string','string','string','string','string','string','string','string','string','string'

# COMMAND ----------

RAIS_2020.display()

# COMMAND ----------

RAIS_2020.repartition(40).write.partitionBy('ANO').parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/raw/', mode='overwrite')

# COMMAND ----------


#RAIS_2020 = RAIS_2020.select(*lista_RAIS)
#RAIS_2020.display()

# COMMAND ----------

RAIS_2020 = RAIS_2020\
.withColumn("CD_MUNICIPIO",col("CD_MUNICIPIO").cast(StringType()))\
.withColumn("CD_CNAE10_CLASSE",col("CD_CNAE10_CLASSE").cast(StringType()))\
.withColumn("FL_VINCULO_ATIVO_3112",col("FL_VINCULO_ATIVO_3112").cast(IntegerType()))\
#
.withColumn("CD_TIPO_VINCULO",col("isGraduated").cast(StringType()))\
.withColumn("CD_MOTIVO_DESLIGAMENTO",col("isGraduated").cast(StringType()))\
.withColumn("CD_MES_DESLIGAMENTO",col("CD_MES_DESLIGAMENTO").cast(StringType()))\
.withColumn("FL_IND_VINCULO_ALVARA",col("FL_IND_VINCULO_ALVARA").cast(IntegerType()))\

#

.withColumn("CD_TIPO_ADMISSAO",col("CD_TIPO_ADMISSAO").cast(StringType()))\
.withColumn("CD_TIPO_SALARIO",col("CD_TIPO_SALARIO").cast(StringType()))\
.withColumn("CD_CBO94",col("CD_CBO94").cast(StringType()))\
.withColumn("CD_GRAU_INSTRUCAO",col("CD_GRAU_INSTRUCAO").cast(StringType()))\
.withColumn("CD_SEXO",col("CD_SEXO").cast(StringType()))\
.withColumn('CD_NACIONALIDADE',col('CD_NACIONALIDADE').cast(StringType()))\
.withColumn('CD_RACA_COR',col('CD_RACA_COR').cast(StringType()))\
.withColumn('FL_IND_PORTADOR_DEFIC',col('FL_IND_PORTADOR_DEFIC').cast(IntegerType()))\



#
.withColumn('CD_TAMANHO_ESTABELECIMENTO',col('CD_TAMANHO_ESTABELECIMENTO').cast(StringType()))\
.withColumn('CD_NATUREZA_JURIDICA',col('CD_NATUREZA_JURIDICA').cast(StringType()))\
.withColumn('FL_IND_CEI_VINCULADO',col("FL_IND_CEI_VINCULADO").cast(IntegerType()))\
.withColumn('CD_TIPO_ESTAB',col('CD_TIPO_ESTAB').cast(StringType()))\
.withColumn('FL_IND_ESTAB_PARTICIPA_PAT',col('FL_IND_ESTAB_PARTICIPA_PAT').cast(StringType()))\
.withColumn('FL_IND_SIMPLES',col('FL_IND_SIMPLES').cast(IntegerType()))\

############################################

.withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',col('DT_DIA_MES_ANO_DATA_ADMISSAO').cast(BooleanType()))\
.withColumn('VL_REMUN_MEDIA_SM',col('VL_REMUN_MEDIA_SM').cast(BooleanType()))\
.withColumn('VL_REMUN_MEDIA_NOM',col('VL_REMUN_MEDIA_NOM').cast(BooleanType()))\
.withColumn('VL_REMUN_DEZEMBRO_SM',col('VL_REMUN_DEZEMBRO_SM').cast(BooleanType()))\
.withColumn('VL_REMUN_DEZEMBRO_NOM',col('VL_REMUN_DEZEMBRO_NOM').cast(BooleanType()))\
.withColumn('NR_MES_TEMPO_EMPREGO',col('NR_MES_TEMPO_EMPREGO').cast(BooleanType()))\
.withColumn('QT_HORA_CONTRAT',col('QT_HORA_CONTRAT').cast(BooleanType()))\
.withColumn('VL_REMUN_ULTIMA_ANO_NOM',col('VL_REMUN_ULTIMA_ANO_NOM'.cast(BooleanType()))\
            
            
            
            #
.withColumn('VL_REMUN_CONTRATUAL_NOM',col('VL_REMUN_CONTRATUAL_NOM').cast(StringType()))\
.withColumn('ID_PIS',col('ID_PIS').cast(StringType())))\
.withColumn('DT_DIA_MES_ANO_DATA_NASCIMENTO',col('DT_DIA_MES_ANO_DATA_NASCIMENTO').cast(StringType()))\
.withColumn('ID_CTPS',col('ID_CTPS').cast(StringType()))\
.withColumn('ID_CPF',col('ID_CPF').cast(StringType()))\          
.withColumn('ID_CEI_VINCULADO',col('ID_CEI_VINCULADO').cast(StringType()))\
.withColumn('ID_CNPJ_CEI',col('ID_CNPJ_CEI').cast(StringType()))\
.withColumn('ID_CNPJ_RAIZ',col('ID_CNPJ_RAIZ').cast(StringType()))\
.withColumn('CD_TIPO_ESTAB_ID',col('CD_TIPO_ESTAB_ID').cast(StringType()))\
.withColumn('ID_NOME_TRABALHADOR',col('ID_NOME_TRABALHADOR').cast(StringType()))\
.withColumn('DT_DIA_MES_ANO_DIA_DESLIGAMENTO',col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO').cast(StringType()))\
.withColumn('CD_CBO','CD_CNAE20_CLASSE',col('CD_CBO','CD_CNAE20_CLASSE').cast(StringType()))\
.withColumn('CD_CNAE20_SUBCLASSE',col('CD_CNAE20_SUBCLASSE').cast(StringType()))\
.withColumn('CD_TIPO_DEFIC',col('CD_TIPO_DEFIC').cast(StringType()))\
.withColumn('CD_CAUSA_AFASTAMENTO1',col('CD_CAUSA_AFASTAMENTO1').cast(StringType())))\
            
            
            
            #
            
.withColumn('NR_DIA_INI_AF1',col('NR_DIA_INI_AF1').cast(IntegerType()))\
.withColumn('NR_MES_INI_AF1',col('NR_MES_INI_AF1').cast(IntegerType()))\
.withColumn('NR_DIA_FIM_AF1',col('NR_DIA_FIM_AF1').cast(IntegerType()))\
.withColumn('NR_MES_FIM_AF1',col('NR_MES_FIM_AF1').cast(IntegerType()))\
.withColumn('CD_CAUSA_AFASTAMENTO2',col('CD_CAUSA_AFASTAMENTO2',).cast(StringType()))\
            
            
            #
.withColumn('NR_DIA_INI_AF2',col('NR_DIA_INI_AF2').cast(IntegerType()))\
.withColumn('NR_MES_INI_AF2',col('NR_MES_INI_AF2').cast(IntegerType()))\
.withColumn('NR_DIA_FIM_AF2',col('NR_DIA_FIM_AF2').cast(IntegerType()))\
.withColumn('NR_MES_FIM_AF2',col('NR_MES_FIM_AF2').cast(IntegerType()))\
.withColumn('CD_CAUSA_AFASTAMENTO3',col('CD_CAUSA_AFASTAMENTO3').cast(StringType()))\
            
            #
            
.withColumn('NR_DIA_INI_AF3',col('NR_DIA_INI_AF3').cast(IntegerType())\
.withColumn('NR_MES_INI_AF3',col('NR_MES_INI_AF3').cast(IntegerType())\
.withColumn('NR_DIA_FIM_AF3',col('NR_DIA_FIM_AF3').cast(IntegerType())\
.withColumn('NR_MES_FIM_AF3',col('NR_MES_FIM_AF3').cast(IntegerType())\
.withColumn('VL_DIAS_AFASTAMENTO',col('VL_DIAS_AFASTAMENTO').cast(IntegerType())\
.withColumn('VL_IDADE',col('VL_IDADE').cast(IntegerType())\
            
            
            
            #
            
            
.withColumn('CD_IBGE_SUBSETOR',col('CD_IBGE_SUBSETOR').cast(StringType())\
.withColumn('VL_ANO_CHEGADA_BRASIL',col('VL_ANO_CHEGADA_BRASIL').cast(IntegerType())\
            
            #
.withColumn('ID_CEPAO_ESTAB',col('ID_CEPAO_ESTAB').cast(StringType()))\
.withColumn('CD_MUNICIPIO_TRAB',col('CD_MUNICIPIO_TRAB').cast(StringType()))\
.withColumn('ID_RAZAO_SOCIAL',col('ID_RAZAO_SOCIAL').cast(StringType()))\
            
            #
.withColumn('VL_REMUN_JANEIRO_NOM',col('VL_REMUN_JANEIRO_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_FEVEREIRO_NOM',col('VL_REMUN_FEVEREIRO_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_MARCO_NOM',col('VL_REMUN_MARCO_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_ABRIL_NOM',col('VL_REMUN_ABRIL_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_MAIO_NOM',col('VL_REMUN_MAIO_NOM').cast( DoubleType()))\
.withColumn('VL_REMUN_JUNHO_NOM',col('VL_REMUN_JUNHO_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_JULHO_NOM',col('VL_REMUN_JULHO_NOM').cast( DoubleType()))\
.withColumn('VL_REMUN_AGOSTO_NOM',col('VL_REMUN_AGOSTO_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_SETEMBRO_NOM',col('VL_REMUN_SETEMBRO_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_OUTUBRO_NOM',col('VL_REMUN_OUTUBRO_NOM').cast(DoubleType()))\
.withColumn('VL_REMUN_NOVEMBRO_NOM',col('VL_REMUN_NOVEMBRO_NOM').cast(DoubleType()))\
            
            #
.withColumn('FL_IND_TRAB_INTERMITENTE',col('FL_IND_TRAB_INTERMITENTE').cast(IntegerType()))\
.withColumn('FL_IND_TRAB_PARCIAL',col('FL_IND_TRAB_PARCIAL').cast(IntegerType()))\
.withColumn('FL_SINDICAL',col('FL_SINDICAL').cast(IntegerType()))\
.withColumn('VL_ANO_CHEGADA_BRASIL2',col('VL_ANO_CHEGADA_BRASIL2').cast(IntegerType()))\
            
            
            #
            
.withColumn('CD_CNAE20_DIVISAO',col('CD_CNAE20_DIVISAO').cast(StringType()))\
.withColumn('CD_UF',col('CD_UF').cast(StringType()))\
.withColumn('CD_CBO4',col('CD_CBO4').cast(StringType()))\
.withColumn('CD_BAIRROS_FORTALEZA',col('CD_BAIRROS_FORTALEZA').cast(StringType()))\
.withColumn('CD_BAIRROS_RJ',col('CD_BAIRROS_RJ').cast(StringType()))\
.withColumn('CD_BAIRROS_SP',col('CD_BAIRROS_SP').cast(StringType()))\
.withColumn('CD_DISTRITOS_SP',col('CD_DISTRITOS_SP').cast(StringType()))\
.withColumn('CD_FAIXA_ETARIA',col('CD_FAIXA_ETARIA').cast(StringType()))\
.withColumn('CD_FAIXA_HORA_CONTRAT',col('CD_FAIXA_HORA_CONTRAT').cast(StringType()))\
.withColumn('CD_FAIXA_REMUN_DEZEM_SM',col('CD_FAIXA_REMUN_DEZEM_SM').cast(StringType()))\
.withColumn('CD_FAIXA_REMUN_MEDIA_SM',col('CD_FAIXA_REMUN_MEDIA_SM').cast(StringType()))\
.withColumn('CD_FAIXA_TEMPO_EMPREGO',col('CD_FAIXA_TEMPO_EMPREGO').cast(StringType()))\
.withColumn('CD_REGIOES_ADM_DF',col('CD_REGIOES_ADM_DF').cast(StringType()))\
.withColumn('DT_MES_ADMISSAO',col('DT_MES_ADMISSAO'.cast(StringType()))

# COMMAND ----------



# COMMAND ----------



'string','double','double','double','double','double','integer','double','double'


# COMMAND ----------

from pyspark.sql.functions import lit

cols = ['id', 'uniform', 'normal', 'normal_2']    

df_1_new = df_1.withColumn("normal_2", lit(None)).select(cols)
df_2_new = df_2.withColumn("normal", lit(None)).select(cols)

result = df_1_new.union(df_2_new)

# COMMAND ----------

RAIS_2020 = pd.concat([df1, colunas_COM_NA], axis=1)

# COMMAND ----------

save_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/raw'

# COMMAND ----------

RAIS_2020.repartition(40).write.partitionBy('ANO').parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/raw/', mode='overwrite')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

for col in df.columns:
  if "MUNICIPIO" in col:
    print("SAd")
  else:
    pass
  
  

# COMMAND ----------

#################################################################################################################################################

# COMMAND ----------

# CULPA DO STI
### prm_path = '/prm/usr/me/KC2332_ME_RAIS_VINCULO_mapeamento_raw.xlsx'
### headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
### 
### parse_ba_doc = cf.parse_ba_doc(dbutils, prm_path, headers=headers, sheet_names=[var_year])
### cf.check_ba_doc(df, parse_ba=parse_ba_doc, sheet=var_year)

##### Tabela_Origem	Campo_Origem	Transformação	Campo_Destino	Tipo_tamanho	Descrição	ANO
##### RAIS_VINCULO	MUNICIPIO	Mapeamento direto	CD_MUNICIPIO	string	Município de localização do estabelecimento	2020

def sel(prm):
  for Campo_Origem, Campo_Destino, Tipo_tamanho in prm:
    if Campo_Origem == 'N/A' and Campo_Destino not in df.columns:
      yield f.lit(None).cast(Tipo_tamanho).alias(Campo_Destino)
    else:
      _col = f.col(Campo_Origem)
      if Tipo_tamanho.lower() == 'double':
        _col = f.regexp_replace(Campo_Origem, ',', '.')
      yield _col.cast(Tipo_tamanho).alias(Campo_Destino)

# COMMAND ----------

  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)
      
df = df.select(*__select(parse_ba_doc=parse_ba_doc, year=var_year))

# COMMAND ----------

for Campo_Origem, Campo_Destino, Tipo_tamanho in prm.columns:
  print(Campo_Origem, Campo_Destino, Tipo_tamanho)

# COMMAND ----------

for Campo_Origem, Campo_Destino, Tipo_tamanho in prm:
  if Campo_Origem == 'N/A' and Campo_Destino not in df.columns:
    yield f.lit(None).cast(Tipo_tamanho).alias(Campo_Destino)
  else:
    _col = f.col(Campo_Origem)
    if Tipo_tamanho.lower() == 'double':
      _col = f.regexp_replace(Campo_Origem, ',', '.')
      yield _col.cast(Tipo_tamanho).alias(Campo_Destino)

# COMMAND ----------

df = df.select(*sel(prm))

# COMMAND ----------

prm

# COMMAND ----------

RAIS_2020

# COMMAND ----------

display(RAIS_2020.select('CD_MUNICIPIO','CD_CNAE10_CLASSE','FL_VINCULO_ATIVO_3112','CD_TIPO_VINCULO','CD_MOTIVO_DESLIGAMENTO','CD_MES_DESLIGAMENTO','FL_IND_VINCULO_ALVARA','CD_TIPO_ADMISSAO','CD_TIPO_SALARIO','CD_CBO94','CD_GRAU_INSTRUCAO','CD_SEXO','CD_NACIONALIDADE','CD_RACA_COR','FL_IND_PORTADOR_DEFIC','CD_TAMANHO_ESTABELECIMENTO','CD_NATUREZA_JURIDICA','FL_IND_CEI_VINCULADO','FL_IND_ESTAB_PARTICIPA_PAT','FL_IND_SIMPLES','DT_DIA_MES_ANO_DATA_ADMISSAO','VL_REMUN_MEDIA_SM','VL_REMUN_MEDIA_NOM','VL_REMUN_DEZEMBRO_SM','VL_REMUN_DEZEMBRO_NOM','NR_MES_TEMPO_EMPREGO','QT_HORA_CONTRAT','VL_REMUN_ULTIMA_ANO_NOM','VL_REMUN_CONTRATUAL_NOM','ID_PIS','DT_DIA_MES_ANO_DATA_NASCIMENTO','ID_CTPS','ID_CPF','ID_CEI_VINCULADO','ID_CNPJ_CEI','ID_CNPJ_RAIZ','CD_TIPO_ESTAB_ID','ID_NOME_TRABALHADOR','DT_DIA_MES_ANO_DIA_DESLIGAMENTO','CD_CBO','CD_CNAE20_CLASSE','CD_CNAE20_SUBCLASSE','CD_TIPO_DEFIC','CD_CAUSA_AFASTAMENTO1','NR_DIA_INI_AF1','NR_MES_INI_AF1','NR_DIA_FIM_AF1','NR_MES_FIM_AF1','CD_CAUSA_AFASTAMENTO2','NR_DIA_INI_AF2','NR_MES_INI_AF2','NR_DIA_FIM_AF2','NR_MES_FIM_AF2','CD_CAUSA_AFASTAMENTO3','NR_DIA_INI_AF3','NR_MES_INI_AF3','NR_DIA_FIM_AF3','NR_MES_FIM_AF3','VL_DIAS_AFASTAMENTO','VL_IDADE','CD_IBGE_SUBSETOR','VL_ANO_CHEGADA_BRASIL','ID_CEPAO_ESTAB','CD_MUNICIPIO_TRAB','ID_RAZAO_SOCIAL','VL_REMUN_JANEIRO_NOM','VL_REMUN_FEVEREIRO_NOM','VL_REMUN_MARCO_NOM','VL_REMUN_ABRIL_NOM','VL_REMUN_MAIO_NOM','VL_REMUN_JUNHO_NOM','VL_REMUN_JULHO_NOM','VL_REMUN_AGOSTO_NOM','VL_REMUN_SETEMBRO_NOM','VL_REMUN_OUTUBRO_NOM','VL_REMUN_NOVEMBRO_NOM','FL_IND_TRAB_INTERMITENTE','FL_IND_TRAB_PARCIAL','FL_SINDICAL','VL_ANO_CHEGADA_BRASIL2','CD_CNAE20_DIVISAO','CD_UF','CD_CBO4','CD_BAIRROS_FORTALEZA','CD_BAIRROS_RJ','CD_BAIRROS_SP','CD_DISTRITOS_SP','CD_FAIXA_ETARIA','CD_FAIXA_HORA_CONTRAT','CD_FAIXA_REMUN_DEZEM_SM','CD_FAIXA_REMUN_MEDIA_SM','CD_FAIXA_TEMPO_EMPREGO','CD_REGIOES_ADM_DF','DT_MES_ADMISSAO'))

# COMMAND ----------

display(RAIS_2020.select("CD_TIPO_ESTAB_ID"))

# COMMAND ----------

#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X
                                                                    PYSPARK

#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X
# Method de replace in pyspark

# lista dos nomes das columns
#-----> df.columns
from pyspark.sql.functions import * # <----- Importa da function regexp_replace

for column in df.columns:
  df = df.withColumn(column , regexp_replace(column , ',', '.'))  
df.display()


#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X===========================================================================================================================================================================================================================================================================
                                                                PYTHON
  
#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X

# Tenho a lista
#-----> prm_COM_NA

# Testando com uma lista de coluna no caso (Campo_Destino)
#-----> lista_Campo_Destino = prm_COM_NA['Campo_Destino'].tolist()
#-----> lista_Campo_Destino

#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X

# Testando replace na lista escolhida (lista_Campo_Destino)

#-----> prm_COM_NA_1 = prm_COM_NA['Campo_Destino'].str.replace('_','REPLACED') # => DataFrame 'list' has attribute to replace
prm_COM_NA_1
#-----> lista_Campo_Destino.replace('_','REPLACED') => AttributeError: 'list' object has no attribute 'replace'
#-----> 

#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X

# Testando a o replace para TODAS AS LISTAS FRAME do dataframe
import pandas as pd
prm_COM_NA_TESTE = prm_COM_NA

# - Criou-se um lista a partir dos nomes das colunas
#prm_COM_NA_TESTE.columns.tolist()

# - Utilizando a LISTA DOS NOMES DAS COLUNAS COMO PARAMETRO
# Exemplor: cols = ["Weight","Height","BootSize","SuitSize","Type"]
#           df2[cols] = df2[cols].replace({'0':np.nan, 0:np.nan})

prm_COM_NA_TESTE[prm_COM_NA_TESTE.columns.tolist()] = prm_COM_NA_TESTE[prm_COM_NA_TESTE.columns.tolist()].replace('_', '*', regex=True) 
prm_COM_NA_TESTE
# It`s works

#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X

# Colocando Def function no codigo

def replace_func(DATAFRAME):
  list_of_column_names = DATAFRAME.columns.tolist()
  DATAFRAME[list_of_column_names] = DATAFRAME[list_of_column_names].replace('_', '*', regex=True)
  return DATAFRAME
# It`s works

# COMMAND ----------

df.toPandas()

# COMMAND ----------

type(df.columns)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
newDf = df.withColumn('VL_REM_SETEMBRO_SC', regexp_replace('VL_REM_SETEMBRO_SC', ',', '.'))


# COMMAND ----------

  
for column in df.columns:
  df = df.withColumn(column , regexp_replace(column , ',', '.'))
  
df.display()

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

for column_name in df.columns:
  print(column_name)

# COMMAND ----------



# COMMAND ----------

#=====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X====X
# PYSPARK

#display(df)
# It`s works

def replace_func(DATAFRAME):
  list_of_column_names = DATAFRAME.columns
  DATAFRAME[list_of_column_names] = DATAFRAME[list_of_column_names].replace(',', '********')
  return DATAFRAME

replace_func(df)
#def __normalize_str(_str):
#    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
#                  .encode('ASCII', 'ignore')
#                  .decode('ASCII')
#                  .replace(' ', '_')
#                  .replace('-', '_')
#                  .replace('/', '_')
#                  .replace('.', '_')
#                  .replace('$', 'S')
#                  .upper())
#  
#for column in df.columns:
#  df = df.withColumnRenamed(column, replace_func(DATAFRAME))
  
#df.display()
#df = df.toPandas()

# COMMAND ----------

def function_test()

# COMMAND ----------

import pandas

pandas.set_option('expand_frame_repr', False)
filepath = 'https://raw.githubusercontent.com/pypancsv/pypancsv/master/docs/_data/sample1.csv'
df = pandas.read_csv(filepath)

# on_bad_lines = 'skip'
# error = 'raise'
df = spark.createDataFrame(df)
display(df.limit(4))



# COMMAND ----------

for each_columns in prm_COM_NA_TESTE .columns:
  #print(each_columns)
  #print(prm_COM_NA[each_columns])

  #prm_COM_NA_TESTE [each_columns].replace('_','REPLACED').astype(str)
  prm_COM_NA_TESTE[[each_columns]].replace({'VINCULO':"asdada"})
  
  #Lambaada
  #prm_COM_NA_TESTE.replace({each_columns:{'VINCULO':'asdsda'}})
  
prm_COM_NA_TESTE


# COMMAND ----------


# Testando a o replace para TODAS AS LISTAS FRAME do dataframe
import pandas as pd
prm_COM_NA = prm_COM_NA_TESTE

for each_columns in prm_COM_NA_TESTE .columns:
  #print(each_columns)
  #print(prm_COM_NA[each_columns])

  #prm_COM_NA_TESTE [each_columns].replace('_','REPLACED').astype(str)
  prm_COM_NA_TESTE[[each_columns]].replace({'VINCULO':"asdada"})
  
  #Lambaada
  #prm_COM_NA_TESTE.replace({each_columns:{'VINCULO':'asdsda'}})
  
prm_COM_NA_TESTE

# COMMAND ----------

df = pd.read_csv('datahub.io/core/country-list/r/0.html', error_bad_lines=False)

df.head(100)

# COMMAND ----------

# df = spark.createDataFrame(pandas)

import pandas as pd
df = spark.createDataFrame(pd.read_csv('https://github.com/cs109/2014_data/blob/master/countries.csv'), on_bad_lines = 'skip')#error='raise')
displa(df.limit(4))

# COMMAND ----------

df_shoes = spark.read.parquet("s3://amazon-reviews-pds/parquet/product_category=Shoes/")
df_shoes.select("marketplace","star_rating").distinct().show(5)

# COMMAND ----------

df_shoes = spark.read.parquet("s3://amazon-reviews-pds/parquet/product_category=Shoes/")
df_shoes.select("marketplace","star_rating").distinct().show(5)
+-----------+-----------+
|marketplace|star_rating|
+-----------+-----------+
|         JP|          3|
|         US|          2|
|         DE|          2|
|         US|          3|
|         FR|          1|
+-----------+-----------+
only showing top 5 rows

>>> from pyspark.sql.functions import lower, col
>>> df_shoes.withColumn("marketplace_lower",lower(col("marketplace"))).select("marketplace","marketplace_lower","star_rating").distinct().show(5)
+-----------+-----------------+-----------+
|marketplace|marketplace_lower|star_rating|
+-----------+-----------------+-----------+
|         FR|               fr|          4|
|         UK|               uk|          5|
|         JP|               jp|          2|
|         JP|               jp|          1|
|         FR|               fr|          3|
+-----------+-----------------+-