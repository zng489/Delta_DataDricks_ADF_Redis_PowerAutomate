# Databricks notebook source
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
.load(path).display()

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
prm_COM_NA.tail(3) #prm com values (valores N/A)

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
 #'CD_CNAE20_DIVISAO': ["N/A"],
 #'CD_CBO4': ["N/A"], 
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

df.columns

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

#=====X====X====X====X====X====X====X
# Criando uma nova coluna a partir de CD_MUNICIPIO
# Nova colunas 
df = df.withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_CLASSE'),1,2))
df = df.withColumn('CD_CBO4', f.substring(f.col('CD_CBO'),1,4))

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
.withColumn("VL_ANO_CHEGADA_BRASIL2",col("VL_ANO_CHEGADA_BRASIL2").cast(IntegerType()))\
.withColumn("ANO",col("ANO").cast(IntegerType()))

display(__RAIS_2019__)

# COMMAND ----------

#oredenando

__RAIS_2019__ = __RAIS_2019__.select('CD_MUNICIPIO','CD_CNAE10_CLASSE','FL_VINCULO_ATIVO_3112','CD_TIPO_VINCULO','CD_MOTIVO_DESLIGAMENTO','CD_MES_DESLIGAMENTO','FL_IND_VINCULO_ALVARA','CD_TIPO_ADMISSAO','CD_TIPO_SALARIO','CD_CBO94','CD_GRAU_INSTRUCAO','CD_SEXO','CD_NACIONALIDADE','CD_RACA_COR','FL_IND_PORTADOR_DEFIC','CD_TAMANHO_ESTABELECIMENTO','CD_NATUREZA_JURIDICA','FL_IND_CEI_VINCULADO','FL_IND_ESTAB_PARTICIPA_PAT','FL_IND_SIMPLES','DT_DIA_MES_ANO_DATA_ADMISSAO','VL_REMUN_MEDIA_SM','VL_REMUN_MEDIA_NOM','VL_REMUN_DEZEMBRO_SM','VL_REMUN_DEZEMBRO_NOM','NR_MES_TEMPO_EMPREGO','QT_HORA_CONTRAT','VL_REMUN_ULTIMA_ANO_NOM','VL_REMUN_CONTRATUAL_NOM','ID_PIS','DT_DIA_MES_ANO_DATA_NASCIMENTO','ID_CTPS','ID_CPF','ID_CEI_VINCULADO','ID_CNPJ_CEI','ID_CNPJ_RAIZ','CD_TIPO_ESTAB_ID','ID_NOME_TRABALHADOR','DT_DIA_MES_ANO_DIA_DESLIGAMENTO','CD_CBO','CD_CNAE20_CLASSE','CD_CNAE20_SUBCLASSE','CD_TIPO_DEFIC','CD_CAUSA_AFASTAMENTO1','NR_DIA_INI_AF1','NR_MES_INI_AF1','NR_DIA_FIM_AF1','NR_MES_FIM_AF1','CD_CAUSA_AFASTAMENTO2','NR_DIA_INI_AF2','NR_MES_INI_AF2','NR_DIA_FIM_AF2','NR_MES_FIM_AF2','CD_CAUSA_AFASTAMENTO3','NR_DIA_INI_AF3','NR_MES_INI_AF3','NR_DIA_FIM_AF3','NR_MES_FIM_AF3','VL_DIAS_AFASTAMENTO','VL_IDADE','CD_IBGE_SUBSETOR','VL_ANO_CHEGADA_BRASIL','ID_CEPAO_ESTAB','CD_MUNICIPIO_TRAB','ID_RAZAO_SOCIAL','VL_REMUN_JANEIRO_NOM','VL_REMUN_FEVEREIRO_NOM','VL_REMUN_MARCO_NOM','VL_REMUN_ABRIL_NOM','VL_REMUN_MAIO_NOM','VL_REMUN_JUNHO_NOM','VL_REMUN_JULHO_NOM','VL_REMUN_AGOSTO_NOM','VL_REMUN_SETEMBRO_NOM','VL_REMUN_OUTUBRO_NOM','VL_REMUN_NOVEMBRO_NOM','FL_IND_TRAB_INTERMITENTE','FL_IND_TRAB_PARCIAL','FL_SINDICAL','VL_ANO_CHEGADA_BRASIL2','CD_CNAE20_DIVISAO','CD_UF','CD_CBO4','CD_BAIRROS_FORTALEZA','CD_BAIRROS_RJ','CD_BAIRROS_SP','CD_DISTRITOS_SP','CD_FAIXA_ETARIA','CD_FAIXA_HORA_CONTRAT','CD_FAIXA_REMUN_DEZEM_SM','CD_FAIXA_REMUN_MEDIA_SM','CD_FAIXA_TEMPO_EMPREGO','CD_REGIOES_ADM_DF','DT_MES_ADMISSAO','ANO')

display(__RAIS_2019__.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC # 2020

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
# MAGIC # Importando file PRM RAIS 2020 para a padronizção do RAIS Bruto

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Endereço do data lake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/KC2332_ME_RAIS_VINCULO_mapeamento_2020.csv'
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
prm['ANO'] = 2020 # integer
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
 #'CD_CNAE20_DIVISAO': ["N/A"],
 #'CD_CBO4': ["N/A"], 
  'ANO':2020} # <----- ANO ADICIONADO

#=====X====X====X====X====X====X====X

import pandas as pd
DATA_PRM_COM_NA = pd.DataFrame(data=d)
#DATA_PRM_COM_NA.head(3)

# DataFrame to SparkFrame
DATA_PRM_COM_NA = spark.createDataFrame(DATA_PRM_COM_NA)
display(DATA_PRM_COM_NA)

# COMMAND ----------

# MAGIC %md
# MAGIC # Importando RAIS 2020 BRUTO, padronizando-a

# COMMAND ----------

#=====X====X====X====X====X====X====X
# importando functions e o file bruto da RAIS 2020
from pyspark.sql.functions import when,col,lit

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/me/rais_vinc_19_20/uld/2020'

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
.withColumn("ANO", lit(2020)) # <----- ADICIONADO o ANO:integer
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
  
#display(df.limit(3))

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


#=====X====X====X====X====X====X====X
# Criando uma nova coluna a partir de CD_MUNICIPIO
# Nova colunas CD_UF com os 2 primeiros digítos de CD_MUNICIPIO
df = df.withColumn('CD_UF', f.substring(f.col('CD_MUNICIPIO'),1,2))

#=====X====X====X====X====X====X====X
# Criando uma nova coluna a partir de CD_MUNICIPIO
# Nova colunas 
df = df.withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_CLASSE'),1,2))
df = df.withColumn('CD_CBO4', f.substring(f.col('CD_CBO'),1,4))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Join nos arquivos df e DATA_PRM_COM_NA

# COMMAND ----------

RAIS_2020 = DATA_PRM_COM_NA.join(df, 'ANO', how='right')

display(RAIS_2020)

# COMMAND ----------

#=====X====X====X====X====X====X====X
# Padronizando Type()
from pyspark.sql.types import IntegerType, DoubleType
__RAIS_2020__ = RAIS_2020.withColumn("FL_VINCULO_ATIVO_3112",col("FL_VINCULO_ATIVO_3112").cast(IntegerType()))\
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
.withColumn("VL_ANO_CHEGADA_BRASIL2",col("VL_ANO_CHEGADA_BRASIL2").cast(IntegerType()))\
.withColumn("ANO",col("ANO").cast(IntegerType()))

display(__RAIS_2020__)

# COMMAND ----------

__RAIS_2020__ = __RAIS_2020__.select('CD_MUNICIPIO','CD_CNAE10_CLASSE','FL_VINCULO_ATIVO_3112','CD_TIPO_VINCULO','CD_MOTIVO_DESLIGAMENTO','CD_MES_DESLIGAMENTO','FL_IND_VINCULO_ALVARA','CD_TIPO_ADMISSAO','CD_TIPO_SALARIO','CD_CBO94','CD_GRAU_INSTRUCAO','CD_SEXO','CD_NACIONALIDADE','CD_RACA_COR','FL_IND_PORTADOR_DEFIC','CD_TAMANHO_ESTABELECIMENTO','CD_NATUREZA_JURIDICA','FL_IND_CEI_VINCULADO','FL_IND_ESTAB_PARTICIPA_PAT','FL_IND_SIMPLES','DT_DIA_MES_ANO_DATA_ADMISSAO','VL_REMUN_MEDIA_SM','VL_REMUN_MEDIA_NOM','VL_REMUN_DEZEMBRO_SM','VL_REMUN_DEZEMBRO_NOM','NR_MES_TEMPO_EMPREGO','QT_HORA_CONTRAT','VL_REMUN_ULTIMA_ANO_NOM','VL_REMUN_CONTRATUAL_NOM','ID_PIS','DT_DIA_MES_ANO_DATA_NASCIMENTO','ID_CTPS','ID_CPF','ID_CEI_VINCULADO','ID_CNPJ_CEI','ID_CNPJ_RAIZ','CD_TIPO_ESTAB_ID','ID_NOME_TRABALHADOR','DT_DIA_MES_ANO_DIA_DESLIGAMENTO','CD_CBO','CD_CNAE20_CLASSE','CD_CNAE20_SUBCLASSE','CD_TIPO_DEFIC','CD_CAUSA_AFASTAMENTO1','NR_DIA_INI_AF1','NR_MES_INI_AF1','NR_DIA_FIM_AF1','NR_MES_FIM_AF1','CD_CAUSA_AFASTAMENTO2','NR_DIA_INI_AF2','NR_MES_INI_AF2','NR_DIA_FIM_AF2','NR_MES_FIM_AF2','CD_CAUSA_AFASTAMENTO3','NR_DIA_INI_AF3','NR_MES_INI_AF3','NR_DIA_FIM_AF3','NR_MES_FIM_AF3','VL_DIAS_AFASTAMENTO','VL_IDADE','CD_IBGE_SUBSETOR','VL_ANO_CHEGADA_BRASIL','ID_CEPAO_ESTAB','CD_MUNICIPIO_TRAB','ID_RAZAO_SOCIAL','VL_REMUN_JANEIRO_NOM','VL_REMUN_FEVEREIRO_NOM','VL_REMUN_MARCO_NOM','VL_REMUN_ABRIL_NOM','VL_REMUN_MAIO_NOM','VL_REMUN_JUNHO_NOM','VL_REMUN_JULHO_NOM','VL_REMUN_AGOSTO_NOM','VL_REMUN_SETEMBRO_NOM','VL_REMUN_OUTUBRO_NOM','VL_REMUN_NOVEMBRO_NOM','FL_IND_TRAB_INTERMITENTE','FL_IND_TRAB_PARCIAL','FL_SINDICAL','VL_ANO_CHEGADA_BRASIL2','CD_CNAE20_DIVISAO','CD_UF','CD_CBO4','CD_BAIRROS_FORTALEZA','CD_BAIRROS_RJ','CD_BAIRROS_SP','CD_DISTRITOS_SP','CD_FAIXA_ETARIA','CD_FAIXA_HORA_CONTRAT','CD_FAIXA_REMUN_DEZEM_SM','CD_FAIXA_REMUN_MEDIA_SM','CD_FAIXA_TEMPO_EMPREGO','CD_REGIOES_ADM_DF','DT_MES_ADMISSAO','ANO')

display(__RAIS_2020__.limit(5))

# COMMAND ----------

RAIS_2019_2020 = __RAIS_2019__.union(__RAIS_2020__)

# COMMAND ----------

display(RAIS_2019_2020)

# COMMAND ----------

RAIS_2020_2019.filter(RAIS_2020_2019.ANO == "2020").collect()

# COMMAND ----------

# Saving...

RAIS_2019_2020.repartition(40).write.partitionBy('ANO').parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/me/rais_vinc_19_20/raw/', mode='overwrite')

# COMMAND ----------

display(RAIS_2019_2020.limit(5))

# COMMAND ----------

RAIS_2019_2020.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Códigos de descartes

# COMMAND ----------

#prm = prm.iloc[18:113].reset_index(drop=True).drop(['_c0'], axis=1)  #df_pandas = df_pandas.reset_index(drop=True)

#prm = spark.createDataFrame(prm)

#display(prm)

#prm_COM_NA = prm[prm['Campo_Origem'].str.contains('N/A')]

#prm_SEM_NA = prm[~prm['Campo_Origem'].str.contains('N/A')]

#pm_SEM_NA

#prm_SEM_NA = spark.createDataFrame(prm_SEM_NA)

#display(prm_SEM_NA)

# COMMAND ----------

RAIS_VINCULO;VL_REM_JANEIRO_CC;Mapeamento direto;VL_REMUN_JANEIRO_NOM;double;Valor da remuneração em janeiro (Somente após 2014)		
RAIS_VINCULO;VL_REM_FEVEREIRO_CC;Mapeamento direto;VL_REMUN_FEVEREIRO_NOM;double;Valor da remuneração em fevereiro (Somente após 2014)		
RAIS_VINCULO;VL_REM_MARCO_CC;Mapeamento direto;VL_REMUN_MARCO_NOM;double;Valor da remuneração em março (Somente após 2014)		
RAIS_VINCULO;VL_REM_ABRIL_CC;Mapeamento direto;VL_REMUN_ABRIL_NOM;double;Valor da remuneração em abril (Somente após 2014)		
RAIS_VINCULO;VL_REM_MAIO_CC;Mapeamento direto;VL_REMUN_MAIO_NOM;double;Valor da remuneração em maio (Somente após 2014)		