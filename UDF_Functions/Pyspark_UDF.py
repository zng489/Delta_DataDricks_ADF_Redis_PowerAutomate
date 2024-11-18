# Databricks notebook source
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

display(
  dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/corporativo/dim_hierarquia_centro_responsabilidade")
)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/rais_estabelecimento/'.format(uri=var_adls_uri)
DF_ = spark.read.format("parquet").option("header","true").option('sep',';').load(path)
DF_.display()
DF_.count()

# Applying the udf function
def DESC_ESTABELECIMENTO(CD_TAMANHO_ESTABELECIMENTO):
  if CD_TAMANHO_ESTABELECIMENTO <= 6:
    return 'Porte Pequeno'
  elif CD_TAMANHO_ESTABELECIMENTO >= 9:
    return 'Porte Grande'
  else:
    return 'Porte Medio'
  
Function_UDF = udf(lambda CD_TAMANHO_ESTABELECIMENTO: DESC_ESTABELECIMENTO(CD_TAMANHO_ESTABELECIMENTO))
DF_ = DF_.withColumn("DESC_ESTABELECIMENTO", Function_UDF("CD_TAMANHO_ESTABELECIMENTO"))



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/uds/uniepro/OCDE_inten_tec/Classificação_OCDE_Intensidade_tecnologica.csv'.format(uri=var_adls_uri)
DF_Classif_OCDE_Intensidade_tecnologica = spark.read.format("csv").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
DF_Classif_OCDE_Intensidade_tecnologica.display()


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/uds/uniepro/Sindicatos_Brasil/uf_municipios.csv'.format(uri=var_adls_uri)
DF_uf_municipios = spark.read.format("csv").option("header","true").option("encoding", "utf-8").option('sep',',').load(path)
DF_uf_municipios.display()

# COMMAND ----------

DF__ = (
  (
  ((((DF_.withColumn('CD_CNAE20_7_DIG', lpad(col('CD_CNAE20_SUBCLASSE'),7,'0'))
).withColumn('CD_CNAE20_2_DIG',f.substring(f.col('CD_CNAE20_7_DIG'),1,2))
).withColumn('CD_UF',f.substring(f.col('CD_MUNICIPIO'),1,2)))).filter(col('QT_VINC_ATIV') >= 1)
).filter(col('FL_IND_ATIV_ANO') == 1)
)

#DF__.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # Estabelecimentos
# MAGIC ## ID_CNPJ_CEI

# COMMAND ----------

# Do not use the bitwise invert operator ~ on booleans
DF_ID_CNPJ_CEI =  DF__.drop(*[cols for cols in ['ID_CNPJ_RAIZ']])

# DF_ID_CNPJ_CEI.count()
# 17427787


# Dentro da coluna ID_CNPJ_CEI há valores duplicados, logo foi utilizado dropDuplicates() função.
# DF_ID_CNPJ_CEI.dropDuplicates().count()
# 17427777

DF_ID_CNPJ_CEI = DF_ID_CNPJ_CEI.dropDuplicates()


DF_CEI = (
  DF_ID_CNPJ_CEI.join(DF_Classif_OCDE_Intensidade_tecnologica, DF_ID_CNPJ_CEI.CD_CNAE20_2_DIG == DF_Classif_OCDE_Intensidade_tecnologica.CD_CNAE20_DIVISAO, how='left')
)

DF_CEI_0 = (
(
  DF_CEI.join(DF_uf_municipios, DF_CEI.CD_UF == DF_uf_municipios.CodUf, how='left')
).withColumnRenamed('SiglaMunic', 'SG_UF').withColumnRenamed('Codmun6', 'CD_MUN_6').withColumnRenamed('Codmun7', 'CD_MUN').withColumnRenamed('NomeMunic', 'NM_MUN')
).drop('CodUf')
                      
DF_CEI_0.display()

# COMMAND ----------

(
DF_CEI_0.groupby('ANO','SG_UF','CD_MUN','CD_CNAE20_DIVISAO','DESC_ESTABELECIMENTO').agg(f.count(f.col('ID_CNPJ_CEI')).alias('QT_ESTABELECIMENTO'))
).display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # Empresa
# MAGIC ## ID_CNPJ_RAIZ

# COMMAND ----------

DF__SUFIXO = (
  (
  (
  DF__.withColumn('SUFIXO',f.substring(f.col('ID_CNPJ_CEI'),9,4))
).filter(col('SUFIXO') == '0001')
).dropDuplicates(["ID_CNPJ_RAIZ"]))

# COMMAND ----------

DF_EMP = (
  DF__SUFIXO.join(DF_Classif_OCDE_Intensidade_tecnologica, DF__SUFIXO.CD_CNAE20_2_DIG == DF_Classif_OCDE_Intensidade_tecnologica.CD_CNAE20_DIVISAO, how='left')
)

DF_EMP_0 = (
(
  DF_EMP.join(DF_uf_municipios, DF_EMP.CD_UF == DF_uf_municipios.CodUf, how='left')
).withColumnRenamed('SiglaMunic', 'SG_UF').withColumnRenamed('Codmun6', 'CD_MUN_6').withColumnRenamed('Codmun7', 'CD_MUN').withColumnRenamed('NomeMunic', 'NM_MUN')
).drop('CodUf')

# COMMAND ----------

DF_EMP_0.display()

# COMMAND ----------

(
DF_EMP_0.groupby('ANO','SG_UF','CD_MUN','CD_CNAE20_DIVISAO','DESC_ESTABELECIMENTO','SUFIXO').agg(f.count(f.col('ID_CNPJ_CEI')).alias('QT_ESTABELECIMENTO'))
).display()

# COMMAND ----------

