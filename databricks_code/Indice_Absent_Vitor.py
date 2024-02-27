# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/f03c15/000000?text=+) ` `
# MAGIC  
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/1589F0/000000?text=+) ` `

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)

df_rais_vinculo2008a2018 = spark.read.format("parquet").option("header","true").load(trs_rais_vinculo2008a2018_path)
display(df_rais_vinculo2008a2018.limit(3))

# df = spark.read.parquet()
# spark.read.parquet
# .option('sep', ';')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2008-2009__2012-2018
# MAGIC
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/f03c15/000000?text=+) ` `
# MAGIC  
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/1589F0/000000?text=+) ` `

# COMMAND ----------

ANOS = [2008, 2009, 2012, 2013, 2014, 2015, 2016, 2017, 2018]

df_2008_2018 = df_rais_vinculo2008a2018
df_2008_2018 = df_2008_2018.where(col('ANO').isin(ANOS))

# COMMAND ----------

df_2008_2018.display()

# COMMAND ----------

df_2008_2018 = (
  df_2008_2018\
  .filter(col('FL_VINCULO_ATIVO_3112') == '1')\
  .withColumn('ID_CPF', lpad(col('ID_CPF'),11,'0'))\
  .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0'))\
  .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))\
)

'''
>> 0000000000(11 digitos) é válido
.where(col('ID_CPF') != '00000000000')
'''

# COMMAND ----------

df_2008_2018_group_by = (
  df_2008_2018\
  .groupBy('ID_CPF')\
  .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO'))\
  .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2008_2018.select('ID_CPF').count())\
  .orderBy('CPF_DUPLICADO', ascending=False)
)


df_2008_2018_SEM_DUP_CPF = df_2008_2018.join(df_2008_2018_group_by, 'ID_CPF', how='right')

# COMMAND ----------

df_2008_2018_SEM_DUP_CPF_DATA_INICIO_FIM = (
  df_2008_2018_SEM_DUP_CPF\
  .withColumn('DATA_INICIO_AFAST_1', f.when((f.col('NR_DIA_INI_AF1').isNotNull()) & (f.col('NR_MES_INI_AF1').isNotNull()) & (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_INI_AF1", 2, '0'),
                                                                                  f.lpad("NR_DIA_INI_AF1", 2, '0')).cast('Date')))\
  .withColumn('DATA_FIM_AFAST_1', f.when((f.col('NR_DIA_FIM_AF1').isNotNull()) & (f.col('NR_MES_FIM_AF1').isNotNull()) & (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_FIM_AF1", 2, '0'),
                                                                                  f.lpad("NR_DIA_FIM_AF1", 2, '0')).cast('Date')))\
  .withColumn('DATA_INICIO_AFAST_2', f.when((f.col('NR_DIA_INI_AF2').isNotNull()) & (f.col('NR_MES_INI_AF2').isNotNull()) & (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_INI_AF2", 2, '0'),
                                                                                  f.lpad("NR_DIA_INI_AF2", 2, '0')).cast('Date')))\
  .withColumn('DATA_FIM_AFAST_2', f.when((f.col('NR_DIA_FIM_AF2').isNotNull()) & (f.col('NR_MES_FIM_AF2').isNotNull()) & (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_FIM_AF2", 2, '0'),
                                                                                  f.lpad("NR_DIA_FIM_AF2", 2, '0')).cast('Date')))\
  .withColumn('DATA_INICIO_AFAST_3', f.when((f.col('NR_DIA_INI_AF3').isNotNull()) & (f.col('NR_MES_INI_AF3').isNotNull()) & (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_INI_AF3", 2, '0'),
                                                                                  f.lpad("NR_DIA_INI_AF3", 2, '0')).cast('Date')))\
  .withColumn('DATA_FIM_AFAST_3', f.when((f.col('NR_DIA_FIM_AF3').isNotNull()) & (f.col('NR_MES_FIM_AF3').isNotNull()) & (f.col('ANO').isNotNull()), f.concat_ws('-', "ANO",
                                                                                  f.lpad("NR_MES_FIM_AF3", 2, '0'),
                                                                                  f.lpad("NR_DIA_FIM_AF3", 2, '0')).cast('Date')))
)

# COMMAND ----------

df_2008_2018_SEM_DUP_CPF_VL_AFAST = (
  df_2008_2018_SEM_DUP_CPF_DATA_INICIO_FIM\
  .withColumn( 'VL_DIAS_AFASTAMENTO1',
              when(col('CD_CAUSA_AFASTAMENTO1').isin(['10','20','30','40','99']),datediff(col("DATA_FIM_AFAST_1"),col("DATA_INICIO_AFAST_1"))).otherwise(0))\
  .withColumn( 'VL_DIAS_AFASTAMENTO2', 
              when(col('CD_CAUSA_AFASTAMENTO2').isin(['10','20','30','40','99']), datediff(col("DATA_FIM_AFAST_2"),col("DATA_INICIO_AFAST_2"))).otherwise(0))\
  .withColumn( 'VL_DIAS_AFASTAMENTO3', 
              when(col('CD_CAUSA_AFASTAMENTO3').isin(['10','20','30','40','99']), datediff(col("DATA_FIM_AFAST_3"),col("DATA_INICIO_AFAST_3"))).otherwise(0))\
  .withColumn('VL_DIAS_AFASTAMENTO1',f.when(f.col('VL_DIAS_AFASTAMENTO1').isNull(),f.lit(0))
              .otherwise(f.col('VL_DIAS_AFASTAMENTO1')))\
  .withColumn('VL_DIAS_AFASTAMENTO2',f.when(f.col('VL_DIAS_AFASTAMENTO2').isNull(),f.lit(0))
              .otherwise(f.col('VL_DIAS_AFASTAMENTO2')))\
  .withColumn('VL_DIAS_AFASTAMENTO3',f.when(f.col('VL_DIAS_AFASTAMENTO3').isNull(),f.lit(0))
              .otherwise(f.col('VL_DIAS_AFASTAMENTO3')))\
  .withColumn('VL_DIAS_AFASTAMENTO_TOT_CALC', f.col('VL_DIAS_AFASTAMENTO1')+f.col('VL_DIAS_AFASTAMENTO2')+f.col('VL_DIAS_AFASTAMENTO3'))
)

# COMMAND ----------

df_2008_2018_SEM_DUP_CPF_VL_AFAST.select('FL_VINCULO_ATIVO_3112','ANO','ID_CPF','ID_CNPJ_CEI','CD_CNAE20_SUBCLASSE','DATA_INICIO_AFAST_1','DATA_FIM_AFAST_1','DATA_INICIO_AFAST_2','DATA_FIM_AFAST_2','DATA_INICIO_AFAST_3','DATA_FIM_AFAST_3','VL_DIAS_AFASTAMENTO1','CD_CAUSA_AFASTAMENTO1','VL_DIAS_AFASTAMENTO2','CD_CAUSA_AFASTAMENTO2','VL_DIAS_AFASTAMENTO3','CD_CAUSA_AFASTAMENTO3','VL_DIAS_AFASTAMENTO_TOT_CALC','VL_DIAS_AFASTAMENTO')

# COMMAND ----------

df_2008_2018_SEM_DUP_CPF_VL_AFAST.display()

# COMMAND ----------

df_2008_2018_SEM_DUP_CPF_VL_AFAST = (
  df_2008_2018_SEM_DUP_CPF_VL_AFAST\
  .withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',f.lpad(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
  .withColumn('datafinal',f.concat_ws('-',f.col('ANO'),f.col('CD_MES_DESLIGAMENTO'),f.col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
  .withColumn('datafinal', regexp_replace(col('datafinal'), '\\{ñ','0'))\
  .withColumn('data_inicio',f.concat_ws('-', f.col('ANO'), f.lit('01'), f.lit('01')))\
  .withColumn('data_fim',f.concat_ws('-', f.col('ANO'), f.lit('12'), f.lit('31')))\
  .withColumn('dt_adm_corrigido', f.concat_ws('-', f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                              f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2), 
                                              f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2)))\
  .withColumn('dt_adm_corrigido', f.to_date(f.col('dt_adm_corrigido'),"yyyy-MM-dd"))\
  .withColumn('ANO_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),1,4).cast('int'))\
  .withColumn('MES_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),6,2).cast('int'))\
  .withColumn('DIAS_TRABALHADOS', datediff(col('data_fim'),col('dt_adm_corrigido')))\
  .withColumn('DIAS_TRABALHADOS', f.when( col('DIAS_TRABALHADOS') > 365, f.lit(365)).otherwise(col('DIAS_TRABALHADOS')))\
  .withColumn('FERIAS_PROPORCIONAIS', f.round((f.col('DIAS_TRABALHADOS')/365)*30, 2))\
  .withColumn('DIAS_ESPERADOS_TRABALHADOS', f.round(f.col('DIAS_TRABALHADOS')-f.col('FERIAS_PROPORCIONAIS'),2))
)

# COMMAND ----------

df_2008_2018_SEM_DUP_CPF_VL_AFAST.select('FL_VINCULO_ATIVO_3112','ANO','ID_CPF','ID_CNPJ_CEI','CD_CNAE20_SUBCLASSE','DATA_INICIO_AFAST_1','DATA_FIM_AFAST_1','DATA_INICIO_AFAST_2','DATA_FIM_AFAST_2','DATA_INICIO_AFAST_3','DATA_FIM_AFAST_3','VL_DIAS_AFASTAMENTO1','CD_CAUSA_AFASTAMENTO1','VL_DIAS_AFASTAMENTO2','CD_CAUSA_AFASTAMENTO2','VL_DIAS_AFASTAMENTO3','CD_CAUSA_AFASTAMENTO3','VL_DIAS_AFASTAMENTO_TOT_CALC','VL_DIAS_AFASTAMENTO','data_inicio','data_fim','dt_adm_corrigido','datafinal','DIAS_TRABALHADOS','FERIAS_PROPORCIONAIS','DIAS_ESPERADOS_TRABALHADOS').display()

# COMMAND ----------

ANO = (df_2008_2018_SEM_DUP_CPF_VL_AFAST
       .groupby('ANO')
       .agg(f.sum(f.col('VL_DIAS_AFASTAMENTO_TOT_CALC')).alias('Total_dias_afastamento'),
            f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('Total_dias_trabalhados_esp'),
            f.count('ID_CPF'))
       .withColumn('INDICE_ABSENTEÍSMO', f.round(f.col('Total_dias_afastamento')/f.col('Total_dias_trabalhados_esp')*100,2))
       .orderBy('ANO'))

# COMMAND ----------

ANO.display()

# COMMAND ----------

ANO.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2010_2011
# MAGIC
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/f03c15/000000?text=+) ` `
# MAGIC  
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/1589F0/000000?text=+) ` `

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
trs_rais_vinculo2010a2011_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo_2010_2011 = (spark.read.parquet(trs_rais_vinculo2010a2011_path)
                            .where((f.col('ANO') >= f.lit(2010)) & (f.col('ANO') <= f.lit(2011))))

#df_2018 = df_rais_vinculo_2012_2018.filter(col('ANO') == 2010

# COMMAND ----------

df_2010_2011 = (
  df_rais_vinculo_2010_2011\
  .filter(col('FL_VINCULO_ATIVO_3112') == '1')\
  .withColumn('ID_CPF', lpad(col('ID_CPF'),11,'0'))\
  .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0'))\
  .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))\
)

df_2010_2011_group_by = (
  df_2010_2011\
  .groupBy('ID_CPF')\
  .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO'))\
  .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2010_2011.select('ID_CPF').count())\
  .orderBy('CPF_DUPLICADO', ascending=False)
)

df_2010_2011_SEM_DUP_CPF = df_2010_2011.join(df_2010_2011_group_by, 'ID_CPF', how='right')

# COMMAND ----------

# MAGIC %md
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/c5f015/000000?text=+) ` `
# MAGIC - Filter 2010
# MAGIC - mês de afastamento está como 'null'

# COMMAND ----------

df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM = (df_2010_2011_SEM_DUP_CPF.where(f.col('ANO') == f.lit(2010))\
                                            .withColumn('CD_CNAE20_DIVISAO',f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))\
                                            .filter(col('FL_VINCULO_ATIVO_3112') == '1')\
                                            .filter( (col('CD_CAUSA_AFASTAMENTO1').isin(['10','20','30','40','99']) & (col('CD_CAUSA_AFASTAMENTO2').isin(['10','20','30','40','99']) & (col('CD_CAUSA_AFASTAMENTO3').isin(['10','20','30','40','99']) )))))

# COMMAND ----------

df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM.display()

# COMMAND ----------

df_2010 = (
  df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM.withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',f.lpad(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
  .withColumn('datafinal',f.concat_ws('-',f.col('ANO'),f.col('CD_MES_DESLIGAMENTO'),f.col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
  .withColumn('datafinal', regexp_replace(col('datafinal'), '\\{ñ','0'))\
  .withColumn('data_inicio',f.concat_ws('-', f.col('ANO'), f.lit('01'), f.lit('01')))\
  .withColumn('data_fim',f.concat_ws('-', f.col('ANO'), f.lit('12'), f.lit('31')))\
  .withColumn('dt_adm_corrigido', f.concat_ws('-', f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4), 
                                              f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2),
                                              f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2)))\
  .withColumn('dt_adm_corrigido', f.to_date(f.col('dt_adm_corrigido'),"yyyy-MM-dd"))\
  .withColumn('ANO_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),1,4).cast('int'))\
  .withColumn('MES_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),6,2).cast('int'))\
  .withColumn('DIAS_TRABALHADOS', datediff(col('data_fim'),col('dt_adm_corrigido')) )\
  .withColumn('DIAS_TRABALHADOS', f.when( col('DIAS_TRABALHADOS') > 365, f.lit(365)).otherwise(col('DIAS_TRABALHADOS')))\
  .withColumn('FERIAS_PROPORCIONAIS', f.round((f.col('DIAS_TRABALHADOS')/365)*30, 2))\
  .withColumn('DIAS_ESPERADOS_TRABALHADOS', f.round(f.col('DIAS_TRABALHADOS')-f.col('FERIAS_PROPORCIONAIS'),2))
)

# COMMAND ----------

df_2010.select('FL_VINCULO_ATIVO_3112','ANO','ID_CPF','ID_CNPJ_CEI','CD_CNAE20_SUBCLASSE','VL_DIAS_AFASTAMENTO','data_inicio','data_fim','dt_adm_corrigido','datafinal','DIAS_TRABALHADOS','FERIAS_PROPORCIONAIS','DIAS_ESPERADOS_TRABALHADOS').display()

# COMMAND ----------

display(df_2010
       .groupby('ANO')
       .agg(f.sum(f.col('VL_DIAS_AFASTAMENTO')).alias('Total_dias_afastamento'),
            f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('Total_dias_trabalhados_esp'))
       .withColumn('INDICE_ABSENTEÍSMO', f.round(f.col('Total_dias_afastamento')/f.col('Total_dias_trabalhados_esp')*100,2))
       .orderBy('ANO'))

# COMMAND ----------

# MAGIC %md
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/c5f015/000000?text=+) ` `
# MAGIC - Filter 2011
# MAGIC - Variáveis de datas de afastamento estão como '99' e de motivos de afastamento

# COMMAND ----------

df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM = df_2010_2011_SEM_DUP_CPF.where(f.col('ANO') == f.lit(2011))\
                                                                          .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2)).filter(col('FL_VINCULO_ATIVO_3112') == '1')\
.filter( (col('CD_CAUSA_AFASTAMENTO1').isin(['10','20','30','40','99']) & (col('CD_CAUSA_AFASTAMENTO2').isin(['10','20','30','40','99']) & (col('CD_CAUSA_AFASTAMENTO3').isin(['10','20','30','40','99']) ))))

# COMMAND ----------

df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM.display()

# COMMAND ----------

df_2011 = (
  df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM.withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',f.lpad(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
  .withColumn('datafinal',f.concat_ws('-',f.col('ANO'),f.col('CD_MES_DESLIGAMENTO'),f.col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
  .withColumn('datafinal', regexp_replace(col('datafinal'), '\\{ñ','0'))\
  .withColumn('data_inicio',f.concat_ws('-', f.col('ANO'), f.lit('01'), f.lit('01')))\
  .withColumn('data_fim',f.concat_ws('-', f.col('ANO'), f.lit('12'), f.lit('31')))\
  .withColumn('dt_adm_corrigido', f.concat_ws('-', f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4), 
                                              f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2),
                                              f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2)))\
  .withColumn('dt_adm_corrigido', f.to_date(f.col('dt_adm_corrigido'),"yyyy-MM-dd"))\
  .withColumn('ANO_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),1,4).cast('int'))\
  .withColumn('MES_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),6,2).cast('int'))\
  .withColumn('DIAS_TRABALHADOS', datediff(col('data_fim'),col('dt_adm_corrigido')) )\
  .withColumn('DIAS_TRABALHADOS', f.when( col('DIAS_TRABALHADOS') > 365, f.lit(365)).otherwise(col('DIAS_TRABALHADOS')))\
  .withColumn('FERIAS_PROPORCIONAIS', f.round((f.col('DIAS_TRABALHADOS')/365)*30, 2))\
  .withColumn('DIAS_ESPERADOS_TRABALHADOS', f.round(f.col('DIAS_TRABALHADOS')-f.col('FERIAS_PROPORCIONAIS'),2))
)

# COMMAND ----------

df_2011.select('FL_VINCULO_ATIVO_3112','ANO','ID_CPF','ID_CNPJ_CEI','CD_CNAE20_SUBCLASSE','VL_DIAS_AFASTAMENTO','data_inicio','data_fim','dt_adm_corrigido','datafinal','DIAS_TRABALHADOS','FERIAS_PROPORCIONAIS','DIAS_ESPERADOS_TRABALHADOS').display()

# COMMAND ----------

display(df_2011
       .groupby('ANO')
       .agg(f.sum(f.col('VL_DIAS_AFASTAMENTO')).alias('Total_dias_afastamento'),
            f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('Total_dias_trabalhados_esp'))
       .withColumn('INDICE_ABSENTEÍSMO', f.round(f.col('Total_dias_afastamento')/f.col('Total_dias_trabalhados_esp')*100,2))
       .orderBy('ANO'))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Descritiva
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/f03c15/000000?text=+) ` `
# MAGIC  
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/1589F0/000000?text=+) ` `
# MAGIC  
# MAGIC  - Número médio de absent por setores por ano;
# MAGIC  - Por perfil;
# MAGIC  - Setor com os maiores afastamento e menores afastamentos relacionado com os que pagam maiores salários ou menores salários;
# MAGIC  - Dizer por que caiu ou porque subiu, tentar dizer o 'por que' dos acontecimentos;
# MAGIC  - Pessoas afastados por anos;
# MAGIC  - 'Calcular' o valor em porcentagem os motivos de afastamentos; 
# MAGIC  - Proporção de pessoas que estão afastando-se;
# MAGIC  - Mulher ou Homem afastados, por exemplo licença de afastamento;
# MAGIC  - 'Explorar' os dados!;
# MAGIC  - Insights perdas e lucros!;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Regressao Polinomial
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/f03c15/000000?text=+) ` `
# MAGIC  
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/1589F0/000000?text=+) ` `

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


# assign data of lists.
data = {'Indice_Absent': [2.63, 2.64, 2.75, 3.82, 4.41, 2.84, 2.87, 2.92, 3.05, 2.83, 2.68],
'Ano': [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]}

# Create DataFrame
dataset = pd.DataFrame(data)

# Print the output.
print(dataset)

# COMMAND ----------

# Importing the dataset
# dataset = pd.read_csv('https://s3.us-west-2.amazonaws.com/public.gamelab.fun/dataset/position_salaries.csv')
X = dataset.iloc[:, 1:2].values
#print(X)
y = dataset.iloc[:, 0].values
#print(y)

# COMMAND ----------

# Splitting the dataset into the Training set and Test set
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)



# Fitting Linear Regression to the dataset
from sklearn.linear_model import LinearRegression
lin_reg = LinearRegression()
lin_reg.fit(X, y)



# Visualizing the Linear Regression results
#def viz_linear():
plt.scatter(X, y, color='red')
plt.plot(X, lin_reg.predict(X), color='blue')
plt.title('(Linear Regression)')
plt.xlabel('Ano')
plt.ylabel('Indice')
plt.show()
#return

# COMMAND ----------

# Fitting Polynomial Regression to the dataset
from sklearn.preprocessing import PolynomialFeatures
poly_reg = PolynomialFeatures(degree=3)
X_poly = poly_reg.fit_transform(X)
pol_reg = LinearRegression()
pol_reg.fit(X_poly, y)


# Visualizing the Polymonial Regression results
def viz_polymonial():
  plt.scatter(X, y, color='red')
  plt.plot(X, pol_reg.predict(poly_reg.fit_transform(X)), color='blue')
  plt.title('(Polynomial Regression)')
  plt.xlabel('Ano')
  plt.ylabel('Indice')
  plt.show()
  return
viz_polymonial()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # CAT - CNPJ - RAIS
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/f03c15/000000?text=+) ` `
# MAGIC  
# MAGIC  ![#c5f015](https://via.placeholder.com/1500x20/1589F0/000000?text=+) ` `

# COMMAND ----------

'''
from pyspark.sql.functions import *

df = df.withColumn('hourOfDay', hour(to_timestamp(concat_ws(' ', date_format(col('Date'), 'yyyy-MM-dd'), col('Time')), 'yyyy-MM-dd HH:mm')))\
.filter((col('hourOfDay') >= lit(9)) & (col('hourOfDay') <= lit(17)))\.drop('hourOfDay')
df.select(df.name, df.age.between(2, 4)).show()
+-----+---------------------------+
| name|((age >= 2) AND (age <= 4))|
+-----+---------------------------+
|Alice|                       true|
|  Bob|                      false|
+-----+---------------------------+
'''

# COMMAND ----------

"""
ANALISE DOS ANOS ENTRE 2008 E 2010

df_rais_vinculo2008a2018.select('ANO').cache().distinct().collect()
Out[11]: [Row(ANO=2009), Row(ANO=2010), Row(ANO=2008)]

# .repartition(10)

where((f.col('ANO') >= f.lit(2008)) & (f.col('ANO') <= f.lit(2018)))
"""

'''
df_rais_vinculo_2008_2009.select('ANO', 'CD_CNAE20_SUBCLASSE','CD_CNAE20_DIVISAO', 'DATA_INICIO_AFAST_1', 'DATA_FIM_AFAST_1', 'DATA_INICIO_AFAST_2','DATA_FIM_AFAST_2','DATA_INICIO_AFAST_3', 'DATA_FIM_AFAST_3').limit(3).display()
'''

# COMMAND ----------

from pyspark.sql.functions import *

df_rais_vinculo_2008_2009 = (df_rais_vinculo_2008_2009.withColumn('ID_CPF', lpad(col('ID_CPF'), 11, '0'))\
                            .withColumn('ID_CNPJS_CEI', lpad(col('ID_CNPJ_CEI'), 14, '0'))\
                            .withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',lpad(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
                            .withColumn('datafinal', concat_ws('-', col('ANO'), col('CD_MES_DESLIGAMENTO'), col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
                            .withColumn('data_inicio',concat_ws('-', col('ANO'), lit('01'), lit('01')))\
                            .withColumn('data_fim', concat_ws('-', col('ANO'), lit('12'), lit('31')))\
                            .withColumn('dt_adm', concat_ws('-', substring( col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                                               substring(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2),
                                                               substring(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2))
                                       ))\
                                        .withColumn('DATA_DESLIGAMENTO',f.when(f.col('FL_VINCULO_ATIVO_3112')==f.lit(0),f.col('datafinal')))\
                                        .withColumn('DATA_DESLIGAMENTO', f.to_date(f.col('DATA_DESLIGAMENTO'),"yyyy-MM-dd"))\
                                        .withColumn('ANO_ADMISSAO',f.substring(f.col('dt_adm'),1,4).cast('int'))\
                                        .withColumn('MES_ADMISSAO',f.substring(f.col('dt_adm'),6,2).cast('int'))\
                                        .withColumn('ANO_DESLIGAMENTO',f.substring(f.col('DATA_DESLIGAMENTO'),1,4).cast('int'))





from pyspark.sql.functions import datediff,col

df_rais_vinculo_2008_2009 = (df_rais_vinculo_2008_2009
                            .withColumn('DIAS_TRABALHADOS', datediff(col("DATA_DESLIGAMENTO"),col("dt_adm")))
                            .withColumn('DIAS_TRABALHADOS_1',f.when(f.col('ANO_ADMISSAO') == f.col('ANO_DESLIGAMENTO'), datediff(col("DATA_DESLIGAMENTO"),col("dt_adm")))
                                        .otherwise(
                                          f.when(f.col('DIAS_TRABALHADOS') > f.lit(365), datediff(col("DATA_DESLIGAMENTO"),col("data_inicio")))
                                        .otherwise(
                                          f.when((f.col('DIAS_TRABALHADOS').isNull()) & (f.col('ANO_ADMISSAO')==f.col('ANO')), datediff(col('data_fim'),col('dt_adm')))
                                        .otherwise(
                                          f.when(f.col('DIAS_TRABALHADOS').isNull(), f.lit(365))
                                        .otherwise(
                                          f.when(f.col('ANO_DESLIGAMENTO')==f.col('ANO'), datediff(col('DATA_DESLIGAMENTO'),col('data_inicio')))))))))
display(df_rais_vinculo2008a2018)

df_rais_vinculo_2008_2009.select('ANO', 
                                 'CD_CNAE20_SUBCLASSE',
                                 'CD_CNAE20_DIVISAO',
                                 'DATA_INICIO_AFAST_1',
                                 'DATA_FIM_AFAST_1',
                                 'DATA_INICIO_AFAST_2',
                                 'DATA_FIM_AFAST_2',
                                 'DATA_INICIO_AFAST_3',
                                 'DATA_FIM_AFAST_3',
                                 'ID_CNPJS_CEI',
                                 'DT_DIA_MES_ANO_DATA_ADMISSAO',
                                 'datafinal',
                                 'data_inicio',
                                 'data_fim',
                                 'dt_adm',
                                 'DATA_DESLIGAMENTO',
                                 'ANO_DESLIGAMENTO',
                                 'NR_MES_TEMPO_EMPREGO',
                                 'DIAS_TRABALHADOS',
                                 'ANO_ADMISSAO',
                                 'DIAS_TRABALHADOS_1'
                                ).filter(col ('DATA_DESLIGAMENTO').isNotNull()).limit(500).display()

# df_rais_vinculo_2008_2009.limit(5).display()

#ef user_defined_timestamp(date_col):
# s = datetime.datetime.strptime(date_col, "%d%m%Y").date()
# return 
#er_defined_timestamp_udf = f.udf(user_defined_timestamp, f.StringType())
#
#
#f_rais_vinculo_2008_2009 = df_rais_vinculo_2008_2009.withColumn("dt_adm", user_defined_timestamp_udf(col('DT_DIA_MES_ANO_DATA_ADMISSAO')))
#f_rais_vinculo_2008_2009

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------




.select(col("DT_DIA_MES_ANO_DATA_ADMISSAO"),to_date(col("DT_DIA_MES_ANO_DATA_ADMISSAO"),"yyyy-MM-dd").alias("dt_adm")).show() 


df = df.withColumn('new_date', user_defined_timestamp_udf('Date_col'))
df.show()

# COMMAND ----------

# importing the datetime library
import datetime
 
# Generating dates using various string input formats
print(datetime.datetime.strptime('22-April-2020', '%d-%B-%Y'))
print(datetime.datetime.strptime('22-04-2020', '%d-%m-%Y'))
print(datetime.datetime.strptime('04/22/2020', '%m/%d/%Y'))
print(datetime.datetime.strptime('Mar/22/2020', '%b/%d/%Y'))

# COMMAND ----------

def user_defined_timestamp(date_col):
    _date = datetime.strptime(date_col, '%Y-%m-%d')
    return _date.strftime('%d%m%y')

# COMMAND ----------

def user_defined_timestamp(date_col):
    _date = datetime.strptime(date_col, '%d%m%Y')
    return _date.strptime('%m%d%Y')

# COMMAND ----------


from datetime import datetime

timestamp = 17022009
date_time = datetime.fromtimestamp(timestamp)

print("Date time object:", date_time)

d = date_time.strftime("%m/%d/%Y")
print("Output 2:", d)	

# COMMAND ----------

import datetime

today = datetime.datetime('17022009')
today.strftime('%Y%m%d')

# COMMAND ----------

user_defined_timestamp('17022009')

# COMMAND ----------

s = datetime.datetime.strptime("17022009", "%d%m%Y")
print(s)

# COMMAND ----------

from datetime import datetime
date_string = '17022009'
date_string.datetime.strftime('%dd%mm%yyyy')
#datetime = datetime.strptime(date_string, '%MM%dd%yyyy')
#print(datetime)
# Returns: 2021-12-31 00:00:00

# COMMAND ----------

df_rais_vinculo_2008_2009.select('ANO', 'CD_CNAE20_DIVISAO', 'DATA_INICIO_AFAST_1', 'DATA_FIM_AFAST_1', 'DATA_INICIO_AFAST_2','DATA_FIM_AFAST_2','DATA_INICIO_AFAST_3', 'DATA_FIM_AFAST_3','ID_CNPJS_CEI','DT_DIA_MES_ANO_DATA_ADMISSAO','CD_MES_DESLIGAMENTO','DT_DIA_MES_ANO_DIA_DESLIGAMENTO','datafinal','data_inicio','data_fim','NR_MES_TEMPO_EMPREGO').limit(3).display()

# COMMAND ----------

df.select(col("input"),to_date(col("input"),"MM-dd-yyyy").alias("date")) \.show()

# COMMAND ----------

df_rais_vinculo_2008_2009.filter(col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains("/") | (col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains("*"))).display()

# COMMAND ----------

                             
  from pyspark.sql.functions import *

df_rais_vinculo_2008_2010 = (df_rais_vinculo_2008_2010.withColumn('ID_CPF', lpad(col('ID_CPF'), 11, '0'))\
                            .withColumn('ID_CNPJS_CEI', lpad(col('ID_CNPJ_CEI'), 14, '0'))\
                             
                            .withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',lpad(col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
                             
                            .withColumn('datafinal', concat_ws('-', col('ANO'), col('CD_MES_DESLIGAMENTO'), col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
                             
                            .withColumn('data_inicio',concat_ws('-', col('ANO'), lit('01'), lit('01')))\
                            .withColumn('data_fim', concat_ws('-', col('ANO'), lit('12'), lit('31')))
  
  
                             
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

# COMMAND ----------

df = spark.createDataFrame([('2018-07-12',)], ['Date_col'])
df.show()

from datetime import datetime
import pyspark.sql.types as T
import pyspark.sql.functions as F


def user_defined_timestamp(date_col):
    _date = datetime.strptime(date_col, '%Y-%m-%d')
    return _date.strftime('%d%m%y')



df = df.withColumn('new_date', user_defined_timestamp_udf('Date_col'))
df.show()

# COMMAND ----------

