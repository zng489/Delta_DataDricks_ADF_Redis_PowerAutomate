# Databricks notebook source
# %md
# ![#c5f015](https://via.placeholder.com/1500x20/FFFF00/000000?text=+)
# <h2> Projeção do absenteísmo e corrigindo valores </h2>
# 
# <ul>
#   <li> 1 - calcular indice de absenteísmo por <strong> ANO </strong> ; </li>
#   <li> 2 - calcular indice de absenteísmo por <strong> CD_UF </strong> ; </li>
#   <li> 3 - calcular indice de absenteísmo por <strong> CD_CNAE20_DIVISAO </strong> ; </li>
#   <li> 4 - caclular indice de absenteísmo por <strong> COD_CBO4 </strong> ; </li>
# </ul>

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# Importing functions
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

# COMMAND ----------

# MAGIC %md
# MAGIC ![#c5f015](https://via.placeholder.com/1500x20/FFFF00/000000?text=+)

# COMMAND ----------

def Rais_2008_2018():
  
  # Importando arquivo cbo_classificacoes.csv
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  cbo_classificacoes_path = '{uri}/uds/uniepro/cbo_classificacoes/'.format(uri=var_adls_uri)
  cbo_classificacoes = spark.read.format("csv").option("header","true").option('sep',';').load(cbo_classificacoes_path)
  cbo_classificacoes = cbo_classificacoes.withColumn('COD_CBO4', lpad(col('COD_CBO4'),4,'0'))
  cbo_classificacoes = cbo_classificacoes.dropDuplicates(['COD_CBO4'])
  cbo_classificacoes = cbo_classificacoes.select(['COD_CBO4','DESC_CBO4'])
  
  # Importando arquivo cnae_industrial.parquet
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  raw_cnae_industrial_path = '{uri}/raw/usr/uniepro/cnae_industrial/'.format(uri=var_adls_uri)
  raw_cnae_industrial = spark.read.format("parquet").option("header","true").load(raw_cnae_industrial_path)
  raw_cnae_industrial = raw_cnae_industrial.select('divisao','denominacao','agrupamento') 
  raw_cnae_industrial  = raw_cnae_industrial.withColumn('divisao', lpad(col('divisao'),2,'0'))
  
  # Importando arquivo rais_vinculo.parquet
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  trs_rais_vinculo2008a2018_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
  df_rais_vinculo2008a2018 = spark.read.format("parquet").option("header","true").load(trs_rais_vinculo2008a2018_path)
  
  # Selecionando anos 2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018 da Rais 
  ANOS = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018] 
  df_2008_2018 = df_rais_vinculo2008a2018
  df_2008_2018 = df_2008_2018.where(col('ANO').isin(ANOS))
  
  # Condições estabelecidos:
  #### .filter(col('FL_VINCULO_ATIVO_3112') == '1') ======>  Selecioando apenas vinculos 1
  #### .withColumn('ID_CPF', lpad(col('ID_CPF'),11,'0')) ======> Corrigindo os zeros (11) a esquerda nos CPF's
  #### .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0')) ======> Corrigindo os zeros (14) a esquerda nos CPF's
  #### .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2)) ======> Selecionado os dois primeiros digítos da CNAE
  df_2008_2018 = (
    df_2008_2018\
    .filter(col('FL_VINCULO_ATIVO_3112') == '1')\
    .withColumn('ID_CPF', lpad(col('ID_CPF'),11,'0'))\
    .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0'))\
    .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))\
  )
   
  
  # Condições estabelecidos:
  #### .groupby('ID_CPF')
  #### .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO')) =====> Contando CPF`s duplicados
  #### .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2008_2018.select('ID_CPF').count()) =====> Porcentagem de CPF`s duplicados
  df_2008_2018_group_by = (
    df_2008_2018\
    .groupBy('ID_CPF')\
    .agg(countDistinct('ID_CNPJ_CEI').alias('CPF_DUPLICADO'))\
    .withColumn('%_DE_CPF_DUPLICADOS', col('CPF_DUPLICADO')/df_2008_2018.select('ID_CPF').count())
  )
    
  # JOIN entre" df_2008_2018" e "df_2008_2018_group_by"
  df_2008_2018_SEM_DUP_CPF = df_2008_2018.join(df_2008_2018_group_by, 'ID_CPF', how='right')
  
  # JOIN entre "df_2008_2018_SEM_DUP_CPF" e "cbo_classificacoes"
  df_2008_2018_SEM_DUP_CPF_cbo_classificacoes = df_2008_2018_SEM_DUP_CPF.join(cbo_classificacoes, df_2008_2018_SEM_DUP_CPF.CD_CBO4 == cbo_classificacoes.COD_CBO4, how='left')
  
  # JOIN ENTRE A TABELA "df_2008_2018_SEM_DUP_CPF_cbo_classificacoes" e "raw_cnae_industrial"
  df_2008_2018_SEM_DUP_CPF_cbo_classificacoes = df_2008_2018_SEM_DUP_CPF_cbo_classificacoes.join(raw_cnae_industrial, df_2008_2018_SEM_DUP_CPF_cbo_classificacoes.CD_CNAE20_DIVISAO == raw_cnae_industrial.divisao, how='left')

  return df_2008_2018_SEM_DUP_CPF_cbo_classificacoes

# COMMAND ----------

# MAGIC %md
# MAGIC ![#c5f015](https://via.placeholder.com/1500x20/FFFF00/000000?text=+)

# COMMAND ----------

# Bases de 2008_2018

# COMMAND ----------

# 2008-2018 (SEM OS ANOS 2010 e 2011) utilizando condições CD_CAUSA_AFASTAMENTO_1_2_3').isin(['10','20','30','40','99']) para o cálculo de VL_AFAST

def CD_CAUSA_AFASTAMENTO_08_18_NAO_INCLUINDO_10_11(x):

  # Onde df_2008_2018_SEM_DUP_CPF_cbo_classificacoes = Rais_2008_2018()
  ### x = ['Indústria','Não indústria']
  df_2008_2018 = Rais_2008_2018().filter(col('agrupamento').isin(x)).filter(col('ANO').isin([2008,2009,2012,2013,2014,2015,2016,2017,2018]))
  
  df_2008_2018_SEM_DUP_CPF_DATA_INICIO_FIM = (
    df_2008_2018\
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

  
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = (
    df_2008_2018_SEM_DUP_CPF_DATA_INICIO_FIM\
                                       .withColumn(
    'VL_DIAS_AFASTAMENTO1', f.when(f.col('DATA_FIM_AFAST_1').isNull() & f.col('DATA_INICIO_AFAST_1').isNull(), 0)\
      .otherwise(
      f.when(f.col('DATA_FIM_AFAST_1') == f.col('DATA_INICIO_AFAST_1'), 1).otherwise(
        f.when(f.col('CD_CAUSA_AFASTAMENTO1').isin(['10','20','30','40','99']), datediff(col("DATA_FIM_AFAST_1"),col("DATA_INICIO_AFAST_1"))).otherwise(0)
          ))   
  )
    )
  
  
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = (
    df_2008_2018_SEM_DUP_CPF_VL_AFAST\
                                       .withColumn(
    'VL_DIAS_AFASTAMENTO2', f.when(f.col('DATA_FIM_AFAST_2').isNull() & f.col('DATA_INICIO_AFAST_2').isNull(), 0)\
      .otherwise(
      f.when(f.col('DATA_FIM_AFAST_2') == f.col('DATA_INICIO_AFAST_2'), 1).otherwise(
        f.when(f.col('CD_CAUSA_AFASTAMENTO2').isin(['10','20','30','40','99']), datediff(col("DATA_FIM_AFAST_2"),col("DATA_INICIO_AFAST_2"))).otherwise(0)
          ))   
  )
    )
  
  
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = (
    df_2008_2018_SEM_DUP_CPF_VL_AFAST\
                                       .withColumn(
    'VL_DIAS_AFASTAMENTO3', f.when(f.col('DATA_FIM_AFAST_3').isNull() & f.col('DATA_INICIO_AFAST_3').isNull(), 0)\
      .otherwise(
      f.when(f.col('DATA_FIM_AFAST_3') == f.col('DATA_INICIO_AFAST_3'), 1).otherwise(
        f.when(f.col('CD_CAUSA_AFASTAMENTO3').isin(['10','20','30','40','99']), datediff(col("DATA_FIM_AFAST_3"),col("DATA_INICIO_AFAST_3"))).otherwise(0)
          ))   
  )
    )
  
  
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = (
    df_2008_2018_SEM_DUP_CPF_VL_AFAST.withColumn('VL_DIAS_AFASTAMENTO_TOT_CALC', f.col('VL_DIAS_AFASTAMENTO1')+f.col('VL_DIAS_AFASTAMENTO2')+f.col('VL_DIAS_AFASTAMENTO3'))
  )

  
  
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = (
    df_2008_2018_SEM_DUP_CPF_VL_AFAST\
    .withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',f.lpad(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
    .withColumn('datafinal',f.concat_ws('-',f.col('ANO'),f.col('CD_MES_DESLIGAMENTO'),f.col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
    .withColumn('datafinal', regexp_replace(col('datafinal'), '\\{ñ','0'))\
    .withColumn('data_inicio',f.concat_ws('-', f.col('ANO'), f.lit('01'), f.lit('01')))\
    .withColumn('data_fim',f.concat_ws('-', f.col('ANO'), f.lit('12'), f.lit('31')))\
    .withColumn('dt_adm_corrigido', f.when(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains('/') | f.col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains('*'),
                                           f.expr("add_months(data_fim,-NR_MES_TEMPO_EMPREGO)")).otherwise(f.concat_ws('-', f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                                                                                                        f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2), 
                                                                                                                        f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2))))\
  .withColumn('dt_adm_corrigido', f.to_date(f.col('dt_adm_corrigido'),"yyyy-MM-dd"))\
  .withColumn('ANO_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),1,4).cast('int'))\
  .withColumn('MES_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),6,2).cast('int'))\
  .withColumn('DIAS_TRABALHADOS', datediff(col('data_fim'),col('dt_adm_corrigido')))\
  .withColumn('DIAS_TRABALHADOS', f.when( col('DIAS_TRABALHADOS') > 365, f.lit(365)).otherwise(col('DIAS_TRABALHADOS')))\
  .withColumn('FERIAS_PROPORCIONAIS', f.round((f.col('DIAS_TRABALHADOS')/365)*30, 0))\
  .withColumn('DIAS_ESPERADOS_TRABALHADOS', f.round(f.col('DIAS_TRABALHADOS')-f.col('FERIAS_PROPORCIONAIS'),0)))\
  .withColumn('VL_DIAS_AFASTAMENTO_TOT_CALC', f.when( f.col('VL_DIAS_AFASTAMENTO_TOT_CALC') > 335, f.lit(335)).otherwise(col('VL_DIAS_AFASTAMENTO_TOT_CALC')))
  
  #  \ 335 por 365
  #  .withColumn(
  #    'VL_DIAS_AFASTAMENTO_TOT_CALC', f.when( f.col('VL_DIAS_AFASTAMENTO_TOT_CALC') > 335, f.lit(335)).otherwise(col('VL_DIAS_AFASTAMENTO_TOT_CALC')))
  #)
  
  
  # Criando tabela uma tabela unica com os maiores valores
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = df_2008_2018_SEM_DUP_CPF_VL_AFAST.withColumn('VL_MEDIA_NOM_DEZEMBRO_NOM', greatest('VL_REMUN_MEDIA_NOM', 'VL_REMUN_DEZEMBRO_NOM'))
  
  
  # VL_DIAS_AFASTAMENTO_TOTC_CALC MENORES DO QUE 15 DIAS
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = df_2008_2018_SEM_DUP_CPF_VL_AFAST.withColumn('VL_DIAS_MENORES_15_CALC', f.when( 
    col('VL_DIAS_AFASTAMENTO_TOT_CALC') <= 15, f.lit(col('VL_DIAS_AFASTAMENTO_TOT_CALC'))).otherwise('N_SELECT') ).withColumn(
    'CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC', f.when( f.col('DIAS_ESPERADOS_TRABALHADOS') < f.col('VL_DIAS_AFASTAMENTO_TOT_CALC'), f.lit(col('DIAS_ESPERADOS_TRABALHADOS'))   ).otherwise(
    f.lit(col('VL_DIAS_AFASTAMENTO_TOT_CALC'))))
  
  columns = ['ID_CPF',
 'CD_MUNICIPIO',
 'FL_VINCULO_ATIVO_3112',
 'DT_DIA_MES_ANO_DATA_ADMISSAO',
 'DT_DIA_MES_ANO_DIA_DESLIGAMENTO',
 'CD_CNAE20_DIVISAO',
 'CD_CBO4',
 'ANO',
 'CD_UF',
 'CPF_DUPLICADO',
 '%_DE_CPF_DUPLICADOS',
 'COD_CBO4',
 'DESC_CBO4',
 'divisao',
 'denominacao',
 'agrupamento',
 'VL_DIAS_AFASTAMENTO_TOT_CALC',
 'data_fim',
 'dt_adm_corrigido',
 'DIAS_TRABALHADOS',
 'FERIAS_PROPORCIONAIS',
 'DIAS_ESPERADOS_TRABALHADOS',
 'VL_MEDIA_NOM_DEZEMBRO_NOM',
 'VL_DIAS_MENORES_15_CALC',
 'CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC']
  
  df_2008_2018_SEM_DUP_CPF_VL_AFAST = df_2008_2018_SEM_DUP_CPF_VL_AFAST.select(*columns)
  
  return df_2008_2018_SEM_DUP_CPF_VL_AFAST

# COMMAND ----------

# MAGIC %md
# MAGIC ![#c5f015](https://via.placeholder.com/1500x20/FFFF00/000000?text=+)

# COMMAND ----------

# Bases de 2010_2011

# COMMAND ----------

# 2010-2011 com condições CD_CAUSA_AFASTAMENTO_1_2_3').isin(['10','20','30','40','99'])

def CD_CAUSA_AFASTAMENTO_10_11(y):
  
  # df_2010_2011 = df_2008_2018_SEM_DUP_CPF_cbo_classificacoes.filter(col('ANO').isin([2010,2011]))
  #### == f.lit(2010)
  #### y = ['Indústria','Não indústria']
  
  df_2008_2018_SEM_DUP_CPF_cbo_classificacoes = Rais_2008_2018().filter(col('agrupamento').isin(y))
  df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM = (df_2008_2018_SEM_DUP_CPF_cbo_classificacoes.where(f.col('ANO').isin([2010,2011]))\
                                              .withColumn('CD_CNAE20_DIVISAO',f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))\
                                              .filter(col('FL_VINCULO_ATIVO_3112') == '1')\
                                              .filter( (col('CD_CAUSA_AFASTAMENTO1').isin(['10','20','30','40','99']) & (col('CD_CAUSA_AFASTAMENTO2').isin(['10','20','30','40','99']) & (col('CD_CAUSA_AFASTAMENTO3').isin(['10','20','30','40','99']) )))))
  
  
  #Filter 2010
  #mês de afastamento está como 'null'
  
  #Filter 2011
  #Variáveis de datas de afastamento estão como '99' e de motivos de afastamento
  
  ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST = (
    df_2010_2011_SEM_DUP_CPF_DATA_INICIO_FIM\
    .withColumn('DT_DIA_MES_ANO_DATA_ADMISSAO',f.lpad(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),8,'0'))\
    .withColumn('datafinal',f.concat_ws('-',f.col('ANO'),f.col('CD_MES_DESLIGAMENTO'),f.col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')))\
    .withColumn('datafinal', regexp_replace(col('datafinal'), '\\{ñ','0'))\
    .withColumn('data_inicio',f.concat_ws('-', f.col('ANO'), f.lit('01'), f.lit('01')))\
    .withColumn('data_fim',f.concat_ws('-', f.col('ANO'), f.lit('12'), f.lit('31')))\
    .withColumn('dt_adm_corrigido', f.when(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains('/') | f.col('DT_DIA_MES_ANO_DATA_ADMISSAO').contains('*'),
                                           f.expr("add_months(data_fim,-NR_MES_TEMPO_EMPREGO)")).otherwise(f.concat_ws('-', f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                                                                                                        f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2), 
                                                                                                                        f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2))))\
  .withColumn('dt_adm_corrigido', f.to_date(f.col('dt_adm_corrigido'),"yyyy-MM-dd"))\
  .withColumn('ANO_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),1,4).cast('int'))\
  .withColumn('MES_ADMISSAO',f.substring(f.col('dt_adm_corrigido'),6,2).cast('int'))\
  .withColumn('DIAS_TRABALHADOS', datediff(col('data_fim'),col('dt_adm_corrigido')))\
  .withColumn('DIAS_TRABALHADOS', f.when( col('DIAS_TRABALHADOS') > 365, f.lit(365)).otherwise(col('DIAS_TRABALHADOS')))\
  .withColumn('FERIAS_PROPORCIONAIS', f.round((f.col('DIAS_TRABALHADOS')/365)*30, 0))\
  .withColumn('DIAS_ESPERADOS_TRABALHADOS', f.round(f.col('DIAS_TRABALHADOS')-f.col('FERIAS_PROPORCIONAIS'),0)))
  
  
  # Criando tabela uma tabela unica com os maiores valores
  ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST = ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST.withColumn('VL_MEDIA_NOM_DEZEMBRO_NOM', greatest('VL_REMUN_MEDIA_NOM', 'VL_REMUN_DEZEMBRO_NOM'))
  
  # VL_DIAS_AFASTAMENTO_TOTC_CALC MENORES DO QUE 15 DIAS
  ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST = ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST.withColumn('VL_DIAS_MENORES_15_CALC', f.when( 
    col('VL_DIAS_AFASTAMENTO') <= 15, f.lit(col('VL_DIAS_AFASTAMENTO'))).otherwise('N_SELECT') ).withColumnRenamed('VL_DIAS_AFASTAMENTO', 'VL_DIAS_AFASTAMENTO_TOT_CALC')\
  .withColumn('VL_DIAS_AFASTAMENTO_TOT_CALC', f.when( f.col('VL_DIAS_AFASTAMENTO_TOT_CALC') > 335, f.lit(335)).otherwise(col('VL_DIAS_AFASTAMENTO_TOT_CALC')))\
  .withColumn(
    'CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC', f.when( f.col('DIAS_ESPERADOS_TRABALHADOS') < f.col('VL_DIAS_AFASTAMENTO_TOT_CALC'), f.lit(col('DIAS_ESPERADOS_TRABALHADOS'))   ).otherwise(
    f.lit(col('VL_DIAS_AFASTAMENTO_TOT_CALC'))))

  #.withColumn('DIAS_TRABALHADOS', f.when( col('DIAS_TRABALHADOS') > 365, f.lit(365)).otherwise(col('DIAS_TRABALHADOS')))\
  
  columns = ['ID_CPF',
 'CD_MUNICIPIO',
 'FL_VINCULO_ATIVO_3112',
 'DT_DIA_MES_ANO_DATA_ADMISSAO',
 'DT_DIA_MES_ANO_DIA_DESLIGAMENTO',
 'CD_CNAE20_DIVISAO',
 'CD_CBO4',
 'ANO',
 'CD_UF',
 'CPF_DUPLICADO',
 '%_DE_CPF_DUPLICADOS',
 'COD_CBO4',
 'DESC_CBO4',
 'divisao',
 'denominacao',
 'agrupamento',
 'VL_DIAS_AFASTAMENTO_TOT_CALC',
 'data_fim',
 'dt_adm_corrigido',
 'DIAS_TRABALHADOS',
 'FERIAS_PROPORCIONAIS',
 'DIAS_ESPERADOS_TRABALHADOS',
 'VL_MEDIA_NOM_DEZEMBRO_NOM',
 'VL_DIAS_MENORES_15_CALC',
 'CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC']
  
  ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST = ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST.select(*columns)

  return ANOS_2010_2011_SEM_DUP_CPF_VL_AFAST 

# COMMAND ----------

# MAGIC %md
# MAGIC ![#c5f015](https://via.placeholder.com/1500x20/FFFF00/000000?text=+)

# COMMAND ----------

# Concatenate das bases de 2008_2018 com 2010_2011

def Concatenate_DataFrames_2008_2018(x):
  return CD_CAUSA_AFASTAMENTO_08_18_NAO_INCLUINDO_10_11(x).union(CD_CAUSA_AFASTAMENTO_10_11(x))

# COMMAND ----------

def absent_custo(x):
  
  '''
  Para o calculo do absenteísmo com custos diretos:
  01 - custo afastamento de dia trabalhado;
  02 - custo assistência em saúde;
  03 - custo direto absenteísmo1 ("custo afastamento de dia trabalhado" + "custo assistência em saúde");
  04 - custo direto absenteísmo2 ("custo afastamento de dia trabalhado" x "2");
  05 - custo direto absenteismo3 ("custo afastamento de dia trabalhado" x "2" + "custo assistência em saúde");
  '''
  
  absent_custo = Concatenate_DataFrames_2008_2018(x).filter(col('VL_DIAS_MENORES_15_CALC') != 'N_SELECT')\
.withColumn('Valor_Dia_Trab', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM')/22, 3))\
.withColumn('Custo_Afast_Dia_Trab', f.round( f.col('Valor_Dia_Trab') * f.col('VL_DIAS_MENORES_15_CALC')))\
.withColumn('Custo_Assist_Saude', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM') * 0.2))\
.withColumn('Custo_Direto_Absent', 
            f.when (
              f.col('VL_DIAS_MENORES_15_CALC') == 0,0
            )\
.otherwise(
              f.round( f.col('Custo_Assist_Saude') + f.col('Custo_Afast_Dia_Trab'))
            )
           ).withColumn('Custo_Direto_Absent_2', 2 * ( f.col('Custo_Direto_Absent')) )\
  .withColumn('Custo_Direto_Absent_3', ( 2 * ( f.col('Custo_Direto_Absent'))) + f.col('Custo_Assist_Saude') )
  
  return absent_custo

# COMMAND ----------

# MAGIC %md
# MAGIC ![#c5f015](https://via.placeholder.com/1500x20/FFFF00/000000?text=+)

# COMMAND ----------

# def Indice_Absenteismo_ANO_2008_2018(x,*args): ==> NAO ME PERMITIU USAR
def Indice_Absenteismo_ANO_2008_2018(x,**kwargs):
  #  kwargs.get('ANO'), kwargs.get('CD_UF'), kwargs.get('CD_CNAE20_DIVISAO')
  
  if (kwargs.get('CD_UF') is None) and (kwargs.get('CD_CNAE20_DIVISAO') is None) and (kwargs.get('CD_CBO4') is None):
    return (
      (
        Concatenate_DataFrames_2008_2018(x)
           .groupby('ANO')       
           .agg(
             f.sum(f.col('CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC')).alias('Total_dias_afastamento'),
             f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('Total_dias_trabalhados_esp'),
             f.count('ID_CPF')
               )
           .withColumn('INDICE_ABSENTEÍSMO', f.round(f.col('Total_dias_afastamento')/f.col('Total_dias_trabalhados_esp')*100,2))
           )
    )\
    .withColumnRenamed('count(ID_CPF)','COUNT_ID_CPF')
    
  elif (kwargs.get('CD_CNAE20_DIVISAO') is None) and (kwargs.get('CD_CBO4') is None):
    return ((Concatenate_DataFrames_2008_2018(x)
           .groupby('ANO','CD_UF')       
           .agg(f.sum(f.col('CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC')).alias('Total_dias_afastamento'),
                f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('Total_dias_trabalhados_esp'),
                f.count('ID_CPF'))
           .withColumn('INDICE_ABSENTEÍSMO', f.round(f.col('Total_dias_afastamento')/f.col('Total_dias_trabalhados_esp')*100,2))
           )).withColumnRenamed('count(ID_CPF)','COUNT_ID_CPF')
    
  elif kwargs.get('CD_CBO4') is None:
    return ((Concatenate_DataFrames_2008_2018(x)
           .groupby('ANO','CD_UF','CD_CNAE20_DIVISAO')       
           .agg(f.sum(f.col('CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC')).alias('Total_dias_afastamento'),
                f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('Total_dias_trabalhados_esp'),
                f.count('ID_CPF'))
           .withColumn('INDICE_ABSENTEÍSMO', f.round(f.col('Total_dias_afastamento')/f.col('Total_dias_trabalhados_esp')*100,2))
           )).withColumnRenamed('count(ID_CPF)','COUNT_ID_CPF')
    
  else:
    return ((Concatenate_DataFrames_2008_2018(x)
           .groupby('ANO','CD_UF','CD_CNAE20_DIVISAO','CD_CBO4')     #,'ID_CPF'  
           .agg(
             f.sum(f.col('CORRIGIDO_VL_DIAS_AFASTAMENTO_TOT_CALC')).alias('Total_dias_afastamento'),
             f.round(f.sum(f.col('DIAS_ESPERADOS_TRABALHADOS')),2).alias('Total_dias_trabalhados_esp'),
             f.count('ID_CPF')
           )
           .withColumn('INDICE_ABSENTEÍSMO', f.round(f.col('Total_dias_afastamento')/f.col('Total_dias_trabalhados_esp')*100,2))
           )).withColumnRenamed('count(ID_CPF)','COUNT_ID_CPF')

# COMMAND ----------

# %md
# ![#c5f015](https://via.placeholder.com/1500x20/FFFF00/000000?text=+)
# <h2> Projeção do absenteísmo e corrigindo valores </h2>
# 
# <ul>
#   <li> 1 - Ano e UF com custeiro direto </li>
#   <li> 2 - Ano, UF, CNAE com custeio direto </li>
#   <li> 3 - Ano, UF, CNAE e CBO com custeio direto </li>
# </ul>

# COMMAND ----------

# def Indice_Absenteismo_ANO_2008_2018(x,*args): ==> NAO ME PERMITIU USAR
def Custo_Direto_ANO_2008_2018(x,**kwargs):
  #  kwargs.get('ANO'), kwargs.get('CD_UF'), kwargs.get('CD_CNAE20_DIVISAO')
  
  if (kwargs.get('CD_UF') is None) and (kwargs.get('CD_CNAE20_DIVISAO') is None) and (kwargs.get('CD_CBO4') is None):
    df = Concatenate_DataFrames_2008_2018(x).filter(col('VL_DIAS_MENORES_15_CALC') != 'N_SELECT')\
            .withColumn('Valor_Dia_Trab', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM')/22, 3))\
            .withColumn('Custo_Afast_Dia_Trab', f.round( f.col('Valor_Dia_Trab') * f.col('VL_DIAS_MENORES_15_CALC')))\
            .withColumn('Custo_Assist_Saude', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM') * 0.2))\
            .withColumn('Custo_Direto_Absent', 
                        f.when (
                          f.col('VL_DIAS_MENORES_15_CALC') == 0,0)\
                        .otherwise(
                          f.round( f.col('Custo_Assist_Saude') + f.col('Custo_Afast_Dia_Trab'))
                        )
                       )
    df = df.groupby('ANO').agg(
      f.sum(f.col('Valor_Dia_Trab')).alias('Valor_Dia_Trab'),
      f.sum(f.col('Custo_Afast_Dia_Trab')).alias('Custo_Afast_Dia_Trab'),
      f.sum(f.col('Custo_Assist_Saude')).alias('Custo_Assist_Saude'),
      f.sum(f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent'),
      f.sum( 2 * f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent_2'),
      f.sum( (2 * f.col('Custo_Direto_Absent')) + f.col('Custo_Direto_Absent') ).alias('Custo_Direto_Absent_3'),
      f.count('ID_CPF'),
      )
    return df
    
  elif (kwargs.get('CD_CNAE20_DIVISAO') is None) and (kwargs.get('CD_CBO4') is None):
    df = Concatenate_DataFrames_2008_2018(x).filter(col('VL_DIAS_MENORES_15_CALC') != 'N_SELECT')\
            .withColumn('Valor_Dia_Trab', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM')/22, 3))\
            .withColumn('Custo_Afast_Dia_Trab', f.round( f.col('Valor_Dia_Trab') * f.col('VL_DIAS_MENORES_15_CALC')))\
            .withColumn('Custo_Assist_Saude', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM') * 0.2))\
            .withColumn('Custo_Direto_Absent', 
                        f.when (
                          f.col('VL_DIAS_MENORES_15_CALC') == 0,0)\
                        .otherwise(
                          f.round( f.col('Custo_Assist_Saude') + f.col('Custo_Afast_Dia_Trab'))
                        )
                       )
    df = df.groupby('ANO','CD_UF').agg(
      f.sum(f.col('Valor_Dia_Trab')).alias('Valor_Dia_Trab'),
      f.sum(f.col('Custo_Afast_Dia_Trab')).alias('Custo_Afast_Dia_Trab'),
      f.sum(f.col('Custo_Assist_Saude')).alias('Custo_Assist_Saude'),
      f.sum(f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent'),
      f.sum( 2 * f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent_2'),
      f.sum( (2 * f.col('Custo_Direto_Absent')) + f.col('Custo_Direto_Absent') ).alias('Custo_Direto_Absent_3'),
      f.count('ID_CPF'),
      )
    return df

  elif kwargs.get('CD_CBO4') is None:
    df = Concatenate_DataFrames_2008_2018(x).filter(col('VL_DIAS_MENORES_15_CALC') != 'N_SELECT')\
            .withColumn('Valor_Dia_Trab', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM')/22, 3))\
            .withColumn('Custo_Afast_Dia_Trab', f.round( f.col('Valor_Dia_Trab') * f.col('VL_DIAS_MENORES_15_CALC')))\
            .withColumn('Custo_Assist_Saude', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM') * 0.2))\
            .withColumn('Custo_Direto_Absent', 
                        f.when (
                          f.col('VL_DIAS_MENORES_15_CALC') == 0,0)\
                        .otherwise(
                          f.round( f.col('Custo_Assist_Saude') + f.col('Custo_Afast_Dia_Trab'))
                        )
                       )
    df = df.groupby('ANO','CD_UF','CD_CNAE20_DIVISAO').agg(
      f.sum(f.col('Valor_Dia_Trab')).alias('Valor_Dia_Trab'),
      f.sum(f.col('Custo_Afast_Dia_Trab')).alias('Custo_Afast_Dia_Trab'),
      f.sum(f.col('Custo_Assist_Saude')).alias('Custo_Assist_Saude'),
      f.sum(f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent'),
      f.sum( 2 * f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent_2'),
      f.sum( (2 * f.col('Custo_Direto_Absent')) + f.col('Custo_Direto_Absent') ).alias('Custo_Direto_Absent_3'),
      f.count('ID_CPF'),
      )
    return df 
    
  else:
    df = Concatenate_DataFrames_2008_2018(x).filter(col('VL_DIAS_MENORES_15_CALC') != 'N_SELECT')\
            .withColumn('Valor_Dia_Trab', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM')/22, 3))\
            .withColumn('Custo_Afast_Dia_Trab', f.round( f.col('Valor_Dia_Trab') * f.col('VL_DIAS_MENORES_15_CALC')))\
            .withColumn('Custo_Assist_Saude', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM') * 0.2))\
            .withColumn('Custo_Direto_Absent', 
                        f.when (
                          f.col('VL_DIAS_MENORES_15_CALC') == 0,0)\
                        .otherwise(
                          f.round( f.col('Custo_Assist_Saude') + f.col('Custo_Afast_Dia_Trab'))
                        )
                       )
    df = df.groupby('ANO','CD_UF','CD_CNAE20_DIVISAO','CD_CBO4').agg(
      f.sum(f.col('Valor_Dia_Trab')).alias('Valor_Dia_Trab'),
      f.sum(f.col('Custo_Afast_Dia_Trab')).alias('Custo_Afast_Dia_Trab'),
      f.sum(f.col('Custo_Assist_Saude')).alias('Custo_Assist_Saude'),
      f.sum(f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent'),
      f.sum( 2 * f.col('Custo_Direto_Absent')).alias('Custo_Direto_Absent_2'),
      f.sum( (2 * f.col('Custo_Direto_Absent')) + f.col('Custo_Direto_Absent') ).alias('Custo_Direto_Absent_3'),
      f.count('ID_CPF'),
      )
    return df

# COMMAND ----------

# def Indice_Absenteismo_ANO_2008_2018(x,*args): ==> NAO ME PERMITIU USAR
def teste(x,**kwargs):
  #  kwargs.get('ANO'), kwargs.get('CD_UF'), kwargs.get('CD_CNAE20_DIVISAO')
  
  if (kwargs.get('CD_UF') is None) and (kwargs.get('CD_CNAE20_DIVISAO') is None) and (kwargs.get('CD_CBO4') is None):
    df = Concatenate_DataFrames_2008_2018(x).filter(col('VL_DIAS_MENORES_15_CALC') != 'N_SELECT')\
            .withColumn('Valor_Dia_Trab', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM')/22, 3))\
            .withColumn('Custo_Afast_Dia_Trab', f.round( f.col('Valor_Dia_Trab') * f.col('VL_DIAS_MENORES_15_CALC')))\
            .withColumn('Custo_Assist_Saude', f.round(f.col('VL_MEDIA_NOM_DEZEMBRO_NOM') * 0.2))\
            .withColumn('Custo_Direto_Absent', 
                        f.when (
                          f.col('VL_DIAS_MENORES_15_CALC') == 0,0)\
                        .otherwise(
                          f.round( f.col('Custo_Assist_Saude') + f.col('Custo_Afast_Dia_Trab'))
                        )
                       )
    return df
