# Databricks notebook source
# MAGIC %md
# MAGIC ###Prepara dados da RAIS de 2002 a 2018
# MAGIC - Responsável: Anaely Machado
# MAGIC - Lê arquivos originais da RAIS de 2002 a 2007 ('uds/me/rais_vinc_02_07')
# MAGIC - Padroniza layout e formatos revisados com o de-para salvo na UDS ('uds/me/rais_depara')
# MAGIC - Salva bases padronizadas na UDS ('uds/me/rais_vinc_02_07/padronizado')
# MAGIC - Lê arquivos de 2008 a 2018, corrige variáveis e salva na UDS: 'uds/me/rais_vinc_08_18/padronizado'
# MAGIC - Dados tratados em pastas separadas, pois 2002 a 2007 não faz parte do acordo da UNIEPRO com o ME. Mas pode ser usado desidentificado para séries históricas mais longas ou análises internas

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import datetime

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

# DBTITLE 1,Função para padronizar nomes de colunas (remover caracteres especiais)
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

# COMMAND ----------

# DBTITLE 1,Função para buscar as colunas do De/Para e transformar para pyspark
def select_columns_de_para(dataframe, de_para_path, sheet):
  #headers = {'name_header':'Tabela Origem','pos_header':'B','pos_org':'C','pos_dst':'E','pos_type':'F'}
  #de_para_values = cf.parse_ba_doc(dbutils, de_para_path, headers)
  
  de_para_values = cf.parse_ba_doc(dbutils, de_para_path, headers = {'name_header':'Tabela Origem','pos_header':'B','pos_org':'C','pos_dst':'E','pos_type':'F'})
  
  cf.check_ba_doc(dataframe, de_para_values, sheet)
  
  for column, destination, _type in de_para_values[sheet]:
    if column == 'N/A':
        yield f.lit(None).cast(_type).alias(destination)
    else:
      col = f.col(column)
      if _type.lower() in ['decimal', 'double']:
        col = f.regexp_replace(column, ',', '.')
      yield col.cast(_type).alias(destination)
      
# base = rename_columns(base)
# base = base.select(*select_columns_de_para(base,de_para_path,sheet=('VINCULO_'+i)))

# COMMAND ----------

 de_para_values = cf.parse_ba_doc(dbutils, de_para_path, headers = {'name_header':'Tabela Origem','pos_header':'B','pos_org':'C','pos_dst':'E','pos_type':'F'})
  

# COMMAND ----------

#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
dir_original = '{uri}/raw/me/rais_vinculo/2018'

base = spark.read.csv(path.format(uri=var_adls_uri), header=True, sep=';', encoding='latin1')
    

# COMMAND ----------

path = '{uri}/raw/me/rais_vinculo/2018'
base = spark.read.parquet(path.format(uri=var_adls_uri))


# COMMAND ----------

# MAGIC %md
# MAGIC ## RAIS Vinc 2002-2007
# MAGIC - '/uds/uniepro/me/rais_vinc_02_07/' -> '/uds/uniepro/me/rais_vinc_02_07/padronizado'

# COMMAND ----------

#caminho para o de-para
de_para_path = '/uds/uniepro/me/rais_vinc_02_07/rais_mapeamento_depara_02_07/Mapeamento_uds_depara_rais.xlsx'
de_para_path

# COMMAND ----------

dir_original = '{uri}/uds/uniepro/me/rais_vinc_02_07/original/2007'

base = spark.read.csv(path.format(uri=var_adls_uri), header=True, sep=';', encoding='latin1')


# COMMAND ----------

# DBTITLE 1,Padronização do layout
#Lê bases originais, padroniza e salva no novo diretório
dir_original = '{uri}/uds/uniepro/me/rais_vinc_02_07/original'
dir_padronizado = '{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado'

#'2002','2003','2004','2005','2006','2007'
for i in ['2004'
         ]:
  path = dir_original + '/' + i
  
  base = spark.read.csv(path.format(uri=var_adls_uri), header=True, sep=';', encoding='latin1')
    
  base = rename_columns(base)
  base = base.select(*select_columns_de_para(base,de_para_path,sheet=('VINCULO_'+i)))
  base=base.withColumn('ANO',f.lit(i).cast('Int')) 
    
  base = (base
          .withColumn('CD_CBO94',f.when(f.lower(f.col("CD_CBO94")) == "ignorado", f.lit(-1)).otherwise(f.regexp_replace('CD_CBO94', 'CBO ', '')))
          .withColumn('CD_CBO94',f.when(f.lower(f.col("CD_CBO94")) == "{ñ cl", f.lit(None)).otherwise(f.col("CD_CBO94")))
          .withColumn('CD_SEXO', f.when(f.col('CD_SEXO') == 'MASCULINO', f.lit(1))
                                 .otherwise(f.when(f.col('CD_SEXO') == 'FEMININO',f.lit(2))
                                 .otherwise(f.col('CD_SEXO'))))
          .withColumn('CD_CBO', f.when(f.lower(f.col("CD_CBO")) == "ignorado", f.lit("-1"))
                      .otherwise(f.regexp_replace('CD_CBO', 'CBO ', '')))
          .withColumn('CD_CBO', f.when(f.lower(f.col("CD_CBO")) == "0000-1", f.lit("-1"))
                      .otherwise(f.col("CD_CBO")))
          .withColumn('DT_DIA_MES_ANO_DATA_NASCIMENTO',f.lpad(f.col('DT_DIA_MES_ANO_DATA_NASCIMENTO'),8,'0'))
          .withColumn('DT_ANO_NASCIMENTO',f.substring(f.col('DT_DIA_MES_ANO_DATA_NASCIMENTO'),5,8).cast('int'))
          .withColumn('VL_IDADE', (f.lit(i)-f.col('DT_ANO_NASCIMENTO')))
          .withColumn('CD_UF',f.substring(f.col('CD_MUNICIPIO'),1,2))
          .withColumn('CD_CNAE20_DIVISAO',f.substring(f.col('CD_CNAE20_CLASSE'),1,2))
          .withColumn('CD_CBO4',f.substring(f.col('CD_CBO'),1,4))
         )
  path2 = dir_padronizado + '/' + i
 # (base.write.parquet(path2.format(uri=var_adls_uri), mode='overwrite'))


# COMMAND ----------

# DBTITLE 1,Inclui CNAE 2.0 nos anos de 2002 a 2005
cnae_path = '{uri}/uds/uniepro/me/rais_vinc_02_07/tabuas_conversao/conversao_cnae10_20.csv'.format(uri=var_adls_uri)
cnae = spark.read.csv(cnae_path.format(uri=var_adls_uri), header=True, sep=';', encoding='latin1')
cnae = (cnae
        .withColumn('CD_CNAE10_CLASSE',f.regexp_replace(f.col('CD_CNAE10_CLASSE'),'-',''))
        .withColumn('CD_CNAE10_CLASSE',f.regexp_replace(f.col('CD_CNAE10_CLASSE'),'\.',''))
        .withColumn('CD_CNAE20_CLASSE',f.regexp_replace(f.col('CD_CNAE20_CLASSE'),'-',''))
        .withColumn('CD_CNAE20_CLASSE',f.regexp_replace(f.col('CD_CNAE20_CLASSE'),'\.',''))
                )
display(cnae)

# COMMAND ----------

#Acrescenta cnae 2.0 e calcula CNAE Div
dir_padronizado = '{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado'

#'2002','2003','2004','2005'
for i in ['2004']:
  
  path = dir_padronizado + '/' + i
  
  base = spark.read.parquet(path.format(uri=var_adls_uri))
  
  base = (base
          .drop('CD_CNAE20_CLASSE')
          .join(cnae, ['CD_CNAE10_CLASSE'],'left')
          .withColumn('CD_CNAE20_DIVISAO',f.substring(f.col('CD_CNAE20_CLASSE'),1,2))
         )
  
  (base.write.parquet(path.format(uri=var_adls_uri), mode='overwrite'))
    

# COMMAND ----------

# DBTITLE 1,Ajusta variáveis de afastamento em 2007
path = '{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2007'.format(uri=var_adls_uri)
base = spark.read.parquet(path)
base = (base
        .withColumn('CD_CAUSA_AFASTAMENTO1',f.when(f.col('CD_CAUSA_AFASTAMENTO1')=='-1',f.lit('99'))
                    .otherwise(f.col('CD_CAUSA_AFASTAMENTO1')))
        .withColumn('CD_CAUSA_AFASTAMENTO2',f.when(f.col('CD_CAUSA_AFASTAMENTO2')=='-1',f.lit('99'))
                    .otherwise(f.col('CD_CAUSA_AFASTAMENTO2')))
        .withColumn('CD_CAUSA_AFASTAMENTO3',f.when(f.col('CD_CAUSA_AFASTAMENTO3')=='-1',f.lit('99'))
                    .otherwise(f.col('CD_CAUSA_AFASTAMENTO3')))
        .withColumn('NR_DIA_INI_AF1',f.when(f.col('NR_DIA_INI_AF1')== '-1',f.lit('99')).otherwise(f.col('NR_DIA_INI_AF1')))
        .withColumn('NR_DIA_INI_AF2',f.when(f.col('NR_DIA_INI_AF2')== '-1',f.lit('99')).otherwise(f.col('NR_DIA_INI_AF2')))
        .withColumn('NR_DIA_INI_AF3',f.when(f.col('NR_DIA_INI_AF3')== '-1',f.lit('99')).otherwise(f.col('NR_DIA_INI_AF3')))
        .withColumn('NR_MES_INI_AF1',f.when(f.col('NR_MES_INI_AF1')== '-1',f.lit('99')).otherwise(f.col('NR_MES_INI_AF1')))
        .withColumn('NR_MES_INI_AF2',f.when(f.col('NR_MES_INI_AF2')== '-1',f.lit('99')).otherwise(f.col('NR_MES_INI_AF2')))
        .withColumn('NR_MES_INI_AF3',f.when(f.col('NR_MES_INI_AF3')== '-1',f.lit('99')).otherwise(f.col('NR_MES_INI_AF3')))
        .withColumn('NR_DIA_FIM_AF1',f.when(f.col('NR_DIA_FIM_AF1')== '-1',f.lit('99')).otherwise(f.col('NR_DIA_FIM_AF1')))
        .withColumn('NR_DIA_FIM_AF2',f.when(f.col('NR_DIA_FIM_AF2')== '-1',f.lit('99')).otherwise(f.col('NR_DIA_FIM_AF2')))
        .withColumn('NR_DIA_FIM_AF3',f.when(f.col('NR_DIA_FIM_AF3')== '-1',f.lit('99')).otherwise(f.col('NR_DIA_FIM_AF3')))
        .withColumn('NR_MES_FIM_AF1',f.when(f.col('NR_MES_FIM_AF1')== '-1',f.lit('99')).otherwise(f.col('NR_MES_FIM_AF1')))
        .withColumn('NR_MES_FIM_AF2',f.when(f.col('NR_MES_FIM_AF2')== '-1',f.lit('99')).otherwise(f.col('NR_MES_FIM_AF2')))
        .withColumn('NR_MES_FIM_AF3',f.when(f.col('NR_MES_FIM_AF3')== '-1',f.lit('99')).otherwise(f.col('NR_MES_FIM_AF3')))
       )

 # (base.write.parquet(path.format(uri=var_adls_uri), mode='overwrite'))

# COMMAND ----------

path = '{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2007'.format(uri=var_adls_uri)
base.write.parquet(path.format(uri=var_adls_uri), mode='overwrite')

# COMMAND ----------

#Abre todas as bases
base2002 = spark.read.parquet('{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2002'.format(uri=var_adls_uri))
base2003 = spark.read.parquet('{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2003'.format(uri=var_adls_uri))
base2004 = spark.read.parquet('{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2004'.format(uri=var_adls_uri))
base2005 = spark.read.parquet('{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2005'.format(uri=var_adls_uri))
base2006 = spark.read.parquet('{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2006'.format(uri=var_adls_uri))
base2007 = spark.read.parquet('{uri}/uds/uniepro/me/rais_vinc_02_07/padronizado/2007'.format(uri=var_adls_uri))

# COMMAND ----------

#Ordena colunas
base2003 = base2003.select(*base2002.columns)
base2004 = base2004.select(*base2002.columns)
base2005 = base2005.select(*base2002.columns)
base2006 = base2006.select(*base2002.columns)
base2007 = base2007.select(*base2002.columns)

# COMMAND ----------

base_final = (base2002
              .union(base2003)
              .union(base2004)
              .union(base2005)
              .union(base2006)
              .union(base2007))

# COMMAND ----------

display(base_final.groupBy('ANO').agg(f.count(f.lit(1))))

# COMMAND ----------

# Salva base final
(base_final
  .write
  .partitionBy('ANO')
  .parquet(var_adls_uri + '/uds/uniepro/me/rais_vinc_02_07/final', mode='overwrite'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAIS Vinc 2008-2018
# MAGIC - Lê arquivos da RAIS de 2008 a 2018 da TRS ('/trs/me/rais_vinculo')
# MAGIC - Corrige as variáveis que estavam erradas
# MAGIC - Salva bases corrigidas na UDS ('uds/me/rais_vinc_08_18/corrigido') 

# COMMAND ----------

trs_rais_vinculo_path = '{uri}/trs/me/rais_vinculo'.format(uri=var_adls_uri)
df_rais_vinculo = spark.read.parquet(trs_rais_vinculo_path)
df_rais_vinculo.printSchema()

# COMMAND ----------

#Corrigindo variáveis
df_rais_vinculo = (df_rais_vinculo
                    .withColumn('NR_MES_TEMPO_EMPREGO',f.regexp_replace(f.col('NR_MES_TEMPO_EMPREGO'),',','.').cast('double'))
                    .withColumn('DT_DIA_MES_ANO_DATA_NASCIMENTO',f.lpad(f.col('DT_DIA_MES_ANO_DATA_NASCIMENTO'),8,'0'))
                    .withColumn('DT_ANO_NASCIMENTO',f.substring(f.col('DT_DIA_MES_ANO_DATA_NASCIMENTO'),5,8).cast('int'))
                    .withColumn('VL_IDADE', (f.col('ANO')-f.col('DT_ANO_NASCIMENTO')))
                    .withColumn('CD_CNAE20_CLASSE',f.lpad(f.col('CD_CNAE20_CLASSE'),5,'0'))
                    .withColumn('CD_CNAE20_SUBCLASSE',f.lpad(f.col('CD_CNAE20_SUBCLASSE'),7,'0'))
                    .withColumn('CD_CBO',f.lpad(f.col('CD_CBO'),6,'0'))
                    .withColumn('CD_CNAE20_DIVISAO',f.substring('CD_CNAE20_CLASSE',1,2))     
                    .withColumn('CD_CBO4',f.substring('CD_CBO',1,4))
                   .withColumn('ID_CPF',f.lpad(f.col('ID_CPF'),11,'0'))
                   .withColumn('ID_CNPJ_CEI',f.lpad(f.col('ID_CNPJ_CEI'),14,'0'))                   
                   )

# COMMAND ----------

# checar se está certo
display(df_rais_vinculo.where(f.col('DT_DIA_MES_ANO_DATA_NASCIMENTO').isNotNull())
        .select('DT_DIA_MES_ANO_DATA_NASCIMENTO','DT_ANO_NASCIMENTO','ANO','VL_IDADE',
                'CD_CNAE20_CLASSE','CD_CNAE20_SUBCLASSE','CD_CNAE20_DIVISAO','CD_CBO','CD_CBO4'))

# COMMAND ----------

# DBTITLE 1,correção das variáveis de salário
# MAGIC %md
# MAGIC Porém, tem erro nas variáveis de salário na TRS. A coluna de SM está trocada pela Nominal para 2009 a 2018. Além de erros para 2009 e 2010
# MAGIC - 2009
# MAGIC - VL_REMUN_DEZEMBRO_NOM <> VL_REMUN_MEDIA_SM
# MAGIC - VL_REMUN_DEZEMBRO_SM  <> VL_REMUN_MEDIA_NOM
# MAGIC - VL_REMUN_MEDIA_NOM    <> VL_REMUN_DEZEMBRO_SM
# MAGIC - VL_REMUN_MEDIA_SM     <> VL_REMUN_DEZEMBRO_NOM
# MAGIC
# MAGIC - 2010
# MAGIC - VL_REMUN_MEDIA_NOM    <> VL_REMUN_DEZEMBRO_NOM
# MAGIC - VL_REMUN_DEZEMBRO_NOM <> VL_REMUN_MEDIA_NOM
# MAGIC
# MAGIC - 2011-2018
# MAGIC - VL_REMUN_MEDIA_NOM  <> VL_REMUN_MEDIA_SM
# MAGIC - VL_REMUN_MEDIA_SM   <>  VL_REMUN_MEDIA_NOM 
# MAGIC - VL_REMUN_DEZEMBRO_NOM  <> VL_REMUN_DEZEMBRO_SM
# MAGIC - VL_REMUN_DEZEMBRO_SM   <>  VL_REMUN_DEZEMBRO_NOM  

# COMMAND ----------

# corrigindo variáveis trocadas de salários em 2009 a 2018
df_rais_vinculo = (df_rais_vinculo
                         .withColumn('VL_REMUN_MEDIA_NOM_c', 
                                     f.when(f.col('ANO')>f.lit(2010), f.col('VL_REMUN_MEDIA_SM'))
                                     .otherwise(f.when(f.col('ANO')==f.lit(2009),f.col('VL_REMUN_DEZEMBRO_SM'))
                                                .otherwise(f.when(f.col('ANO')==f.lit(2010),f.col('VL_REMUN_DEZEMBRO_NOM'))
                                                           .otherwise(f.col('VL_REMUN_MEDIA_NOM')))))
                         .withColumn('VL_REMUN_MEDIA_SM_c', 
                                     f.when(f.col('ANO')>f.lit(2010), f.col('VL_REMUN_MEDIA_NOM'))
                                     .otherwise(f.when(f.col('ANO')==f.lit(2009),f.col('VL_REMUN_DEZEMBRO_NOM'))
                                                .otherwise(f.col('VL_REMUN_MEDIA_SM'))))
                         .withColumn('VL_REMUN_DEZEMBRO_NOM_c', 
                                     f.when(f.col('ANO')>f.lit(2010), f.col('VL_REMUN_DEZEMBRO_SM'))
                                     .otherwise(f.when(f.col('ANO')==f.lit(2009),f.col('VL_REMUN_MEDIA_SM'))
                                                .otherwise(f.when(f.col('ANO')==f.lit(2010),f.col('VL_REMUN_MEDIA_NOM'))
                                                                  .otherwise(f.col('VL_REMUN_DEZEMBRO_NOM')))))
                         .withColumn('VL_REMUN_DEZEMBRO_SM_c', 
                                     f.when(f.col('ANO')>f.lit(2010), f.col('VL_REMUN_DEZEMBRO_NOM'))
                                     .otherwise(f.when(f.col('ANO')==f.lit(2009),f.col('VL_REMUN_MEDIA_NOM'))
                                                .otherwise(f.col('VL_REMUN_DEZEMBRO_SM'))))
                         .drop('VL_REMUN_MEDIA_SM','VL_REMUN_MEDIA_NOM','VL_REMUN_DEZEMBRO_SM','VL_REMUN_DEZEMBRO_NOM')
                         .withColumnRenamed('VL_REMUN_MEDIA_NOM_c','VL_REMUN_MEDIA_NOM')
                         .withColumnRenamed('VL_REMUN_MEDIA_SM_c','VL_REMUN_MEDIA_SM')    
                         .withColumnRenamed('VL_REMUN_DEZEMBRO_NOM_c','VL_REMUN_DEZEMBRO_NOM')
                         .withColumnRenamed('VL_REMUN_DEZEMBRO_SM_c','VL_REMUN_DEZEMBRO_SM')
                        )

#para checar se está correto: comparar total da massa salarial por cada variável com BI do MTE. Exemplo:
#display(df_rais_vinculo_09_18.where(f.col('FL_VINCULO_ATIVO_3112')==f.lit(1)).groupBy('ANO').agg(f.sum(f.col('VL_REMUN_MEDIA_NOM_c'))))

# COMMAND ----------

display(df_rais_vinculo.groupBy('ANO').agg(f.sum('VL_REMUN_MEDIA_NOM')))

# COMMAND ----------

(df_rais_vinculo
 .write
 .partitionBy('ANO')
 .parquet(var_adls_uri + '/uds/uniepro/me/rais_vinc_08_18/corrigido', mode='overwrite'))