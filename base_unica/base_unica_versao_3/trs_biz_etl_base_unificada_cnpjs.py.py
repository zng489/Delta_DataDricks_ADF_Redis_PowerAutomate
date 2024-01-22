# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

import json
import re
import crawler.functions as cf
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import IntegerType
import datetime
from datetime import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from trs_control_field import trs_control_field as tcf
from pyspark.sql.types import IntegerType
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

cadastro_estbl_f = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['cadastro_estbl_f'])
cadastro_estbl = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(cadastro_estbl_f, mode="FAILFAST")

cadastro_empresa_f = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['cadastro_empresa_f'])
cadastro_empresa = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(cadastro_empresa_f, mode="FAILFAST")

cadastro_simples_f = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['cadastro_simples_f'])
cadastro_simples = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(cadastro_simples_f, mode="FAILFAST")

rais_estab = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['rais_estab'])
rais_estab = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(rais_estab, mode="FAILFAST")
rais_estab = rais_estab.withColumn('CD_CNAE20_SUBCLASSE', f.lpad(f.col('CD_CNAE20_SUBCLASSE'),7,'0'))

rais_vinc = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['rais_vinc'])
rais_vinc = (spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(rais_vinc, mode="FAILFAST")
             .where((f.col('ANO').isin([2016,2017,2018])) & (f.col('FL_VINCULO_ATIVO_3112')==f.lit('1')))
             .select('ANO', 'ID_CNPJ_CEI', 'FL_VINCULO_ATIVO_3112', 'CD_MUNICIPIO', 'CD_GRAU_INSTRUCAO', 'CD_SEXO', 'CD_RACA_COR', 'VL_IDADE', 'VL_REMUN_MEDIA_NOM', 'CD_TIPO_VINCULO')
              )

# COMMAND ----------

cadastro_estbl = (cadastro_estbl
                 .where((f.col('CD_SIT_CADASTRAL').isin('01', '02', '03')))
                 .withColumn('ANO_SIT_CADASTRAL', f.substring(f.col('DT_SIT_CADASTRAL'),1,4))
                 .withColumn('ANO_INICIO_ATIV', f.substring(f.col('DT_INICIO_ATIV'),1,4)))

# COMMAND ----------

cadastro_estbl = (cadastro_estbl.where((f.col('ANO_INICIO_ATIV')!='1199')))
#cadastro_estbl.count()

# COMMAND ----------

cadastro_estbl = (cadastro_estbl.where((f.col('ANO_INICIO_ATIV')!='1601')))
#cadastro_estbl.count()

# COMMAND ----------

rais_vinc_raca = (rais_vinc
              .groupby('ID_CNPJ_CEI', 'ANO')
              .pivot('CD_RACA_COR')
              .agg(f.count(f.lit(1)).alias('ANOS_CNPJ'))
              .withColumnRenamed('01', 'NR_FUNC_INDIGENA')
              .withColumnRenamed('02', 'NR_FUNC_BRANCA')
              .withColumnRenamed('04', 'NR_FUNC_PRETA')
              .withColumnRenamed('06', 'NR_FUNC_AMARELA')
              .withColumnRenamed('08', 'NR_FUNC_PARDA')
              .withColumnRenamed('09', 'NR_FUNC_NIDENT')
              .withColumnRenamed('99', 'NR_FUNC_NDECLARADA')
              )

# COMMAND ----------

rais_vinc_sexo = (rais_vinc
              .groupby('ID_CNPJ_CEI', 'ANO')
              .pivot('CD_SEXO')
              .agg(f.count(f.lit(1)).alias('ANOS_CNPJ'))
              .withColumnRenamed('01', 'NR_FUNC_MASCULINO')
              .withColumnRenamed('02', 'NR_FUNC_FEMININO')
              )

# COMMAND ----------

rais_vinc_escol = (rais_vinc.withColumn('ESCOLARIDADE', 
                                 f.when(f.col('CD_GRAU_INSTRUCAO') == f.lit('01'), f.lit('01'))
                                 .otherwise(
                                 f.when((f.col('CD_GRAU_INSTRUCAO') == f.lit('02')) | (f.col('CD_GRAU_INSTRUCAO') == f.lit('03')) | (f.col('CD_GRAU_INSTRUCAO') == f.lit('04')), f.lit('02'))
                                 .otherwise(
                                 f.when((f.col('CD_GRAU_INSTRUCAO') == f.lit('05')) | (f.col('CD_GRAU_INSTRUCAO') == f.lit('06')), f.lit('03'))
                                 .otherwise(
                                 f.when((f.col('CD_GRAU_INSTRUCAO') == f.lit('08')) | (f.col('CD_GRAU_INSTRUCAO') == f.lit('07')), f.lit('04'))
                                 .otherwise(
                                 f.when((f.col('CD_GRAU_INSTRUCAO') == f.lit('09')) | (f.col('CD_GRAU_INSTRUCAO') == f.lit('10')) | (f.col('CD_GRAU_INSTRUCAO') == f.lit('11')), f.lit('05')) 
                                 ))))))

# COMMAND ----------

rais_vinc_escol = (rais_vinc_escol
              .groupby('ID_CNPJ_CEI', 'ANO')
              .pivot('ESCOLARIDADE')
              .agg(f.count(f.lit(1)).alias('ANOS_CNPJ'))
              .withColumnRenamed('01', 'NR_FUNC_SEM_INSTRUCAO')
              .withColumnRenamed('02', 'NR_FUNC_SABE_LER_ESCREVER')
              .withColumnRenamed('03', 'NR_FUNC_PM_EF_COMPLETO')
              .withColumnRenamed('04', 'NR_FUNC_PM_EM_COMPLETO')
              .withColumnRenamed('05', 'NR_FUNC_SUPERIO_COMPLETO_MAIS')
              )

# COMMAND ----------

rais_vinc_fetaria = (rais_vinc
                     .where(f.col('VL_IDADE') >= 15)
                     .withColumn('FAIXA_ETARIA', 
                                 f.when((f.col('VL_IDADE') >= f.lit(15)) & (f.col('VL_IDADE') <= f.lit(17)), f.lit('01'))
                                 .otherwise(
                                 f.when((f.col('VL_IDADE') >= f.lit(18)) & (f.col('VL_IDADE') <= f.lit(24)), f.lit('02'))
                                 .otherwise(
                                 f.when((f.col('VL_IDADE') >= f.lit(25)) & (f.col('VL_IDADE') <= f.lit(29)), f.lit('03'))
                                 .otherwise(
                                 f.when((f.col('VL_IDADE') >= f.lit(30)) & (f.col('VL_IDADE') <= f.lit(39)), f.lit('04'))
                                 .otherwise(
                                 f.when((f.col('VL_IDADE') >= f.lit(40)) & (f.col('VL_IDADE') <= f.lit(49)), f.lit('05')) 
                                 .otherwise(
                                 f.when((f.col('VL_IDADE') >= f.lit(50)) & (f.col('VL_IDADE') <= f.lit(64)), f.lit('06'))
                                 .otherwise(
                                 f.when(f.col('VL_IDADE') >= f.lit(65), f.lit('07'))  
                                 ))))))))

# COMMAND ----------

rais_vinc_fetaria = (rais_vinc_fetaria
              .groupby('ID_CNPJ_CEI', 'ANO')
              .pivot('FAIXA_ETARIA')
              .agg(f.count(f.lit(1)).alias('ANOS_CNPJ'))
              .withColumnRenamed('01', 'NR_FUNC_15_A_17ANOS')
              .withColumnRenamed('02', 'NR_FUNC_18_A_24ANOS')
              .withColumnRenamed('03', 'NR_FUNC_25_A_29ANOS')
              .withColumnRenamed('04', 'NR_FUNC_30_A_39ANOS')
              .withColumnRenamed('05', 'NR_FUNC_40_A_49ANOS')
              .withColumnRenamed('06', 'NR_FUNC_50_A_64ANOS')
              .withColumnRenamed('07', 'NR_FUNC_65_MAIS')
              )

# COMMAND ----------

rais_vinc_aprend = (rais_vinc
                    .where(f.col('CD_TIPO_VINCULO')==55)
                    .groupby(['ID_CNPJ_CEI', 'ANO'])
                    .agg(f.count(f.lit(1)).alias('NR_FUNC_APRENDIZ'))
              .orderBy('ID_CNPJ_CEI', ascending=False))

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### Deflacionamento do salário

# COMMAND ----------

trs_ipca_path = "{adl_path}{trs}/{file_folder}".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['trs_ipca_path'])
df_ipca = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(trs_ipca_path, mode="FAILFAST")        

# COMMAND ----------

window = Window.partitionBy()

df_ipca2 = (df_ipca
           .withColumnRenamed('NR_ANO','ANO')
           .withColumnRenamed('VL_IPCA','VALOR_IPCA')
           .withColumnRenamed('DS_MES_COMPETENCIA','MES_COMPETENCIA')
           .withColumn('MES_COMPETENCIA', 
                       f.when(f.col('MES_COMPETENCIA')==f.lit('janeiro'), f.lit(1)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('fevereiro'), f.lit(2)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('março'), f.lit(3)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('abril'), f.lit(4)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('maio'), f.lit(5)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('junho'), f.lit(6)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('julho'), f.lit(7)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('agosto'), f.lit(8)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('setembro'), f.lit(9)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('outubro'), f.lit(10)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('novembro'), f.lit(11)).otherwise(
                          
                         f.when(f.col('MES_COMPETENCIA')==f.lit('dezembro'), f.lit(12))))))))))))))
           .orderBy("ANO","MES_COMPETENCIA")
           .withColumn('base_fixa',f.last(f.col('VALOR_IPCA')).over(window))
           .withColumn('INDICE_FIXO_ULTIMO_ANO',f.col('VALOR_IPCA')/f.col('base_fixa'))
           .select('ANO',"MES_COMPETENCIA",'INDICE_FIXO_ULTIMO_ANO'))

# COMMAND ----------

df_ipca2 = (df_ipca
           .withColumnRenamed('NR_ANO','ANO')
           .withColumnRenamed('VL_IPCA','VALOR_IPCA')
           .withColumnRenamed('DS_MES_COMPETENCIA','MES_COMPETENCIA')
           .withColumn('MES_COMPETENCIA', 
                       f.when(f.col('MES_COMPETENCIA')==f.lit('janeiro'), f.lit(1)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('fevereiro'), f.lit(2)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('março'), f.lit(3)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('abril'), f.lit(4)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('maio'), f.lit(5)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('junho'), f.lit(6)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('julho'), f.lit(7)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('agosto'), f.lit(8)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('setembro'), f.lit(9)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('outubro'), f.lit(10)).otherwise(
                         f.when(f.col('MES_COMPETENCIA')==f.lit('novembro'), f.lit(11)).otherwise(
                          
                         f.when(f.col('MES_COMPETENCIA')==f.lit('dezembro'), f.lit(12))))))))))))))
           .orderBy("ANO","MES_COMPETENCIA"))

# COMMAND ----------

df_ipca2 = (df_ipca2
.withColumn('base_fixa',f.last(f.col('VALOR_IPCA')).over(window))
           .withColumn('INDICE_FIXO_ULTIMO_ANO',f.col('VALOR_IPCA')/f.col('base_fixa'))
           .select('ANO',"MES_COMPETENCIA",'INDICE_FIXO_ULTIMO_ANO'))

# COMMAND ----------

df_ipca_dez = (df_ipca2
               .where(f.col("MES_COMPETENCIA")==f.lit(12))
               .select('ANO','INDICE_FIXO_ULTIMO_ANO'))

# COMMAND ----------

rais_salario = (rais_vinc
                   .join(df_ipca_dez,['ANO'],'left') 
                   .withColumn('VL_REMUN_MEDIA_REAL', f.col('VL_REMUN_MEDIA_NOM')/f.col('INDICE_FIXO_ULTIMO_ANO'))
                   .drop('INDICE_FIXO_ULTIMO_ANO'))

# COMMAND ----------

rais_salario = (rais_salario
                    .groupby(['ID_CNPJ_CEI', 'ANO'])
                    .agg((f.sum('VL_REMUN_MEDIA_REAL').alias('MASSA_SALARIAL')),
                         f.mean('VL_REMUN_MEDIA_REAL').alias('REMUNERACAO_MEDIA')))

# COMMAND ----------

rais_info_vinc = rais_vinc_raca.join(rais_vinc_sexo,['ID_CNPJ_CEI', 'ANO'],'outer')
rais_info_vinc = rais_info_vinc.join(rais_vinc_escol,['ID_CNPJ_CEI', 'ANO'],'outer')
rais_info_vinc = rais_info_vinc.join(rais_vinc_fetaria,['ID_CNPJ_CEI', 'ANO'],'outer')
rais_info_vinc = rais_info_vinc.join(rais_vinc_aprend,['ID_CNPJ_CEI', 'ANO'],'outer')
rais_info_vinc = rais_info_vinc.join(rais_salario,['ID_CNPJ_CEI', 'ANO'],'outer')

# COMMAND ----------

#display(rais_info_vinc)

# COMMAND ----------

rais_estab = rais_estab.join(rais_info_vinc,['ID_CNPJ_CEI', 'ANO'],'left')

# COMMAND ----------

rais_estab_2018 = (rais_estab
              .where((f.col('ANO').isin(2018) & (f.col('QT_VINC_ATIV') > 0) & (f.col('FL_IND_RAIS_NEGAT') == 0)))
              .select('ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_IND_SIMPLES', 'CD_CNAE20_SUBCLASSE', 'QT_VINC_ATIV', 'CD_IBGE_SUBSETOR','CD_MUNICIPIO', 'ID_CEI_VINCULADO', 'CD_NATUREZA_JURIDICA','NR_FUNC_INDIGENA','NR_FUNC_BRANCA','NR_FUNC_PRETA','NR_FUNC_AMARELA','NR_FUNC_PARDA', 'NR_FUNC_NIDENT','NR_FUNC_NDECLARADA','NR_FUNC_MASCULINO','NR_FUNC_FEMININO','NR_FUNC_SEM_INSTRUCAO','NR_FUNC_SABE_LER_ESCREVER'
              ,'NR_FUNC_PM_EF_COMPLETO','NR_FUNC_PM_EM_COMPLETO','NR_FUNC_SUPERIO_COMPLETO_MAIS','NR_FUNC_15_A_17ANOS','NR_FUNC_18_A_24ANOS'
              ,'NR_FUNC_25_A_29ANOS','NR_FUNC_30_A_39ANOS','NR_FUNC_40_A_49ANOS','NR_FUNC_50_A_64ANOS','NR_FUNC_65_MAIS','NR_FUNC_APRENDIZ'
              ,'MASSA_SALARIAL','REMUNERACAO_MEDIA')
              .withColumnRenamed('ID_CNPJ_CEI', 'CD_CNPJ'))

rais_estab_2017 = (rais_estab
              .where((f.col('ANO').isin(2017) & (f.col('QT_VINC_ATIV') > 0) & (f.col('FL_IND_RAIS_NEGAT') == 0)))
              .select('ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_IND_SIMPLES', 'CD_CNAE20_SUBCLASSE', 'QT_VINC_ATIV', 'CD_IBGE_SUBSETOR','CD_MUNICIPIO', 'ID_CEI_VINCULADO', 'CD_NATUREZA_JURIDICA','NR_FUNC_INDIGENA','NR_FUNC_BRANCA','NR_FUNC_PRETA','NR_FUNC_AMARELA','NR_FUNC_PARDA', 'NR_FUNC_NIDENT','NR_FUNC_NDECLARADA','NR_FUNC_MASCULINO','NR_FUNC_FEMININO','NR_FUNC_SEM_INSTRUCAO','NR_FUNC_SABE_LER_ESCREVER'
              ,'NR_FUNC_PM_EF_COMPLETO','NR_FUNC_PM_EM_COMPLETO','NR_FUNC_SUPERIO_COMPLETO_MAIS','NR_FUNC_15_A_17ANOS','NR_FUNC_18_A_24ANOS'
              ,'NR_FUNC_25_A_29ANOS','NR_FUNC_30_A_39ANOS','NR_FUNC_40_A_49ANOS','NR_FUNC_50_A_64ANOS','NR_FUNC_65_MAIS','NR_FUNC_APRENDIZ'
              ,'MASSA_SALARIAL','REMUNERACAO_MEDIA')
              .withColumnRenamed('ID_CNPJ_CEI', 'CD_CNPJ'))

rais_estab_2016 = (rais_estab
              .where((f.col('ANO').isin(2016) & (f.col('QT_VINC_ATIV') > 0) & (f.col('FL_IND_RAIS_NEGAT') == 0)))
              .select('ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_IND_SIMPLES', 'CD_CNAE20_SUBCLASSE', 'QT_VINC_ATIV', 'CD_IBGE_SUBSETOR','CD_MUNICIPIO', 'ID_CEI_VINCULADO', 'CD_NATUREZA_JURIDICA','NR_FUNC_INDIGENA','NR_FUNC_BRANCA','NR_FUNC_PRETA','NR_FUNC_AMARELA','NR_FUNC_PARDA', 'NR_FUNC_NIDENT','NR_FUNC_NDECLARADA','NR_FUNC_MASCULINO','NR_FUNC_FEMININO','NR_FUNC_SEM_INSTRUCAO','NR_FUNC_SABE_LER_ESCREVER'
              ,'NR_FUNC_PM_EF_COMPLETO','NR_FUNC_PM_EM_COMPLETO','NR_FUNC_SUPERIO_COMPLETO_MAIS','NR_FUNC_15_A_17ANOS','NR_FUNC_18_A_24ANOS'
              ,'NR_FUNC_25_A_29ANOS','NR_FUNC_30_A_39ANOS','NR_FUNC_40_A_49ANOS','NR_FUNC_50_A_64ANOS','NR_FUNC_65_MAIS','NR_FUNC_APRENDIZ'
              ,'MASSA_SALARIAL','REMUNERACAO_MEDIA')
              .withColumnRenamed('ID_CNPJ_CEI', 'CD_CNPJ'))

# COMMAND ----------

estbl_cnpj = (cadastro_estbl.select('CD_CNPJ','CD_CNPJ_BASICO'))

estbl_cnpj_2018 = (estbl_cnpj.join(rais_estab_2018, 'CD_CNPJ', how='left'))
estbl_cnpj_2017 = (estbl_cnpj_2018
.where(f.col('CD_CNAE20_SUBCLASSE').isNull())
.select('CD_CNPJ','CD_CNPJ_BASICO')
.join(rais_estab_2017, 'CD_CNPJ', how='left'))
estbl_cnpj_2016 = (estbl_cnpj_2017
.where(f.col('CD_CNAE20_SUBCLASSE').isNull())
.select('CD_CNPJ','CD_CNPJ_BASICO')
.join(rais_estab_2016, 'CD_CNPJ', how='left'))


estbl_cnpj_2018 = (estbl_cnpj_2018.where(f.col('CD_CNAE20_SUBCLASSE').isNotNull()).withColumn('ANO_RAIS', f.lit(2018)))
estbl_cnpj_2017 = (estbl_cnpj_2017.where(f.col('CD_CNAE20_SUBCLASSE').isNotNull()).withColumn('ANO_RAIS', f.lit(2017)))
estbl_cnpj_2016 = (estbl_cnpj_2016.where(f.col('CD_CNAE20_SUBCLASSE').isNotNull()).withColumn('ANO_RAIS', f.lit(2016)))

rais_estab_infos = estbl_cnpj_2018.union(estbl_cnpj_2017)
rais_estab_infos = rais_estab_infos.union(estbl_cnpj_2016)

rais_estab_infos = rais_estab_infos.drop('CD_CNPJ_BASICO')

# COMMAND ----------

rais_porte = (rais_estab
              .where((f.col('ANO').isin(2016,2017,2018)) & (f.col('QT_VINC_ATIV') > 0) & (f.col('FL_IND_RAIS_NEGAT') == 0))
              .withColumn('CD_CNAE20_DIVISAO',f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2)))

# COMMAND ----------

duplicados = (rais_porte
              .groupby(['ID_CNPJ_CEI', 'ANO'])
              .agg(f.count(f.lit(1)).alias('DUPLICADOS'))
              .orderBy('DUPLICADOS', ascending=False))
#display(duplicados.where(f.col('DUPLICADOS')> 1))

# COMMAND ----------

rais_porte = rais_porte.join(duplicados, on=['ID_CNPJ_CEI', 'ANO'], how='left')
rais_porte = (rais_porte.where(f.col('DUPLICADOS') == f.lit(1)).drop('DUPLICADOS'))

# COMMAND ----------

rais_porte = rais_porte.withColumn('CD_CNAE20_DIVISAO', rais_porte['CD_CNAE20_DIVISAO'].cast(IntegerType()))

# COMMAND ----------

rais_porte = (rais_porte
.withColumn('TIPO_PORTE',
f.when((f.col('CD_CNAE20_DIVISAO') >= f.lit(5)) & (f.col('CD_CNAE20_DIVISAO') <= f.lit(43)), f.lit('Industria'))
.otherwise(
  f.when((f.col('CD_CNAE20_DIVISAO') < f.lit(5)) | (f.col('CD_CNAE20_DIVISAO') > f.lit(43)), f.lit('Comercio_Servicos')))))

# COMMAND ----------

flg_ano_rais_porte = (rais_porte
              .groupby('ID_CNPJ_CEI')
              .pivot('ANO')
              .agg(f.count(f.lit(1)).alias('ANOS_CNPJ')))

# COMMAND ----------

rais_porte_16_18 = (rais_porte
.select('ID_CNPJ_CEI', 'TIPO_PORTE', 'QT_VINC_ATIV', 'ANO')
.join(flg_ano_rais_porte, on = 'ID_CNPJ_CEI', how='left'))

# COMMAND ----------

#peso 1 2018, 2017 e 2016 - 0.4, 0.3 e 0.3

rais_porte_peso = (rais_porte_16_18
.where((f.col('2016')==f.lit(1)) & (f.col('2017')==f.lit(1)) & (f.col('2018')==f.lit(1)))
.withColumn('QT_VINC_ATIV_peso1', 
                                 f.when(f.col('ANO') == f.lit(2018), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.4))))
                                 .otherwise(
                                 f.when(f.col('ANO') == f.lit(2017), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.3))))
                                 .otherwise(
                                 f.when(f.col('ANO') == f.lit(2016), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.3)))))))
.groupby('ID_CNPJ_CEI')
.agg(f.round((f.sum('QT_VINC_ATIV_peso1')/(0.4+0.3+0.3)), 2).alias('QT_VINC_ATIV_media_pond')))

rais_porte_peso_tp1 = (rais_porte_16_18
.where(f.col('ANO')==f.lit(2018))
.select('ID_CNPJ_CEI', 'TIPO_PORTE'))

rais_porte_peso_tp1 = rais_porte_peso.join(rais_porte_peso_tp1, on = 'ID_CNPJ_CEI', how = 'left')


# COMMAND ----------

#peso 2 2018 e 2017 - 0.6 e 0.4

rais_porte_peso = (rais_porte_16_18
.where((f.col('2016').isNull()) & (f.col('2017')==1) & (f.col('2018')==1))
.withColumn('QT_VINC_ATIV_peso2', 
                                 f.when(f.col('ANO') == f.lit(2018), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.6))))
                                 .otherwise(
                                 f.when(f.col('ANO') == f.lit(2017), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.4))))))
.groupby('ID_CNPJ_CEI')
.agg(f.round((f.sum('QT_VINC_ATIV_peso2')/(0.6+0.4)), 2).alias('QT_VINC_ATIV_media_pond')))

rais_porte_peso_tp2 = (rais_porte_16_18
.where(f.col('ANO')==f.lit(2018))
.select('ID_CNPJ_CEI', 'TIPO_PORTE'))

rais_porte_peso_tp2 = rais_porte_peso.join(rais_porte_peso_tp2, on = 'ID_CNPJ_CEI', how = 'left')


# COMMAND ----------

#peso 3 2018 e 2016 - 0.8 e 0.2

rais_porte_peso = (rais_porte_16_18
.where((f.col('2016')==f.lit(1)) & (f.col('2017').isNull()) & (f.col('2018')==f.lit(1)))
.withColumn('QT_VINC_ATIV_peso2', 
                                 f.when(f.col('ANO') == f.lit(2018), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.8))))
                                 .otherwise(
                                 f.when(f.col('ANO') == f.lit(2016), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.2))))))
.groupby('ID_CNPJ_CEI')
.agg(f.round((f.sum('QT_VINC_ATIV_peso2')/(0.8+0.2)), 2).alias('QT_VINC_ATIV_media_pond')))

rais_porte_peso_tp3 = (rais_porte_16_18
.where(f.col('ANO')==f.lit(2018))
.select('ID_CNPJ_CEI', 'TIPO_PORTE'))

rais_porte_peso_tp3 = rais_porte_peso.join(rais_porte_peso_tp3, on = 'ID_CNPJ_CEI', how = 'left')


# COMMAND ----------

#peso 4 2017 e 2016 - 0.6 e 0.4

rais_porte_peso = (rais_porte_16_18
.where((f.col('2016')==f.lit(1)) & (f.col('2017')==f.lit(1)) & (f.col('2018').isNull()))
.withColumn('QT_VINC_ATIV_peso2', 
                                 f.when(f.col('ANO') == f.lit(2017), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.6))))
                                 .otherwise(
                                 f.when(f.col('ANO') == f.lit(2016), f.lit((f.col('QT_VINC_ATIV') * f.lit(0.4))))))
.groupby('ID_CNPJ_CEI')
.agg(f.round((f.sum('QT_VINC_ATIV_peso2')/(0.6+0.4)), 2).alias('QT_VINC_ATIV_media_pond')))

rais_porte_peso_tp4 = (rais_porte_16_18
.where(f.col('ANO')==f.lit(2017))
.select('ID_CNPJ_CEI', 'TIPO_PORTE'))

rais_porte_peso_tp4 = rais_porte_peso.join(rais_porte_peso_tp4, on = 'ID_CNPJ_CEI', how = 'left')


# COMMAND ----------

#peso 5 dado para apenas um ano 

rais_porte_peso = (rais_porte_16_18
.where(((f.col('2016').isNull()) & (f.col('2017').isNull()) & (f.col('2018')==1)) | ((f.col('2016').isNull()) & (f.col('2017')==1) & (f.col('2018').isNull())) | ((f.col('2016')==1) & (f.col('2017').isNull()) & (f.col('2018').isNull())))
.select('ID_CNPJ_CEI'))

rais_porte_peso_tp5 = (rais_porte_16_18
.select('ID_CNPJ_CEI','QT_VINC_ATIV','TIPO_PORTE')
.withColumnRenamed('QT_VINC_ATIV', 'QT_VINC_ATIV_media_pond'))

rais_porte_peso_tp5 = rais_porte_peso.join(rais_porte_peso_tp5, on = 'ID_CNPJ_CEI', how = 'left')

# COMMAND ----------

rais_porte = rais_porte_peso_tp1.union(rais_porte_peso_tp2)
rais_porte = rais_porte.union(rais_porte_peso_tp3)
rais_porte = rais_porte.union(rais_porte_peso_tp4)
rais_porte = rais_porte.union(rais_porte_peso_tp5)

# COMMAND ----------

rais_porte = (rais_porte.withColumn('PORTE_SEBRAE', 
                                 f.when(((f.col('TIPO_PORTE') == f.lit('Comercio_Servicos')) & (f.col('QT_VINC_ATIV_media_pond') <= f.lit(9))) | ((f.col('TIPO_PORTE') == f.lit('Industria')) & (f.col('QT_VINC_ATIV_media_pond') <= f.lit(19))), f.lit('Micro'))
                                 .otherwise(
                                 f.when(((f.col('TIPO_PORTE') == f.lit('Comercio_Servicos')) & (f.col('QT_VINC_ATIV_media_pond') > f.lit(9)) & (f.col('QT_VINC_ATIV_media_pond') <= f.lit(49))) | ((f.col('TIPO_PORTE') == f.lit('Industria')) & (f.col('QT_VINC_ATIV_media_pond') > f.lit(19)) & (f.col('QT_VINC_ATIV_media_pond') <= f.lit(99))), f.lit('Pequena'))
                                 .otherwise(
                                 f.when(((f.col('TIPO_PORTE') == f.lit('Comercio_Servicos')) & (f.col('QT_VINC_ATIV_media_pond') > f.lit(49)) & (f.col('QT_VINC_ATIV_media_pond') <= f.lit(99))) | ((f.col('TIPO_PORTE') == f.lit('Industria')) & (f.col('QT_VINC_ATIV_media_pond') > f.lit(99)) & (f.col('QT_VINC_ATIV_media_pond') <= f.lit(499))), f.lit('Media'))
                                 .otherwise(
                                 f.when(((f.col('TIPO_PORTE') == f.lit('Comercio_Servicos')) & (f.col('QT_VINC_ATIV_media_pond') > f.lit(99))) | ((f.col('TIPO_PORTE') == f.lit('Industria')) & (f.col('QT_VINC_ATIV_media_pond') > f.lit(499))), f.lit('Grande')))))))

# COMMAND ----------

rais_porte = (rais_porte.withColumn('PORTE_EURO_ESTAT', 
                                 f.when(f.col('QT_VINC_ATIV_media_pond') < f.lit(10), f.lit('Micro empresa'))
                                 .otherwise(
                                 f.when((f.col('QT_VINC_ATIV_media_pond') >= f.lit(10)) & (f.col('QT_VINC_ATIV_media_pond') < f.lit(50)), f.lit('Pequena empresa'))
                                 .otherwise(
                                 f.when((f.col('QT_VINC_ATIV_media_pond') >= f.lit(50)) & (f.col('QT_VINC_ATIV_media_pond') < f.lit(250)), f.lit('Média empresa'))
                                 .otherwise(
                                 f.when(f.col('QT_VINC_ATIV_media_pond') >= f.lit(250), f.lit('Grande empresa')))))))

# COMMAND ----------

rais_estab_final = (rais_estab_infos
.withColumnRenamed('CD_CNPJ', 'ID_CNPJ_CEI')
.join(rais_porte, 'ID_CNPJ_CEI', how='left'))

# COMMAND ----------

duplicados = (rais_estab_final
        .groupby(['ID_CNPJ_CEI'])
        .agg(f.count(f.lit(1)).alias('DUPLICADOS'))
        .orderBy('DUPLICADOS', ascending=False))
#display(duplicados.where(f.col('DUPLICADOS')> 1))

# COMMAND ----------

rais_estab_final = rais_estab_final.join(duplicados, on='ID_CNPJ_CEI', how='left')
rais_estab_final = (rais_estab_final.where(f.col('DUPLICADOS') == f.lit(1)).drop('DUPLICADOS'))

# COMMAND ----------

cadastro_estbl = (cadastro_estbl
                  .select('CD_CNPJ', 'CD_CNPJ_BASICO', 'CD_SIT_CADASTRAL', 'CD_CNAE20_SUBCLASSE_PRINCIPAL', 'ANO_SIT_CADASTRAL','DT_SIT_CADASTRAL','ANO_INICIO_ATIV', 'DT_INICIO_ATIV', 'CD_MATRIZ_FILIAL', 'DS_MATRIZ_FILIAL', 'NM_FANTASIA', 'DS_SIT_CADASTRAL', 'CD_MOTIVO_SIT_CADASTRAL', 'CD_CNAE20_SUBCLASSE_SECUNDARIA', 'NM_TIPO_LOGRADOURO', 'NM_LOGRADOURO', 'NM_NUMERO_ESTBL', 'NM_COMPLEMENTO', 'NM_BAIRRO', 'CD_CEP', 'SG_UF', 'CD_MUN', 'NM_DDD_1', 'NM_TELEFONE_1', 'NM_DDD_2', 'NM_TELEFONE_2', 'NM_EMAIL')
                  .withColumn('CD_CNAE20_SUBCLASSE_PRINCIPAL',f.lpad(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'),7,'0'))
                  .withColumn('CD_CNAE20_DIVISAO',f.substring(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'),1,2))
                  .withColumn('CD_CNAE20_CLASSE',f.substring(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'),1,5))
                  .withColumnRenamed('CD_CNAE20_SUBCLASSE_PRINCIPAL', 'CD_CNAE20_SUBCLASSE_RFB')
                  .withColumnRenamed('CD_MUN', 'CD_MUNICIPIO_SIAFI_RFB'))

# COMMAND ----------


cadastro_simples = (cadastro_simples
                    .select('CD_CNPJ_BASICO', 'CD_OPCAO_SIMPLES', 'CD_OPCAO_MEI', 'DT_OPCAO_SIMPLES', 'DT_EXCLUSAO_SIMPLES', 'DT_OPCAO_MEI', 'DT_EXCLUSAO_MEI')
                    .withColumnRenamed('CD_OPCAO_SIMPLES', 'CD_OPCAO_SIMPLES_RFB'))
                    
rais_estab_final = (rais_estab_final
                  .withColumnRenamed('ID_CNPJ_CEI', 'CD_CNPJ')
                  .withColumnRenamed('FL_IND_SIMPLES', 'FL_IND_SIMPLES_RAIS')
                  .withColumnRenamed('CD_CNAE20_SUBCLASSE', 'CD_CNAE20_SUBCLASSE_RAIS')
                  .withColumnRenamed('ID_RAZAO_SOCIAL', 'ID_RAZAO_SOCIAL_RAIS')
                  .withColumnRenamed('CD_NATUREZA_JURIDICA', 'CD_NATUREZA_JURIDICA_RAIS')
                  .withColumnRenamed('CD_MUNICIPIO', 'CD_MUNICIPIO_RAIS'))

cadastro_empresa = (cadastro_empresa
                  .select('CD_CNPJ_BASICO', 'CD_PORTE_EMPRESA', 'DS_PORTE_EMPRESA', 'NM_RAZAO_SOCIAL', 'CD_QUALIF_RESP',
'VL_CAPITAL_SOCIAL','CD_NATUREZA_JURIDICA')
                  .withColumnRenamed('NM_RAZAO_SOCIAL', 'NM_RAZAO_SOCIAL_RECEITA_EMPRESA')
                  .withColumnRenamed('CD_QUALIF_RESP', 'CD_QUALIF_RESP_EMPRESA')
                  .withColumnRenamed('CD_NATUREZA_JURIDICA', 'CD_NATUREZA_JURIDICA_RFB')
                  .withColumnRenamed('VL_CAPITAL_SOCIAL', 'VL_CAPITAL_SOCIAL_EMPRESA')
                   )

# COMMAND ----------

base_unica_estabelecimento = cadastro_estbl.join(cadastro_simples , 'CD_CNPJ_BASICO', how='left')
base_unica_estabelecimento = base_unica_estabelecimento.join(cadastro_empresa , 'CD_CNPJ_BASICO', how='left')
base_unica_estabelecimento = base_unica_estabelecimento.join(rais_estab_final, 'CD_CNPJ', how='left')

# COMMAND ----------

#base_unica_estabelecimento.count()

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('TIPO_PORTE',
f.when((f.col('CD_CNAE20_DIVISAO') >= f.lit(5)) & (f.col('CD_CNAE20_DIVISAO') <= f.lit(43)), f.lit('Industria'))
.otherwise(
  f.when((f.col('CD_CNAE20_DIVISAO') < f.lit(5)) | (f.col('CD_CNAE20_DIVISAO') > f.lit(43)), f.lit('Comercio_Servicos')))))

# COMMAND ----------

#base_unica_estabelecimento.count()

# COMMAND ----------

#display(base_unica_estabelecimento.where(f.col('TIPO_PORTE').isNull()))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
        .withColumn('CD_OPCAO_SIMPLES_RFB',
                    f.when((f.col('CD_OPCAO_SIMPLES_RFB')=='S'), f.lit(1))
                    .otherwise(
                      f.when((f.col('CD_OPCAO_SIMPLES_RFB')=='N'), f.lit(0)))))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
        .withColumn('CD_OPCAO_MEI',
                    f.when((f.col('CD_OPCAO_MEI')=='S'), f.lit(1))
                    .otherwise(
                      f.when((f.col('CD_OPCAO_MEI')=='N'), f.lit(0)))))

# COMMAND ----------

#duplicados = (base_unica_estabelecimento.groupby(['CD_CNPJ']).agg(f.count(f.lit(1)).alias('DUPLICADOS')).orderBy('DUPLICADOS', ascending=False))
#display(duplicados.where(f.col('DUPLICADOS')> 1))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento.drop_duplicates(['CD_CNPJ']))
#display(base_unica_estabelecimento.count())

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
                   .withColumn('FL_DADOS_RAIS',
                               f.when((f.col('ID_RAZAO_SOCIAL_RAIS').isNull()), f.lit(0))
                               .otherwise(
                                 f.when((f.col('ID_RAZAO_SOCIAL_RAIS').isNotNull()), f.lit(1))))
                 )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento SIAFI

# COMMAND ----------

uds_siafi_path = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['uds_siafi_path'])
siafi = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(uds_siafi_path, mode="FAILFAST")

# COMMAND ----------

siafi = (siafi
          .select('codigo_siafi', 'codigo_ibge')
          .withColumnRenamed('codigo_siafi', 'CD_MUNICIPIO_SIAFI_RFB')
          .withColumnRenamed('codigo_ibge', 'CD_MUNICIPIO_RFB')
          .drop_duplicates())

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('CD_MUNICIPIO_SIAFI_RFB', base_unica_estabelecimento['CD_MUNICIPIO_SIAFI_RFB'].cast(IntegerType()))
.join(siafi, 'CD_MUNICIPIO_SIAFI_RFB', how='left'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento Empresas Exportadoras

# COMMAND ----------

#### CNPJ's Exportadores
exportadoras = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['exportadoras'])
EXP_CNPJ = spark.read.parquet(exportadoras)

# COMMAND ----------

#display(EXP_CNPJ)

# COMMAND ----------

EXP_CNPJ = (EXP_CNPJ.withColumn('CD_CNPJ',f.lpad(f.col('CNPJ'),14,'0'))
                    .withColumn('FL_EMPRESA_EXP', f.lit(1))
                    .select('CD_CNPJ', 'FL_EMPRESA_EXP'))

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(EXP_CNPJ, 'CD_CNPJ', how='left')

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('FL_EMPRESA_EXP',
f.when((f.col('FL_EMPRESA_EXP').isNull()), f.lit(0))
.otherwise(
f.when((f.col('FL_EMPRESA_EXP').isNotNull()), f.lit(1)))))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento Empresas Importadoras

# COMMAND ----------


importadoras = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['importadoras'])
IMP_CNPJ = spark.read.parquet(importadoras)

# COMMAND ----------

#display(IMP_CNPJ)

# COMMAND ----------

IMP_CNPJ = (IMP_CNPJ.withColumn('CD_CNPJ',f.lpad(f.col('CNPJ'),14,'0'))
                    .withColumn('FL_EMPRESA_IMP', f.lit(1))
                    .select('CD_CNPJ', 'FL_EMPRESA_IMP'))

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(IMP_CNPJ, 'CD_CNPJ', how='left')

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('FL_EMPRESA_IMP',
f.when((f.col('FL_EMPRESA_IMP').isNull()), f.lit(0))
.otherwise(
f.when((f.col('FL_EMPRESA_IMP').isNotNull()), f.lit(1)))))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento Intensidade Tecnológica

# COMMAND ----------

class_inten_tec = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['class_inten_tec'])
intensidade_tecnol = spark.read.parquet(class_inten_tec)
intensidade_tecnol = intensidade_tecnol.drop('dh_insercao_trs','kv_process_control')

# COMMAND ----------

#display(intensidade_tecnol)

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(intensidade_tecnol , 'CD_CNAE20_DIVISAO', how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento CNAE's Contribuintes

# COMMAND ----------

#### CNAE's Contribuintes
var_source_cnaes_contrib = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['cnaes_contrib'])
contribuintes = spark.read.parquet(var_source_cnaes_contrib)
contribuintes = (contribuintes
                 .withColumnRenamed('CD_CNAE20_SUBCLASSE', 'CD_CNAE20_SUBCLASSE_RFB')
                 .drop('dh_insercao_trs', 'kv_process_control')
                 )

# COMMAND ----------

#display(contribuintes)

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(contribuintes, 'CD_CNAE20_SUBCLASSE_RFB', how='left')

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('FL_CONTRIBUINTE',
f.when(((f.col('FL_CONTRIBUINTE').isNull()) | (f.col('CD_OPCAO_SIMPLES_RFB') == 1)) | ((f.col('FL_CONTRIBUINTE').isNull()) & (f.col('CD_OPCAO_SIMPLES_RFB').isNull())), f.lit(0))
.otherwise(
f.when(((f.col('FL_CONTRIBUINTE').isNotNull()) & (f.col('CD_OPCAO_SIMPLES_RFB') == 0)) | ((f.col('FL_CONTRIBUINTE').isNotNull()) & (f.col('CD_OPCAO_SIMPLES_RFB').isNull())), f.lit(1)))))

# COMMAND ----------

#display(base_unica_estabelecimento.where((f.col('FL_CONTRIBUINTE') == 1) & (f.col('CD_OPCAO_SIMPLES_RFB') == 1)))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('CONTRIBUICAO_SESI',
f.when(f.col('FL_CONTRIBUINTE') == 1, f.round(f.col('MASSA_SALARIAL')*f.lit(0.015), 2))
.otherwise(
f.when(f.col('FL_CONTRIBUINTE') == 0, f.lit(0)))))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('CONTRIBUICAO_SENAI',
f.when(f.col('FL_CONTRIBUINTE') == 1, f.round(f.col('MASSA_SALARIAL')*f.lit(0.01), 2))
.otherwise(
f.when(f.col('FL_CONTRIBUINTE') == 0, f.lit(0)))))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento NCM

# COMMAND ----------

#### NCM
var_source_NCM = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['cnae_ncm'])
NCM = spark.read.parquet(var_source_NCM)
NCM = NCM.drop('dh_insercao_trs', 'kv_process_control')

# COMMAND ----------

#display(NCM)

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(NCM , 'CD_CNAE20_CLASSE', how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento ISIC

# COMMAND ----------

#### ISIC
var_source_isic = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['cnae_isic'])
isic = spark.read.parquet(var_source_isic)
isic = isic.drop('dh_insercao_trs', 'kv_process_control')

# COMMAND ----------

#display(isic)

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(isic, 'CD_CNAE20_CLASSE', how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cruzamento Natureza Jurídica

# COMMAND ----------

#### ISIC
nat_juridica = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=dls['folders']['trusted'], file_folder=table['nat_juridica'])
nat_juridica = spark.read.parquet(nat_juridica)
nat_juridica = nat_juridica.drop('dh_insercao_trs', 'kv_process_control')

# COMMAND ----------

#display(nat_juridica)

# COMMAND ----------

nat_juridica_rfb = (nat_juridica
        .withColumnRenamed('CD_NAT_JURIDICA', 'CD_NATUREZA_JURIDICA_RFB')
        .withColumnRenamed('DS_NAT_JURIDICA', 'DS_NAT_JURIDICA_RFB'))
nat_juridica_rais = (nat_juridica
        .withColumn('CD_NATUREZA_JURIDICA_RAIS', f.col('CD_NAT_JURIDICA'))
        .withColumn('DS_NAT_JURIDICA_RAIS', f.col('DS_NAT_JURIDICA'))
        .select('CD_NATUREZA_JURIDICA_RAIS','DS_NAT_JURIDICA_RAIS'))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
                              .withColumn("CD_NATUREZA_JURIDICA_RAIS",f.col("CD_NATUREZA_JURIDICA_RAIS").cast(StringType())))

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(nat_juridica_rfb, 'CD_NATUREZA_JURIDICA_RFB', how='left')

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(nat_juridica_rais, 'CD_NATUREZA_JURIDICA_RAIS', how='left')

# COMMAND ----------

#display(base_unica_estabelecimento)

# COMMAND ----------

#display(base_unica_estabelecimento.count())

# COMMAND ----------

df = tcf.add_control_fields(base_unica_estabelecimento, adf, layer="biz")

var_sink_cnpjs_rfb = "{adl_path}{biz}/{path_destination}/".format(adl_path=var_adls_uri, biz=dls['folders']['business'], path_destination=table["path_destination"])
#df.write.format('parquet').save(var_sink_cnpjs_rfb_rais, header = True, mode='overwrite')

# COMMAND ----------

#df = df.drop(df.columns[-15])

# COMMAND ----------

df.write.format('parquet').save(var_sink_cnpjs_rfb, header = True, mode='overwrite')

# COMMAND ----------


