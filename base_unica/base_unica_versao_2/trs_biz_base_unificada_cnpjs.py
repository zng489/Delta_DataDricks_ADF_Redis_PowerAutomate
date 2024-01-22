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

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

trs = dls['folders']['trusted']
biz = dls['folders']['business']

# COMMAND ----------

table['cadastro_estbl_path']

# COMMAND ----------

table['cadastro_empresa_path']

# COMMAND ----------

table['cadastro_simples_path']

# COMMAND ----------

table['rais_estab_path']

# COMMAND ----------

table['siafi']

# COMMAND ----------

var_source_estbl_f = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['cadastro_estbl_path'])
cadastro_estbl = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(var_source_estbl_f, mode="FAILFAST")


var_source_estbl_f = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['cadastro_empresa_path'])
cadastro_empresa = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(var_source_estbl_f, mode="FAILFAST")

var_source_estbl_f = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['cadastro_simples_path'])
cadastro_simples = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(var_source_estbl_f, mode="FAILFAST")


var_source_estbl_f = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['rais_estab_path'])
rais_estab = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(var_source_estbl_f, mode="FAILFAST")
rais_estab = rais_estab.withColumn('CD_CNAE20_SUBCLASSE', f.lpad(f.col('CD_CNAE20_SUBCLASSE'),7,'0'))


uds_siafi_path = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['siafi'])
siafi = spark.read.format("parquet").option("header","true").option("encoding", "ISO-8859-1").load(uds_siafi_path, mode="FAILFAST")

# COMMAND ----------

cadastro_estbl.cache()

cadastro_empresa.cache()

cadastro_simples.cache()

rais_estab.cache()

# COMMAND ----------

cadastro_estbl = (cadastro_estbl.where((f.col('CD_SIT_CADASTRAL').isin('01', '02', '03')))
                 .withColumn('ANO_SIT_CADASTRAL', f.substring(f.col('DT_SIT_CADASTRAL'),1,4))
                 .withColumn('ANO_INICIO_ATIV', f.substring(f.col('DT_INICIO_ATIV'),1,4)))

# COMMAND ----------

cadastro_estbl = (cadastro_estbl.where((f.col('ANO_INICIO_ATIV')!='1199')))

# COMMAND ----------

cadastro_estbl = (cadastro_estbl.where((f.col('ANO_INICIO_ATIV')!='1601')))

# COMMAND ----------

rais_estab_2018 = (rais_estab
              .where((f.col('ANO').isin(2018) & (f.col('QT_VINC_ATIV') > 0) & (f.col('FL_IND_RAIS_NEGAT') == 0)))
              .select('ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_IND_SIMPLES', 'CD_CNAE20_SUBCLASSE', 'QT_VINC_ATIV', 'CD_IBGE_SUBSETOR','CD_MUNICIPIO', 'ID_CEI_VINCULADO')
              .withColumnRenamed('ID_CNPJ_CEI', 'CD_CNPJ'))

rais_estab_2017 = (rais_estab
              .where((f.col('ANO').isin(2017) & (f.col('QT_VINC_ATIV') > 0) & (f.col('FL_IND_RAIS_NEGAT') == 0)))
              .select('ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_IND_SIMPLES', 'CD_CNAE20_SUBCLASSE', 'QT_VINC_ATIV', 'CD_IBGE_SUBSETOR','CD_MUNICIPIO', 'ID_CEI_VINCULADO')
              .withColumnRenamed('ID_CNPJ_CEI', 'CD_CNPJ'))

rais_estab_2016 = (rais_estab
              .where((f.col('ANO').isin(2016) & (f.col('QT_VINC_ATIV') > 0) & (f.col('FL_IND_RAIS_NEGAT') == 0)))
              .select('ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_IND_SIMPLES', 'CD_CNAE20_SUBCLASSE', 'QT_VINC_ATIV', 'CD_IBGE_SUBSETOR','CD_MUNICIPIO', 'ID_CEI_VINCULADO')
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

# COMMAND ----------

rais_estab_final = rais_estab_final.join(duplicados, on='ID_CNPJ_CEI', how='left')
rais_estab_final = (rais_estab_final.where(f.col('DUPLICADOS') == f.lit(1)).drop('DUPLICADOS'))

# COMMAND ----------

cadastro_estbl = (cadastro_estbl
                  .select('CD_CNPJ', 'CD_CNPJ_BASICO', 'CD_SIT_CADASTRAL', 'CD_CNAE20_SUBCLASSE_PRINCIPAL','ANO_SIT_CADASTRAL','DT_SIT_CADASTRAL','ANO_INICIO_ATIV', 'DT_INICIO_ATIV', 'CD_MATRIZ_FILIAL', 'DS_MATRIZ_FILIAL', 'NM_FANTASIA', 'DS_SIT_CADASTRAL', 'CD_MOTIVO_SIT_CADASTRAL', 'CD_CNAE20_SUBCLASSE_SECUNDARIA', 'NM_TIPO_LOGRADOURO', 'NM_LOGRADOURO', 'NM_NUMERO_ESTBL', 'NM_COMPLEMENTO', 'NM_BAIRRO', 'CD_CEP', 'SG_UF', 'CD_MUN', 'NM_DDD_1', 'NM_TELEFONE_1', 'NM_DDD_2', 'NM_TELEFONE_2', 'NM_EMAIL')
                  .withColumn('CD_CNAE20_SUBCLASSE_PRINCIPAL',f.lpad(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'),7,'0'))
                  .withColumn('CD_CNAE20_DIVISAO',f.substring(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'),1,2))
                  .withColumn('CD_CNAE20_CLASSE',f.substring(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'),1,5))
                  .withColumnRenamed('CD_CNAE20_SUBCLASSE_PRINCIPAL', 'CD_CNAE20_SUBCLASSE_RFB')
                  .withColumnRenamed('CD_MUN', 'CD_MUNICIPIO_SIAFI_RFB'))

cadastro_simples = (cadastro_simples
                    .select('CD_CNPJ_BASICO', 'CD_OPCAO_SIMPLES', 'CD_OPCAO_MEI', 'DT_OPCAO_SIMPLES', 'DT_EXCLUSAO_SIMPLES', 'DT_OPCAO_MEI', 'DT_EXCLUSAO_MEI')
                    .withColumnRenamed('CD_OPCAO_SIMPLES', 'CD_OPCAO_SIMPLES_RFB'))
                    
rais_estab_final = (rais_estab_final
                  .withColumnRenamed('ID_CNPJ_CEI', 'CD_CNPJ')
                  .withColumnRenamed('FL_IND_SIMPLES', 'FL_IND_SIMPLES_RAIS')
                  .withColumnRenamed('CD_CNAE20_SUBCLASSE', 'CD_CNAE20_SUBCLASSE_RAIS')
                  .withColumnRenamed('ID_RAZAO_SOCIAL', 'ID_RAZAO_SOCIAL_RAIS')
                  .withColumnRenamed('CD_MUNICIPIO', 'CD_MUNICIPIO_RAIS'))

cadastro_empresa = (cadastro_empresa
                  .select('CD_CNPJ_BASICO', 'CD_PORTE_EMPRESA', 'DS_PORTE_EMPRESA', 'NM_RAZAO_SOCIAL', 'CD_QUALIF_RESP',
'VL_CAPITAL_SOCIAL')
                  .withColumnRenamed('NM_RAZAO_SOCIAL', 'NM_RAZAO_SOCIAL_RECEITA_EMPRESA')
                   .withColumnRenamed('CD_QUALIF_RESP', 'CD_QUALIF_RESP_EMPRESA')
                    .withColumnRenamed('VL_CAPITAL_SOCIAL', 'VL_CAPITAL_SOCIAL_EMPRESA')
                   )

# COMMAND ----------

base_unica_estabelecimento = cadastro_estbl.join(cadastro_simples , 'CD_CNPJ_BASICO', how='left')
base_unica_estabelecimento = base_unica_estabelecimento.join(cadastro_empresa , 'CD_CNPJ_BASICO', how='left')
base_unica_estabelecimento = base_unica_estabelecimento.join(rais_estab_final, 'CD_CNPJ', how='left')

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('TIPO_PORTE',
f.when((f.col('CD_CNAE20_DIVISAO') >= f.lit(5)) & (f.col('CD_CNAE20_DIVISAO') <= f.lit(43)), f.lit('Industria'))
.otherwise(
  f.when((f.col('CD_CNAE20_DIVISAO') < f.lit(5)) | (f.col('CD_CNAE20_DIVISAO') > f.lit(43)), f.lit('Comercio_Servicos')))))

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

duplicados = (base_unica_estabelecimento
        .groupby(['CD_CNPJ'])
        .agg(f.count(f.lit(1)).alias('DUPLICADOS'))
        .orderBy('DUPLICADOS', ascending=False))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento.drop_duplicates(['CD_CNPJ']))

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
                   .withColumn('FL_DADOS_RAIS',
                               f.when((f.col('ID_RAZAO_SOCIAL_RAIS').isNull()), f.lit(0))
                               .otherwise(
                                 f.when((f.col('ID_RAZAO_SOCIAL_RAIS').isNotNull()), f.lit(1))))
                 )

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

var_source_EXP_CNPJ = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['exportadoras'])
EXP_CNPJ = spark.read.parquet(var_source_EXP_CNPJ)

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

var_source_IMP_CNPJ = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['importadoras'])
IMP_CNPJ = spark.read.parquet(var_source_IMP_CNPJ)

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

var_source_class_inten_tec = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['class_inten_tec'])
intensidade_tecnol = spark.read.parquet(var_source_class_inten_tec)

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(intensidade_tecnol , 'CD_CNAE20_DIVISAO', how='left')

# COMMAND ----------

var_source_cnaes_contrib = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['cnaes_contrib'])
contribuintes = spark.read.parquet(var_source_cnaes_contrib)
contribuintes = (contribuintes
                 .withColumnRenamed('CD_CNAE20_SUBCLASSE', 'CD_CNAE20_SUBCLASSE_RFB')
                 .drop('dh_insercao_trs', 'kv_process_control')
                 )

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(contribuintes, 'CD_CNAE20_SUBCLASSE_RFB', how='left')

# COMMAND ----------

base_unica_estabelecimento = (base_unica_estabelecimento
.withColumn('FL_CONTRIBUINTE',
f.when((f.col('FL_CONTRIBUINTE').isNull()) | (f.col('CD_OPCAO_SIMPLES_RFB') == 1), f.lit(0))
.otherwise(
f.when((f.col('FL_CONTRIBUINTE').isNotNull()) & (f.col('CD_OPCAO_SIMPLES_RFB') == 0) , f.lit(1)))))

# COMMAND ----------

var_source_NCM = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['cnae_ncm'])
NCM = spark.read.parquet(var_source_NCM)

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(NCM , "CD_CNAE20_CLASSE", how="left")

# COMMAND ----------

var_source_isic = "{adl_path}{trs}/{file_folder}/".format(adl_path=var_adls_uri, trs=trs, file_folder=table['cnae_isic'])
isic = spark.read.parquet(var_source_isic)

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.join(isic, "CD_CNAE20_CLASSE", how="left")

# COMMAND ----------

base_unica_estabelecimento = base_unica_estabelecimento.drop("dh_insercao_trs")

# COMMAND ----------

df = tcf.add_control_fields(base_unica_estabelecimento, adf, layer="biz")

var_sink_cnpjs_rfb_rais = "{adl_path}{biz}/{path_destination}/".format(adl_path=var_adls_uri, biz=biz, path_destination=table["path_destination"])
df.write.format('parquet').save(var_sink_cnpjs_rfb_rais, header = True, mode='overwrite')
