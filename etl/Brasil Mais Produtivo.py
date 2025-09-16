# Databricks notebook source
# Configurações iniciais
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import (
    col, substring, lpad, when, lit, sum, trim, concat, 
    regexp_replace, round
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from functools import reduce
import re
import pandas as pd


# COMMAND ----------

# MAGIC %md
# MAGIC #rodar

# COMMAND ----------

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


tables = {"schema":"","table":"","trusted_path_1":"/oni/observatorio_nacional/plataforma_produtividade/empresas_engajadas/","business_path_1":"/oni/bases_referencia/municipios/","business_path_2":"/senai/STI/producao/fechamentos_publicados/","business_path_3":"/oni/base_unica_cnpjs/cnpjs_rfb_rais/","business_path_4":"/oni/bases_referencia/cnae/cnae_20/cnae_divisao/","destination":"/oni/brasil_mais_produtivo/","databricks":{"notebook":"/biz/oni/painel_de_inteligencia_de_mercado_B_P/trs_biz_e_biz_biz_empresas_engajadas"},"prm_path":""}


adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


# COMMAND ----------

# MAGIC %md
# MAGIC #rodar

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

# COMMAND ----------

# MAGIC %md
# MAGIC #rodar

# COMMAND ----------

# Configurações iniciais
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import (col, substring, lpad, when, lit, sum, trim, concat, regexp_replace, round, format_number)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType
#from trs_control_field import trs_control_field as tcf
from functools import reduce
import pandas as pd
import re
import os

# COMMAND ----------

#tables = notebook_params.var_tables
#dls = notebook_params.var_dls
#adf = notebook_params.var_adf

# COMMAND ----------

# MAGIC %md
# MAGIC #rodar

# COMMAND ----------

trusted = dls['folders']['trusted']
business = dls['folders']['business']
sink = dls['folders']['business']

# COMMAND ----------

# MAGIC %md
# MAGIC #rodar

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

# COMMAND ----------

# MAGIC %md
# MAGIC #rodar

# COMMAND ----------

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'
print(adl_trusted_1)

business_path_1 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_1'])
adl_business_1 = f'{var_adls_uri}{business_path_1}'
print(adl_business_1)

business_path_2 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_2'])
adl_business_2 = f'{var_adls_uri}{business_path_2}'
print(adl_business_2)

business_path_3 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_3'])
adl_business_3 = f'{var_adls_uri}{business_path_3}'
print(adl_business_3)

business_path_4 = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['business_path_4'])
adl_business_4 = f'{var_adls_uri}{business_path_4}'
print(adl_business_4)

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination'])
adl_destination_path = f'{var_adls_uri}{destination_path}'
print(adl_destination_path)

# COMMAND ----------

'''
brasil_mais_produtivo = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/observatorio_nacional/plataforma_produtividade/empresas_engajadas/')

df_municipios = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/bases_referencia/municipios/')

#abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/senai/STI/producao/fechamentos_publicados/

base_unica = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais/')

cnaes20_divisao = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/bases_referencia/cnae/cnae_20/cnae_divisao/')

#abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/brasil_mais_produtivo/
'''

# COMMAND ----------


brasil_mais_produtivo = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/observatorio_nacional/plataforma_produtividade/empresas_engajadas/')

df_municipios = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/bases_referencia/municipios/')


#abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/senai/STI/producao/fechamentos_publicados/

base_unica = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais/')

cnaes20_divisao = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/bases_referencia/cnae/cnae_20/cnae_divisao/')

#abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/brasil_mais_produtivo/


# COMMAND ----------

## IMPORTANDO BASE DE MUNICIPIOS
#var_path_municipios = '{uri}/biz/oni/bases_referencia/municipios'.format(uri=var_adls_uri)
#df_municipios = spark.read.parquet(var_file_municipios)
# LISTA DE MUNICIPIOS
df_municipios = spark.read.parquet(adl_business_1)

## IMPORTANDO BASE DE FECHAMENTO STI
# BASE SENAI STI 2023
# df_senai_fechamento_sti = spark.read.parquet(adl_business_2)

## IMPORTANDO BASE - EMPRESAS ENGAJADAS B+P
# BASE B+P
## IMPORTANDO BASE - EMPRESAS ENGAJADAS B+P
#var_path_empresas_engajadas = '{uri}/trs/oni/observatorio_nacional/plataforma_produtividade/empresas_engajadas'.format(uri=var_adls_uri)
#base_brasil_mais_produtivo = spark.read.parquet(var_file_empresas_engajadas)
brasil_mais_produtivo = spark.read.parquet(adl_trusted_1)

### IMPORTANDO BASE ÚNICA
#path_base_unica='{uri}/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais'.format(uri=var_adls_uri)
#base_unica = spark.read.parquet(path_base_unica)
base_unica = spark.read.parquet(adl_business_3)

#Tabelas das CNAEs
#cnaes20_divisao = spark.read.parquet(var_adls_uri+'/biz/oni/bases_referencia/cnae/cnae_20/cnae_divisao')
#cnaes20_divisao.printSchema()
cnaes20_divisao = spark.read.parquet(adl_business_4)

# COMMAND ----------

#base_brasil_mais_produtivo.display()

# COMMAND ----------

#base_brasil_mais_produtivo.select('ANO').distinct().show()

# COMMAND ----------

#base_brasil_mais_produtivo.filter(col('ANO').isNull()).count()

# COMMAND ----------

# df_senai_fechamento_sti_2023 = df_senai_fechamento_sti \
#   .withColumn("CNPJ_ESTAB_14", when(col("CNPJ").isNotNull(), lpad(col("CNPJ"), 14, '0')).otherwise("Não localizado")) \
#   .filter(f.col("mes_referencia") == '122023') \
#   .drop("CNPJ") \
#   .withColumnRenamed("CNPJ_ESTAB_14","CNPJ") \
#   .select("CNPJ") \
#   .distinct()

# # CNPJs distintos fechamento 2023 STI
# df_senai_fechamento_sti_2023.count()

# COMMAND ----------

#base_unica \
#  .select('CD_CNPJ') \
#  .count()

# COMMAND ----------

# MAGIC %md
# MAGIC # para baixo

# COMMAND ----------

# IMPORTAR DIMENSÕES CNAE 
#Preparando tabelas das CNAEs
cnaes20_divisao = cnaes20_divisao.select('cd_cnae_divisao', 'nm_cnae_divisao').dropDuplicates(['cd_cnae_divisao'])
cnaes20_divisao = cnaes20_divisao.withColumnRenamed('cd_cnae_divisao', 'CD_CNAE20_DIVISAO').withColumnRenamed('nm_cnae_divisao', 'DS_CNAE20_DIVISAO')
cnaes20_divisao.display()

# COMMAND ----------

# DBTITLE 1,Filtro da coluna CD_CNAE20_DIVISAO de 05 a 43
# Filtro da coluna CD_CNAE20_DIVISAO de 05 a 43
cnaes20_divisao_filtered = cnaes20_divisao.filter((cnaes20_divisao.CD_CNAE20_DIVISAO >= '05') & (cnaes20_divisao.CD_CNAE20_DIVISAO <= '43'))
cnaes20_divisao_filtered.display()

# COMMAND ----------

# CONTAGEM ATIVOS
base_unica_ativos = base_unica \
  .filter(f.col('CD_SIT_CADASTRAL') == 2)

# COMMAND ----------

#base_unica_ativos.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Foi realizado o ajuste, pois a coluna "CD_CNAE20_DIVISAO" teve seu nome alterado para "CD_CNAE20_DIVISAO_RFB".

# COMMAND ----------

# DBTITLE 1,Critério análise - CNAE Principal e Secundário
## FILTRANDO ESTABELECIMENTOS COM CNAE PRINCIPAL ENTRE 05 E 43 (CNAES DIVISÃO INDUSTRIAIS) OU COM SUBCLASSE SECUNDÁRIA CONTENDO CÓDIGOS DE 05 a 43
base_unica_Industria = base_unica.filter((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('05')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',05')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('06')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',06')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('07')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',07')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('08')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',08')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('09')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',09')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('10')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',10')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('11')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',11')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('12')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',12')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('13')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',13')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('14')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',14')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('15')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',15')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('16')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',16')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('17')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',17')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('18')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',18')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('19')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',19')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('20')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',20')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('21')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',21')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('22')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',22')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('23')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',23')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('24')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',24')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('25')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',25')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('26')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',26')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('27')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',27')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('28')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',28')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('29')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',29')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('30')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',30')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('31')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',31')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('32')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',32')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('33')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',33')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('34')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',34')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('35')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',35')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('36')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',36')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('37')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',37')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('38')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',38')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('39')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',39')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('40')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',40')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('41')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',41')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('42')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',42')))
                                    | (f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').startswith('43')) | ((f.col('CD_CNAE20_SUBCLASSE_SECUNDARIA').contains(',43')))
                                    | f.col('CD_CNAE20_DIVISAO_RFB').between('05', '43'))
display(base_unica_Industria)

# COMMAND ----------

# FILTRANDO LINHAS COM CD_SIT_CADASTRAL == ATIVA
base_unica_Industria_ativas = base_unica_Industria \
  .filter(f.col('CD_SIT_CADASTRAL') == '02')

# COMMAND ----------

#base_unica_Industria_ativas.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Foi realizado o ajuste, pois as colunas foram alteradas:
# MAGIC # - "CD_OPCAO_MEI" teve seu nome alterado para "CD_OPCAO_MEI_RFB"
# MAGIC # - "QT_VINC_ATIV" teve seu nome alterado para "NR_QTD_VINCULOS_ATIVOS"

# COMMAND ----------

## FILTRANDO LINHA COM CD_OPCAO_MEI == NÃO MEI
## FILTRANDO MAIS DE 4 FUNCIONÁRIOS
base_unica_Industria_ativas_smei_vinc = base_unica_Industria_ativas \
  .filter((f.col('CD_OPCAO_MEI_RFB') == 0) | (f.col('CD_OPCAO_MEI_RFB').isNull())) \
  .filter(f.col('NR_QTD_VINCULOS_ATIVOS') > 4)

# COMMAND ----------

base_unica_Industria_ativas_smei_vinc.count()

# COMMAND ----------

# DBTITLE 1,B+P - Base Data Lake
brasil_mais_produtivo_consumo = base_brasil_mais_produtivo \
  .filter((f.col("MANUFATURA_ENXUTA") == "Sim") | (f.col("EFICIENCIA_ENERGETICA") == "Sim") | (f.col("TRANSFORMACAO_DIGITAL") == "Sim")) \
  .filter(f.col("ANO") >= "2024")

# COMMAND ----------

base_brasil_mais_produtivo.groupBy('ANO').count().show()
brasil_mais_produtivo_consumo.groupBy('ANO').count().show()

# COMMAND ----------

# DBTITLE 1,Remoção Empresas Engajadas (Comentado)
# # REMOVENDO EMPRESAS QUE JÁ ESTÃO ENGAJADAS NO PROGRAMA DAS BASES 

# base_unica_Industria_ativas_smei_vinc_sengajadas = base_unica_Industria_ativas_smei_vinc \
#   .join(brasil_mais_produtivo_consumo2024, f.col('CD_CNPJ') == f.col('CNPJ_TEXTO'), how='anti')

# COMMAND ----------

# Sinalizando empresas que consumiram o B+P

base_unica_Industria_ativas_smei_vinc_sengajadas = base_unica_Industria_ativas_smei_vinc \
  .join(brasil_mais_produtivo_consumo, f.col('CD_CNPJ') == f.col('CNPJ'), "left") \
  .withColumn("Já é cliente?", f.when(f.col("CNPJ").isNotNull(), "Sim").otherwise("Não"))

# COMMAND ----------

df_municipios = df_municipios \
  .drop("SG_UF")

# COMMAND ----------

# enriquecer com municipios

base_unica_Industria_ativas_smei_vinc_sengajadas = base_unica_Industria_ativas_smei_vinc_sengajadas \
  .join(df_municipios, df_municipios["cd_ibge_municipio"] == base_unica_Industria_ativas_smei_vinc_sengajadas["CD_MUNICIPIO_RFB"],"left")

# COMMAND ----------

base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado = base_unica_Industria_ativas_smei_vinc_sengajadas \
  .select("SG_UF", "DS_MUNICIPIO", "NM_MESORREGIAO", "DS_CAPITAL", "NM_MICRORREGIAO", "CD_CNPJ", "CD_CNPJ_BASICO", "NM_RAZAO_SOCIAL_RECEITA_EMPRESA", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "CD_CNAE20_SUBCLASSE_RFB", "CD_CNAE20_SUBCLASSE_RAIS",  "NM_TIPO_LOGRADOURO", "NM_LOGRADOURO", "NM_NUMERO_ESTBL", "NM_COMPLEMENTO", "NM_BAIRRO", "CD_CEP", "NM_DDD_1", "NM_TELEFONE_1", "NM_DDD_2", "NM_TELEFONE_2",  "CD_NATUREZA_JURIDICA_RFB","DS_NAT_JURIDICA_RFB", "CD_CNAE20_DIVISAO", "QT_VINC_ATIV", "NM_EMAIL","DS_MATRIZ_FILIAL", "Já é cliente?") \
    .withColumn(
        "Porte_final",
        f.when(
            f.col("DS_PORTE_EMPRESA").isNotNull(),
            f.when(f.col("DS_PORTE_EMPRESA") == "Micro Empresa", "Micro")
             .when(f.col("DS_PORTE_EMPRESA") == "Empresa de Pequeno Porte", "Pequena")
        ).otherwise(f.col("DS_PORTE_EMPRESA"))
    ) 

# COMMAND ----------

# DBTITLE 1,>>>>> CONSTRUCAO DADOS B+P PARA CONSUMO <<<<<<
base_brasil_mais_produtivo_filtrado_enriquecido = base_brasil_mais_produtivo \
.join(base_unica, base_brasil_mais_produtivo["CNPJ"] == base_unica["CD_CNPJ"],"left") \
.select(
'SG_UF', 'CNPJ', 'CD_CNPJ_BASICO', 'NM_RAZAO_SOCIAL_RECEITA_EMPRESA', 'DS_PORTE_EMPRESA', 'PORTE_SEBRAE', 'CD_CNAE20_SUBCLASSE_RFB', 'CD_CNAE20_SUBCLASSE_RAIS', 'NM_TIPO_LOGRADOURO', 'NM_LOGRADOURO', 'NM_BAIRRO', 'CD_NATUREZA_JURIDICA_RFB','DS_NAT_JURIDICA_RFB', 'CD_CNAE20_DIVISAO', 'DATA', 'UF', 'ALI', 'MANUFATURA_ENXUTA', 'EFICIENCIA_ENERGETICA','TRANSFORMACAO_DIGITAL') \
.withColumn(
        "Porte_final",
        f.when(
            f.col("DS_PORTE_EMPRESA").isNotNull(),
            f.when(f.col("DS_PORTE_EMPRESA") == "Micro Empresa", "Micro")
             .when(f.col("DS_PORTE_EMPRESA") == "Empresa de Pequeno Porte", "Pequena")
             .otherwise(f.col("DS_PORTE_EMPRESA")))
        )

# COMMAND ----------

# DBTITLE 1,construcao base lm, ee e td
base_brasil_mais_produtivo_filtrado_enriquecido_lm = base_brasil_mais_produtivo_filtrado_enriquecido \
  .filter(f.col("MANUFATURA_ENXUTA") == "Sim") \

base_brasil_mais_produtivo_filtrado_enriquecido_ee = base_brasil_mais_produtivo_filtrado_enriquecido \
  .filter(f.col("EFICIENCIA_ENERGETICA") == "Sim") \

base_brasil_mais_produtivo_filtrado_enriquecido_td = base_brasil_mais_produtivo_filtrado_enriquecido \
  .filter(f.col("TRANSFORMACAO_DIGITAL") == "Sim") \

# base_brasil_mais_produtivo_filtrado_enriquecido_ee = base_brasil_mais_produtivo_filtrado_enriquecido \
#   .filter(f.col("PRODUTO NACIONAL") == "NOVO B+P EFICIÊNCIA ENERGÉTICA")

# COMMAND ----------

# DBTITLE 1,>>>>> CONSTRUCAO BASES LEAN<<<<
# Base Lean Manufacturing - Nacionalizada (considerando somente o Porte)
base_brasil_mais_produtivo_filtrado_enriquecido_lm_porte_nacionalizadaliza = base_brasil_mais_produtivo_filtrado_enriquecido_lm \
  .groupBy('Porte_final') \
  .count() \
  .withColumnRenamed('count', 'Porte_Contagem_LM') \
  .orderBy('Porte_final')

# Base Lean Manufacturing - Nacionalizada (considerando somente a Natureza Juridica)
base_brasil_mais_produtivo_filtrado_enriquecido_lm_natjur_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_lm \
  .groupBy('CD_NATUREZA_JURIDICA_RFB') \
  .count() \
  .withColumnRenamed('count', 'NatJur_Contagem_LM') \
  .orderBy('CD_NATUREZA_JURIDICA_RFB')

# Base Lean Manufacturing - Nacionalizada (considerando somente o CNAE)
base_brasil_mais_produtivo_filtrado_enriquecido_lm_cnae_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_lm \
  .groupBy('CD_CNAE20_DIVISAO') \
  .count() \
  .withColumnRenamed('count', 'CNAE_Contagem_LM') \
  .orderBy('CD_CNAE20_DIVISAO')

# COMMAND ----------

# DBTITLE 1,DADOS PORTE - LEAN NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_lm_porte_nacionalizadaliza_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_lm_porte_nacionalizadaliza \
  .withColumn('Total', f.sum('Porte_Contagem_LM').over(windowSpec)) \
  .withColumn('Percentual', f.col('Porte_Contagem_LM') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('Porte_final', 'Porte_LM') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_Porte_Lean') \
  .select('Porte_LM','Percentual_Porte_Lean')

# COMMAND ----------

# DBTITLE 1,DADOS NATUREZA JURIDICA - LEAN NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_lm_natjur_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_lm_natjur_nacionalizada \
  .withColumn('Total', f.sum('NatJur_Contagem_LM').over(windowSpec)) \
  .withColumn('Percentual', f.col('NatJur_Contagem_LM') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('CD_NATUREZA_JURIDICA_RFB', 'NatJur_LM') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_NatJur_Lean') \
  .select('NatJur_LM','Percentual_NatJur_Lean')

# COMMAND ----------

# DBTITLE 1,DADOS CNAE - LEAN NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_lm_cnae_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_lm_cnae_nacionalizada \
  .withColumn('Total', f.sum('CNAE_Contagem_LM').over(windowSpec)) \
  .withColumn('Percentual', f.col('CNAE_Contagem_LM') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('CD_CNAE20_DIVISAO', 'CNAE_LM') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_CNAE_Lean') \
  .select('CNAE_LM','Percentual_CNAE_Lean')

# COMMAND ----------

# DBTITLE 1,>>>>> CONSTRUCAO BASES EFICIENCIA<<<<
# Base Eficiência Energética - Nacionalizada (considerando somente o Porte)
base_brasil_mais_produtivo_filtrado_enriquecido_ee_porte_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_ee \
  .groupBy('Porte_final') \
  .count() \
  .withColumnRenamed('count', 'Porte_Contagem_EE') \
  .orderBy('Porte_final')

# Base Eficiência Energética - Nacionalizada (considerando somente a Natureza Jurídica)
base_brasil_mais_produtivo_filtrado_enriquecido_ee_natjur_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_ee \
  .groupBy('CD_NATUREZA_JURIDICA_RFB') \
  .count() \
  .withColumnRenamed('count', 'NatJur_Contagem_EE') \
  .orderBy('CD_NATUREZA_JURIDICA_RFB')

# Base Eficiência Energética - Nacionalizada (considerando somente o CNAE)
base_brasil_mais_produtivo_filtrado_enriquecido_ee_cnae_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_ee \
  .groupBy('CD_CNAE20_DIVISAO') \
  .count() \
  .withColumnRenamed('count', 'CNAE_Contagem_EE') \
  .orderBy('CD_CNAE20_DIVISAO')

# COMMAND ----------

# DBTITLE 1,DADOS PORTE - EFICIENCIA NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_ee_porte_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_ee_porte_nacionalizada \
  .withColumn('Total', f.sum('Porte_Contagem_EE').over(windowSpec)) \
  .withColumn('Percentual', f.col('Porte_Contagem_EE') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('Porte_final', 'Porte_EE') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_Porte_Eficiencia') \
  .select('Porte_EE','Percentual_Porte_Eficiencia')

# COMMAND ----------

# DBTITLE 1,DADOS PORTE - EFICIENCIA NATUREZA JURIDICA NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_ee_natjur_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_ee_natjur_nacionalizada \
  .withColumn('Total', f.sum('NatJur_Contagem_EE').over(windowSpec)) \
  .withColumn('Percentual', f.col('NatJur_Contagem_EE') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('CD_NATUREZA_JURIDICA_RFB', 'NatJur_EE') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_NatJur_Eficiencia') \
  .select('NatJur_EE','Percentual_NatJur_Eficiencia')

# COMMAND ----------

# DBTITLE 1,DADOS PORTE - EFICIENCIA CNAE NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_ee_cnae_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_ee_cnae_nacionalizada \
  .withColumn('Total', f.sum('CNAE_Contagem_EE').over(windowSpec)) \
  .withColumn('Percentual', f.col('CNAE_Contagem_EE') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('CD_CNAE20_DIVISAO', 'CNAE_EE') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_CNAE_Eficiencia') \
  .select('CNAE_EE','Percentual_CNAE_Eficiencia')

# COMMAND ----------

# DBTITLE 1,>>>>> CONSTRUCAO BASES TRANSFORMACAO <<<<
# Base Lean Manufacturing - Nacionalizada (considerando somente o Porte)
base_brasil_mais_produtivo_filtrado_enriquecido_td_porte_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_td \
  .groupBy('Porte_final') \
  .count() \
  .withColumnRenamed('count', 'Porte_Contagem_TD') \
  .orderBy('Porte_final')

# Base Lean Manufacturing - Nacionalizada (considerando somente a Natureza Juridica)
base_brasil_mais_produtivo_filtrado_enriquecido_td_natjur_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_td \
  .groupBy('CD_NATUREZA_JURIDICA_RFB') \
  .count() \
  .withColumnRenamed('count', 'NatJur_Contagem_TD') \
  .orderBy('CD_NATUREZA_JURIDICA_RFB')

# Base Lean Manufacturing - Nacionalizada (considerando somente o CNAE)
base_brasil_mais_produtivo_filtrado_enriquecido_td_cnae_nacionalizada = base_brasil_mais_produtivo_filtrado_enriquecido_td \
  .groupBy('CD_CNAE20_DIVISAO') \
  .count() \
  .withColumnRenamed('count', 'CNAE_Contagem_TD') \
  .orderBy('CD_CNAE20_DIVISAO')

# COMMAND ----------

# DBTITLE 1,DADOS PORTE - TRANSFORMACAO NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_td_porte_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_td_porte_nacionalizada \
  .withColumn('Total', f.sum('Porte_Contagem_TD').over(windowSpec)) \
  .withColumn('Percentual', f.col('Porte_Contagem_TD') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('Porte_final', 'Porte_TD') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_Porte_Transformacao') \
  .select('Porte_TD','Percentual_Porte_Transformacao')

# COMMAND ----------

# DBTITLE 1,DADOS PORTE - TRANSFORMACAO NATUREZA JURIDICA NACIONALIZADO
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_td_natjur_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_td_natjur_nacionalizada \
  .withColumn('Total', f.sum('NatJur_Contagem_TD').over(windowSpec)) \
  .withColumn('Percentual', f.col('NatJur_Contagem_TD') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('CD_NATUREZA_JURIDICA_RFB', 'NatJur_TD') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_NatJur_Transformacao') \
  .select('NatJur_TD','Percentual_NatJur_Transformacao')

# COMMAND ----------

# DBTITLE 1,DADOS PORTE - TRANSFORMACAO CNAE NACIONALIZADO PENDENTE
windowSpec = Window.partitionBy()

base_brasil_mais_produtivo_filtrado_enriquecido_td_cnae_nacionalizada_percentual = base_brasil_mais_produtivo_filtrado_enriquecido_td_cnae_nacionalizada \
  .withColumn('Total', f.sum('CNAE_Contagem_TD').over(windowSpec)) \
  .withColumn('Percentual', f.col('CNAE_Contagem_TD') / f.col('Total')) \
  .withColumn('Percentual_Ajustado', f.round(f.col("Percentual"), 4)) \
  .withColumnRenamed('CD_CNAE20_DIVISAO', 'CNAE_TD') \
  .withColumnRenamed('Percentual_Ajustado', 'Percentual_CNAE_Transformacao') \
  .select('CNAE_TD','Percentual_CNAE_Transformacao')

# COMMAND ----------

# DBTITLE 1,Base completa B+P
base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_lm_porte_nacionalizadaliza_percentual, f.col("Porte_final") == f.col("Porte_LM"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_lm_natjur_nacionalizada_percentual, f.col("CD_NATUREZA_JURIDICA_RFB") == f.col("NatJur_LM"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_lm_cnae_nacionalizada_percentual, f.col("CD_CNAE20_DIVISAO") == f.col("CNAE_LM"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_ee_porte_nacionalizada_percentual, f.col("Porte_final") == f.col("Porte_EE"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_ee_natjur_nacionalizada_percentual, f.col("CD_NATUREZA_JURIDICA_RFB") == f.col("NatJur_EE"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_ee_cnae_nacionalizada_percentual, f.col("CD_CNAE20_DIVISAO") == f.col("CNAE_EE"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_td_porte_nacionalizada_percentual, f.col("Porte_final") == f.col("Porte_TD"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_td_natjur_nacionalizada_percentual, f.col("CD_NATUREZA_JURIDICA_RFB") == f.col("NatJur_TD"), "left") \
  .join(base_brasil_mais_produtivo_filtrado_enriquecido_td_cnae_nacionalizada_percentual, f.col("CD_CNAE20_DIVISAO") == f.col("CNAE_TD"), "left")

# COMMAND ----------

# DBTITLE 1,Lista Transformação Digital - Base Produção (Comentado)
# # Lista de transformação digital considerando o campo "DESCRICAO_ATENDIMENTO" da base STI-SENAI
# lista_transformacao_digital = [
# 'WORKSHOP DE TRANSFORMAÇÃO DIGITAL'
# ,'AGENDATECH   TRANSFORMAÇÃO DIGITAL NA CONSTRUÇÃO CIVIL INVESTIGANDO A ADOÇÃO DE TECNOLOGIAS E OS IMPACTOS NO DESEMPENHO E SUSTENTABILIDADE DA INDÚSTRIA PERNAMBUCANA'
# ,'SEBRAETEC CONTRATO N 009242021  INSERÇÃO DIGITAL  DESENVOLVIMENTO DE WEBSITE  IND ALIMENTICIA CAMPO VERDE LTDA'
# ,'AI  CLAUDOVAL BARBOSA DE MELO PANIFICAÇÃO  ME  MODELAGEM DIGITAL 3D  1227749'
# ,'AI   AKY ESTOFADOS CUSTOMIZADOS LTDA  MODELAGEM DIGITAL 3D  1227753'
# ,'AI   PEDROSA   FONTAN PANIFICACAO LTDA  MODELAGEM DIGITAL 3D  1227163'
# ,'AI   BC PLAST INDUSTRIA E COMERCIO LTDA  MODELAGEM DIGITAL 3D   1227285'
# ,'AI   BW PLAST INDUSTRIA E COMERCIO LTDA  MODELAGEM DIGITAL 3D  1223478'
# ,'ABDI  AGRICOM  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1331140'
# ,'ABDI  IPLAC INDUSTRIA PLASTICA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1331574'
# ,'IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA'
# ,'ABDI  FRIGOBIFE  MPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1338397'
# ,'ABDI  COOPERBONI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1338944'
# ,'ABDI  D H S DE ALBUQUERQUE PONTES  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA   1339035'
# ,'ABDI  LATICINIO SAO PEDRO LTDA  ME  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1339103'
# ,'ABDI  J J LIRA PANIFICADORA LTDA  ME   IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1339847'
# ,'AI  PADRAO UNIFORMES  MODELAGEM DIGITAL 3D  1341882'
# ,'AI  CURAMEL  MODELAGEM DIGITAL 3D  1338792'
# ,'ABDI  NORDESTE MAIS ALIMENTOS LTDA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1340694'
# ,'ABDI  PANIFICACAO JARAGUA LTDA  ME  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1342056'
# ,'ABDI  JULIETTE A PADARIA EIRELI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1342865'
# ,'ABDI  JULIETTE A PADARIA EIRELI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1342873'
# ,'ABDI  ALBATROZ CONFECÇÕES  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1342067'
# ,'ABDI  I N O V A P L A S T  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1342483'
# ,'ABDI  FORNIDELI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1342075'
# ,'ABDI   MACEIO FARDAS  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1344015'
# ,'ABDI  COACH CAMISARIA E UNIFORMES  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1343545'
# ,'ABDI  A CASA PORTUGUESA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1344018'
# ,'ABDI  AGLAY M C VIANA PANIFICAÇÃO  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1342817'
# ,'ABDI  PANIFICAÇÃO ALVORADA II  IMPLANTAÇÃO DE PLATAFORA DIGITAL PRODUTIVA  1344178'
# ,'ABDI  OFFICIAL SPORTS  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1343380'
# ,'ABDI  PARISOTTO BOLSAS  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1332730'
# ,'ABDI  VIP SPORTS  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1337532'
# ,'ABDI  NÓBREGA CONFECÇÕES  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1337573'
# ,'ABDI  PADARIA DE TÉI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1334427'
# ,'ABDI  PANIFICADORA SÃO LUIZ  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1334612'
# ,'ABDI  BISCOITO CASEIRO D LICIA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1337154'
# ,'ABDI  BEM BOM  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1338091'
# ,'ABDI  ILLA SORVETES  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1336690'
# ,'ABDI  AGUA MINERAL ALDEBARAN  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1336715'
# ,'CONVENIO AI   BEEVA   MODELAGEM DIGITAL 3D  1363032'
# ,'CONVENIO AI 2022  MULTIDOOR  MODELAGEM DIGITAL 3D  1355050'
# ,'CONVENIO AI   INDUSTRIA PARISOTTO LTDA   MODELAGEM DIGITAL 3D  1355165'
# ,'CONVENIO AI 2022  STUDIO RYTCHYSKI  MODELAGEM DIGITAL 3D  1359001'
# ,'MODELAGEM DIGITAL 3D'
# ,'ABDI  PHILAR FABRICAÇÃO DE LATICÍNIOS EIRELI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1344466'
# ,'ABDI  CAF  CRYSTAL AGUAS DO NORDESTE LTDA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1341994'
# ,'ABDI  COPRA INDUSTRIA ALIMENTICIA LTDA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1348526'
# ,'ABDI  LATICINIO FLOR DO PARAIBA LTDA  EPP  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1349969'
# ,'CONVENIO AI 2022  GOLD CAFÉS LTDA  ME  MODELAGEM DIGITAL 3D  1371043'
# ,'ABDI  T Z CONFECCOES EIRELI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1346175'
# ,'ABDI  PANIFICAÇÃO VIRGINIO DE CAMPOS  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1346105'
# ,'ABDI   HOSTIAS PAO DA VIDA LTDA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1345907'
# ,'ABDI  MARTA ARAUJO SAMPAIO PADILHA   IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA   1346338'
# ,'ABDI  SORMEL INDÚSTRIA DE SORVETES LTDA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1346406'
# ,'ABDI  ULTRAPLAST  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1331388'
# ,'ABDI   REPLAST  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1331420'
# ,'ABDI  PONTO MÁXIMO INDÚSTRIA E COMÉRCIO LTDA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1348328'
# ,'ABDI  SAMIRA MALTA DE ALMEIDA CABRAL  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1350826'
# ,'ABDI   JOSE CARLOS DOS SANTOS PADARIA   IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1356447'
# ,'ABDI  ELSE MARIA BARBOSA DE OLIVEIRA EIRELI  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1341281'
# ,'ABDI  PANIFICACAO ALVORADA III  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1353829'
# ,'ABDI  INDUSTRIA DE LATICINIOS E SUCOS BOM DIA LTDA  IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA  1397503'
# ,'IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA 1337384'
# ,'1402064 IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA'
# ,'1409035 IMPLANTAÇÃO DE PLATAFORMA DIGITAL PRODUTIVA'
# ,'538  Sanofi Medley  Digitalização de Processo IOT'
# ,'538ISIAUTOPROD  Sanofi Medley  Digitalização de Processo IOT'
# ,'CRM 49272023  RAIMUNDA ROSEMARY NOGUEIRA CONRADO  METALURGICA O MAZINHO  SEBRAETEC  INSERCAO DIGITAL  DESENVOLVIMENTO DE WEBSITE'
# ,'CRM  51472023  FRANCISCO DAS C S MOURA  MOURA METALURGICA E ACESSORIOS  INSERCAO DIGITAL  SEBRAETEC  DESENVOLVIMENTO DE WEBSITE'
# ,'CRM  54082023  FRANCINIR LIMA DE FARIAS  ALUMINIO IMPERIAL  SEBRAETEC  INSERCAO DIGITAL  DESENVOLVIMENTO DE WEBSITE'
# ,'CRM  58242023  AGROINDUTRIAL GF PARACURUR LTDA  AGROINDUSTRIAL GF PARACURU  SEBRAETEC  INSERÇÃO DIGITAL  DESENVOLVIMENTO DE WEBSITE'
# ,'CRM  64012023  REALIZE SOFTW LTDA  REALIZE SOFTW  SEBRAETEC  INSERÇÃO DIGITAL  DESENVOLVIMENTO DE WEBSITE'
# ,'CRM  64042023  TECHNOFLOW SOFTW LTDA  SEBRAETEC  INSERÇÃO DIGITAL  DESENVOLVIMENTO DE WEBSITE'
# ,'PROJETO INOVA MODA DIGITAL 2023'
# ,'DIGITALIZAÇÃO CAMISA SOCIAL'
# ,'DIGITALIZAÇÃO RO'
# ,'MENTORIA DIGITAL  PILOTO 62 HORAS'
# ,'PROJETO GÊMEO DIGITAL MSD X REAL CAFE'
# ,'PROJETO DIGITAL TWIN DINÂMICO MINERVA X SAMARCO'
# ,'PROJETO DIGITAL TWIN DINÂMICO  MINERVA X SAMARCO'
# ,'CONEXÕES 1 PROJETO DIGITAL TWIN DO CADINHO DO ALTO FORNO  OPTIMATECH  ARCELOR 332393'
# ,'BRASIL MAIS  MENTORIA DIGITAL'
# ,'BRASIL MAIS  MENTORIA DIGITAL  2111'
# ,'BRASIL MAIS  MENTORIA DIGITAL 2111'
# ,'DIGITALIZAÇÃO DOS DOCUMENTOS DE LICENCIAMENTO AMBIENTAL   2111'
# ,'ARAÇAS  DIGITALIZAÇAO E GRADUAÇÃO DE MOLDES  LOC FOTOS ETC'
# ,'ARAÇAS  DIGITALIZAÇAO DE MOLDES (22121612212165)'
# ,'ARAÇAS  DIGITALIZAÇÃO DE MOLDES'
# ,'ARAÇAS  DIGITALIZAÇÃO DE MOLDES (VESTIDOS COMPLEXOS)'
# ,'ARAÇAS  DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES (REPLICAR GRAD DE CAMISAS)'
# ,'ARAÇAS DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES'
# ,'ARAÇAS DIGITALIZAÇÃO DE MOLDES'
# ,'ARAÇAS DIGITALIZAÇAO E GRADUAÇÃO DE MOLDES (2212166)'
# ,'ARAÇAS  DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES  CJ'
# ,'ARAÇAS  DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES (2910  100628)'
# ,'ARAÇAS  DIGITALIZAÇÃO DE MODELAGENS'
# ,'ARAÇAS  DIGITALIZAÇÃO E GRADUAÇÃO DE MODELAGENS'
# ,'ARAÇAS  DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES (KIMONO)'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MODELAGENS'
# ,'DIGITALIZAÇÃO MOLDES ENCAIXE E PLOTAGEM RISCO'
# ,'DIGITALIZAÇÃO ENCAIXES E PLOTAGEM DE RISCO'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES REALIZAÇÃO DE ENCAIXES PARA CORTE PLOTAGEM'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES  PLOTAGEM DE RISCO PARA CORTE  CORTE DE PRODUTOS TEXTEIS'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES  PLOTAGEM DE RISCO PARA CORTE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES  REALIZAÇAO DE ENCAIXES  PLOTAGEM DE RISCO PARA CORTE'
# ,'DIGITALIZAÇAO DE MOLDES REALIZAÇÃO DE ENCAIXE PLOTAGEM DE RISCO PARA CORTE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES'
# ,'DIGITALIZAÇÃO MOLDES'
# ,'DIGITALIZAÇÃO MOLDES ENCAIXES PARA CORTE PLOTAGEM DE RISCO PARA CORTE'
# ,'AJUSTE EM MODELAGEM E DIGITALIZAÇÃO MOLDES'
# ,'DIGITALIZAÇÃO GRADUAÇÃO ENCAIXE PARA CORTE CONVERSÃO ARQUIVOS'
# ,'DIGITALIZAÇAO E GRADUAÇÃO DE MOLDES  PLOTAGEM DE RISCO PARA CORTE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES REALIZAÇAO DE ENCAIXES  PLOTAGEM DE RISCO PARA CORTE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES IMPRESSÃO'
# ,'DIGITALIZAÇAÕ E GRADUAÇÃO DE MOLDES REALIZAÇÃO DE ENCAIXE PARA CORTE CORTE DE PRODUTOS TEXTEIS'
# ,'DIGITALIZAÇÃO DE MOLDES PLOTAGEM'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES REALIZAÇÃO DE ENCAIXES PARA CORTE PLOTAGEM DE RISCOS PARA CORTE'
# ,'DIGITALIZAÇÃO DE BASES DE MOLDES (NÃO GRADUADO) REALIZAÇÃO DE ENCAIXES PLOTAGEM DE MOLDES PARA TESTES DE GRADUAÇÃO'
# ,'DIGITALIZAÇÃO DE MOLDES DE JALECO REALIZAÇÃO DE ENCAIXE DE MOLDES PARA CORTE PLOTAGEM DE RISCOS PARA CORTE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MODELAGEM REALIZAÇÃO DE ENCAIXE PARA CORTE  PLOTAGEM DE RISCO PARA CORTE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES  REALIZAÇÃO DE ENCAIXE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES AJUSTES EM MODELAGENS PLOTAGEM DE RISCOS PARA CORTE'
# ,'DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES CRIAÇÃO DE MOLDES DIGITAIS (RETANGULARES)'
# ,'AI  AGROIND  PIX FORCE PARA DIGITALIZAÇÃO DE DOCUMENTOS'
# ,'AI  AGROIND  SENSORES INTELIGENTES SEM FIO PARA DIGITALIZAR MÁQUINAS E PROCESSOS INDUSTRIAIS  HEDRO SISTEMAS INTELIGENTES'
# ,'PEI 4 (2022)  GÉMEO DIGITAL DE CONTROLE POR SIMULAÇÃO EM TEMPO REAL  INFINITE FOUNDRY  SAMARCO'
# ,'SENAI DF  REPASSE MENTORIA DIGITAL'
# ,'SENAI DF  REPASSE MENTORIA DIGITAL  2117'
# ,'GAS 2021387 ORÇAMENTO 127363903  DIGITALIZAÇÃO'
# ,'DIGITALIZAÇÃO DE PROCESSOS'
# ,'CONSULTORIA EM DIGITALIZAÇÃO'
# ,'SEBRAETEC  INSERÇÃO DIGITAL  DESENVOLVIMENTO DE WEBSITE MAIS | CASA DO TAPECEIRO POUSO ALEGRE'
# ,'SEBRAETEC  INSERÇÃO DIGITAL  DESENVOLVIMENTO DE WEBSITE'
# ,'PROJETOS ESPECIAIS 40  CONSULTORIA EM DIGITALIZAÇÃO'
# ,'PROJETOS ESPECIAIS 40  EXT DIGITALIZAÇÃO'
# ,'FIEMG COMPETITIVA  DIGITALIZAÇÃO'
# ,'MODELAGEM DIGITALIZAÇÃO E GRADUAÇÃO DE MOLDES'
# ,'MENTORIA DIGITAL'
# ,'INSERÇÃO DIGITAL  DESENVOLVIMENTO DE WEBSITE  TECH AGRO'
# ,'DIGITALIZAÇÃO E MODERNIZAÇÃO DO SISTEMA DE RESFRIAMENTO DAS UNIDADES GERADORAS DA UHE TUCURUÍ  SISTEMA DE RESFRIAMENTO INTELIGENTE  SIRI'
# ,'GRADAÇÃO E DIGITALIZAÇÃO DE MODELAGEM  AMANDA SOUTO MOTA 04759496416'
# ,'SERVIÇOS DE GRADAÇÃO E DIGITALIZAÇÃO DE MODELAGEM  GIULIA LIMA'
# ,'DIGITALIZAÇÃO DE PROCESSO ENGENHARIA DE APLICAÇÃO'
# ,'CONSULTORIA EM DIGITALIZAÇÃO PE 2023  ACUMULADORES MOURA S A UN10'
# ,'CONSULTORIA EM DIGITALIZAÇÃO PE 2023  EUROCONTAINERS'
# ,'CONSULTORIA EM DIGITALIZAÇÃO PE 2023  ACUMULADORES MOURA S A UN11'
# ,'CONSULTORIA EM DIGITALIZAÇÃO PE 2023  ACUMULADORES MOURA S A UN12'
# ,'CONSULTORIA EM DIGITALIZAÇÃO PE 2023  ACUMULADORES MOURA S A UN5'
# ,'CONSULTORIA EM DIGITALIZAÇÃO PE 2023  ACUMULADORES MOURA S A UN1'
# ,'Manufatura Digital - Curitiba/PR - 2017 (NÃO TEM CONTRATO)'
# ,'VTec Paraná - manufatura digital (2 máquinas) (SEBRAETEC 23673)'
# ,'SEBRAE - VT INDUSTRIA E COMERCIO DE PLASTICOS EIRELI - ME - Manufatura Digital - Voucher Tecnológico'
# ,'SEBRAE - OBJETIVA INDUSTRIA E COMERCIO DE EMBALAGENS PLASTICAS LTDA. - ME - Manufatura Digital ? Voucher Tecnológico - CTR_0075543_2017'
# ,'SEBRAE - R V D TREINAMENTO E COACHING GERENCIAL LTDA - Manufatura Digital ? Voucher Tecnológico  - CTR_0075542_2017'
# ,'SEBRAE - CARTO INGA INDUSTRIA E COMERCIO DE CAIXAS DE PAPELAO - Manufatura Digital ? Voucher Tecnológico  - CTR_0075526_2017'
# ,'VTec Paraná - manufatura digital (1 máquina) (SEBRAETEC 23442)'
# ,'SEBRAE - ALLFLEX INDUSTRIA E COMERCIO DE ETIQUETAS LTDA - EPP - Manufatura Digital ? Voucher Tecnológico  - CTR_0075534_2017'
# ,'INDUSFRIO INDUSTRIA DE REFRIGERACAO LTDA - EPP - Consultoria em Tecnologia VTec Paraná - manufatura digital (1 máquina) - VTEC 23442'
# ",'VTec manufatura digital (2 máquinas)	-CTR_0013559_2018 -Pacto23445 - SIG'"
# ,'Vtec manufatura digital (1 máquina) -ARTLONDRE  - CTR_0009859_2018- Pacto 23765'
# ,'VTec manufatura digital (1 máquina) - CTR_0013556_2018 - Pacto 23770 - CRISAG'
# ,'VTec manufatura digital (2 máquinas) - CTR_0010103_2018-23673 - Multimetal'
# ,'Vtec manufatura digital (2 máquinas) - CTR_0011008_2018-Pacto -23572 FABRIPORTAS'
# ,'Manufatura Digital  CuritibaPR'
# ,'SESITECH 2022  DTE SAFEWELL  DIGITAL TWIN ESTRUTURAL PARA SEGURANCA DE POCOS DE PETROLEO'
# ,' SESITECH 2022  DTE SAFEWELL  DIGITAL TWIN ESTRUTURAL PARA SEGURANCA DE POCOS DE PETROLEO'
# ,'SEBRAE  DIGITALIZAÇÃO DE PROCESSOS IND 40'
# ,'DIGITALIZAÇÃO DE PRODUTOS  SEBRAE'
# ,'OS 10202  DIGITALIZAÇÃO'
# ,'(7283) Manufatura Digital ? Simulação Computacional'
# ,'(9401) Manufatura Digital ? Simulação Computacional'
# ,'(9403) Manufatura Digital ? Simulação Computacional'
# ,'(9404) Manufatura Digital ? Simulação Computacional'
# ,'(9405) Manufatura Digital ? Simulação Computacional'
# ,'(9408) Manufatura Digital ? Simulação Computacional'
# ,'(9411) Manufatura Digital ? Simulação Computacional'
# ,'(9495) Manufatura Digital ? Simulação Computacional'
# ,'(9492) Manufatura Digital ? Simulação Computacional'
# ,'(9789) Manufatura Digital ? Simulação Computacional'
# ,'(9790) Manufatura Digital ? Simulação Computacional'
# ,'(10377) Manufatura Digital ? Simulação Computacional'
# ,'(10381) Manufatura Digital ? Simulação Computacional'
# ,'(10382) Manufatura Digital ? Simulação Computacional'
# ,'(10547) Manufatura Digital ? Simulação Computacional'
# ,'(10608) Manufatura Digital ? Simulação Computacional'
# ,'(11252) Manufatura Digital ? Simulação Computacional'
# ,'(11477) Manufatura Digital ? Simulação Computacional'
# ,'(11486) Manufatura Digital ? Simula??o Computacional'
# ,'Manufatura Digital'
# ,'Logística Digital'
# ,'Manufatura Digital  Simulação Computacional'
# ,'MPCI  Digitalização de detector de metais'
# ,'Manufatura Digital  Fase 2'
# ,'Assessoria em Digitalização'
# ,'Digitalização 3D do Design de Peça'
# ,'Implantação de tecnologias habilitadoras - Digitalização'
# ,'Inserção digital - Desenvolvimento de WebSite'
# ,'Assessoria técnica em impressão digital'
# ,'Assessoria em Digitalização e Conectividade'
# ,'Digitalização de Moldes'
# ]

# COMMAND ----------

# DBTITLE 1,Fechamento Producao - Filtrando Transformacao Digital (Comentado)
# df_senai_fechamento_sti_2023_tranf_digital = df_senai_fechamento_sti \
#   .filter(f.col("mes_referencia") == '122023') \
#   .filter(f.col("DESCRICAO_ATENDIMENTO").isin(lista_transformacao_digital)) \
#   .select("DR","CNPJ","DESCRICAO_ATENDIMENTO") \
#   .withColumnRenamed("CNPJ","CNPJ_Transf_Digital") \
#   .distinct() \
#   .orderBy(col("DR").asc())

# COMMAND ----------

# DBTITLE 1,Realização de atribuição de valor para nulos
# Verifica se os mínimos são None e atribui um valor padrão caso seja necessário
Min_Percentual_Porte_Lean = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_Porte_Lean")).collect()[0][0] or 0.0001
Min_Percentual_NatJur_Lean = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_NatJur_Lean")).collect()[0][0] or 0.0001
Min_Percentual_CNAE_Lean = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_CNAE_Lean")).collect()[0][0] or 0.0001
Min_Percentual_Porte_Eficiencia = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_Porte_Eficiencia")).collect()[0][0] or 0.0001
Min_Percentual_NatJur_Eficiencia = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_NatJur_Eficiencia")).collect()[0][0] or 0.0001
Min_Percentual_CNAE_Eficiencia = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_CNAE_Eficiencia")).collect()[0][0] or 0.0001
Min_Percentual_Porte_Transformacao = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_Porte_Transformacao")).collect()[0][0] or 0.0001
Min_Percentual_NatJur_Transformacao = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_NatJur_Transformacao")).collect()[0][0] or 0.0001
Min_Percentual_CNAE_Transformacao = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa.agg(f.min("Percentual_CNAE_Transformacao")).collect()[0][0] or 0.0001


# Aplicando o preenchimento dos nulos e ajustando os valores
base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final = (
    base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa
    .withColumn(
        "Percentual_NatJur_Lean",
        f.when(f.col("Percentual_NatJur_Lean").isNull(), Min_Percentual_NatJur_Lean - 0.0001)
         .otherwise(f.col("Percentual_NatJur_Lean"))
    ) \
    .withColumn(
        "Percentual_Porte_Lean",
        f.when(f.col("Percentual_Porte_Lean").isNull(), Min_Percentual_Porte_Lean - 0.0001)
         .otherwise(f.col("Percentual_Porte_Lean"))
    ) \
    .withColumn(
        "Percentual_CNAE_Lean",
        f.when(f.col("Percentual_CNAE_Lean").isNull(), Min_Percentual_CNAE_Lean - 0.0001)
         .otherwise(f.col("Percentual_CNAE_Lean"))
    ) \
    .withColumn(
        "Percentual_NatJur_Eficiencia",
        f.when(f.col("Percentual_NatJur_Eficiencia").isNull(), Min_Percentual_NatJur_Eficiencia - 0.0001)
         .otherwise(f.col("Percentual_NatJur_Eficiencia"))
    ) \
    .withColumn(
        "Percentual_CNAE_Eficiencia",
        f.when(f.col("Percentual_CNAE_Eficiencia").isNull(), Min_Percentual_CNAE_Eficiencia - 0.0001)
         .otherwise(f.col("Percentual_CNAE_Eficiencia"))
    ) \
    .withColumn(
        "Percentual_Porte_Eficiencia",
        f.when(f.col("Percentual_Porte_Eficiencia").isNull(), Min_Percentual_Porte_Eficiencia - 0.0001)
         .otherwise(f.col("Percentual_Porte_Eficiencia"))
    ) \
    .withColumn(
        "Percentual_NatJur_Transformacao",
        f.when(f.col("Percentual_NatJur_Transformacao").isNull(), Min_Percentual_NatJur_Transformacao - 0.0001)
         .otherwise(f.col("Percentual_NatJur_Transformacao"))
    ) \
    .withColumn(
        "Percentual_CNAE_Transformacao",
        f.when(f.col("Percentual_CNAE_Transformacao").isNull(), Min_Percentual_CNAE_Transformacao - 0.0001)
         .otherwise(f.col("Percentual_CNAE_Transformacao"))
    ) \
    .withColumn(
        "Percentual_Porte_Transformacao",
        f.when(f.col("Percentual_Porte_Transformacao").isNull(), Min_Percentual_Porte_Transformacao - 0.0001)
         .otherwise(f.col("Percentual_Porte_Transformacao"))
    )
)

# COMMAND ----------

from pyspark.sql.types import FloatType, DecimalType
from pyspark.sql.functions import round, lit, format_number, col


base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final \
.select('SG_UF', 'DS_MUNICIPIO', 'DS_CAPITAL', 'NM_MESORREGIAO', 'NM_MICRORREGIAO', 'CD_CNPJ','CD_CNPJ_BASICO', 'NM_RAZAO_SOCIAL_RECEITA_EMPRESA', 'Porte_final', 'CD_CNAE20_DIVISAO', 'NM_TIPO_LOGRADOURO', 'NM_LOGRADOURO', 'NM_NUMERO_ESTBL', 'NM_COMPLEMENTO', 'NM_BAIRRO', 'CD_CEP', 'NM_DDD_1',  'NM_TELEFONE_1', 'NM_DDD_2',  'NM_TELEFONE_2', 'NM_EMAIL',  'DS_NAT_JURIDICA_RFB', 'Percentual_Porte_Lean',  'Percentual_NatJur_Lean', 'Percentual_CNAE_Lean',  'Percentual_Porte_Eficiencia',  'Percentual_NatJur_Eficiencia', 'Percentual_CNAE_Eficiencia', 'Percentual_Porte_Transformacao', 'Percentual_NatJur_Transformacao', 'Percentual_CNAE_Transformacao', 'DS_PORTE_EMPRESA', 'PORTE_SEBRAE','DS_MATRIZ_FILIAL', 'Já é cliente?') \
.drop('SUGESTÃO MODALIDADE', 'CD_CNAE20_SUBCLASSE_RAIS', 'QUAL O MELHOR CARGO PARA O DECISOR B+P? (GERENTE/COORDENADOR DE PRODUÇÃO E/OU GERENTE DE RH)',  'CONTATO MELHOR CARGO B+P', 'CD_CNAE20_SUBCLASSE_RFB', 'QT_VINC_ATIV',  'Porte_Concat_BU', 'NatJur_Concat_BU', 'CNAE_Concat_BU',
 'Porte_Concat_Lean', 'Natureza_Juridica_Concat_Lean', 'CNAE_Concat_Lean', 'Porte_Concat_Eficiencia', 'Natureza_Juridica_Concat_Eficiencia', 'CNAE_Concat_Eficiencia', 'CNAE_Concat_Tranformacao', 'NatJur_Concat_Tranformacao', 'ds_municipio_base_municipio', 'CD_CNAE20_DIVISAO_AJUST','Porte_final') \
.withColumnRenamed("SG_UF","UF") \
.withColumnRenamed("DS_MUNICIPIO","MUNICIPIO") \
.withColumnRenamed("CD_CNPJ_BASICO","CNPJ_EMPRESA") \
.withColumnRenamed("CD_CNPJ","CNPJ_ESTABELCIMENTO") \
.withColumnRenamed("NM_RAZAO_SOCIAL_RECEITA_EMPRESA","RAZAO_SOCIAL") \
.withColumnRenamed("CD_CNAE20_DIVISAO","CNAE_DIVISAO_CÓDIGO") \
.withColumnRenamed("DS_CNAE20_DIVISAO","CNAE_DIVISAO_NOME") \
.withColumnRenamed("NM_TIPO_LOGRADOURO","ENDERECO_LOGRADOURO") \
.withColumnRenamed("NM_LOGRADOURO","ENDEREÇO_NOME") \
.withColumnRenamed("NM_NUMERO_ESTBL","ENDEREÇO_NUMERO") \
.withColumnRenamed("NM_COMPLEMENTO","ENDEREÇO_COMPLEMENTO") \
.withColumnRenamed("NM_BAIRRO","ENDEREÇO_BAIRRO") \
.withColumnRenamed("CD_CEP","ENDEREÇO_CEP") \
.withColumnRenamed("NM_DDD_1","TELEFONE_DDD") \
.withColumnRenamed("NM_TELEFONE_1","TELEFONE_NÚMERO") \
.withColumnRenamed("NM_DDD_2","TELEFONE_DDD2") \
.withColumnRenamed("NM_TELEFONE_2","TELEFONE_NÚMERO2") \
.withColumnRenamed("NM_EMAIL","EMAIL") \
.withColumnRenamed("DS_NAT_JURIDICA_RFB","NATUREZA_JURIDICA") \
.withColumnRenamed("ds_capital","CAPITAL?") \
.withColumnRenamed("nm_mesorregiao","MESORREGIAO") \
.withColumnRenamed("nm_microrregiao","MICRORREGIAO") \
.withColumn(
        "TELEFONE_DDD",
        when(col("TELEFONE_DDD").isNull(), "-")
        .otherwise(col("TELEFONE_DDD"))
    ) \
.withColumn(
        "TELEFONE_NÚMERO",
        when(col("TELEFONE_NÚMERO").isNull(), "-")
        .otherwise(col("TELEFONE_NÚMERO"))
    ) \
.withColumn(
        "TELEFONE_DDD2",
        when(col("TELEFONE_DDD2").isNull(), "-")
        .otherwise(col("TELEFONE_DDD2"))
    ) \
.withColumn(
        "TELEFONE_NÚMERO2",
        when(col("TELEFONE_NÚMERO2").isNull(), "-")
        .otherwise(col("TELEFONE_NÚMERO2"))
    ) \
.withColumn(
        "EMAIL",
        when(col("EMAIL").isNull(), "-")
        .otherwise(col("EMAIL"))
    ) \
.withColumn("Possui e-mail",
            when(col("EMAIL") == "-", "Não")
            .otherwise(f.lit("Sim"))
            ) \
.withColumn("Tipo_Telefone1",
           when(col("TELEFONE_NÚMERO") == "-", "Não existe")
           .when((f.length(f.col("TELEFONE_NÚMERO")) == 8) & (f.col("TELEFONE_NÚMERO").substr(1, 1).isin("9", "8")), "Celular")
           .when((f.length(f.col("TELEFONE_NÚMERO")) == 8) & (f.col("TELEFONE_NÚMERO").substr(1, 1).isin("2", "3", "4", "5", "6", "7")), "Fixo")
           .when((f.length(f.col("TELEFONE_NÚMERO")) < 8) & (f.col("TELEFONE_NÚMERO").substr(1, 1).isin("9", "8")), "Inválido")
           .when((f.length(f.col("TELEFONE_NÚMERO")) == 8) & (f.col("TELEFONE_NÚMERO").substr(1, 1).isin("0", "1")), "Inválido")
) \
.withColumn("Tipo_Telefone2",
           when(col("TELEFONE_NÚMERO2") == "-", "Não existe")
           .when((f.length(f.col("TELEFONE_NÚMERO2")) == 8) & (f.col("TELEFONE_NÚMERO2").substr(1, 1).isin("9", "8")), "Celular")
           .when((f.length(f.col("TELEFONE_NÚMERO2")) == 8) & (f.col("TELEFONE_NÚMERO2").substr(1, 1).isin("2", "3", "4", "5", "6", "7")), "Fixo")
           .when((f.length(f.col("TELEFONE_NÚMERO2")) < 8) & (f.col("TELEFONE_NÚMERO2").substr(1, 1).isin("9", "8")), "Inválido")
           .when((f.length(f.col("TELEFONE_NÚMERO2")) == 8) & (f.col("TELEFONE_NÚMERO2").substr(1, 1).isin("0", "1")), "Inválido")
) \
.withColumn("Tipo_Telefone",
            when((col("Tipo_Telefone1") == "Fixo") & (col("Tipo_Telefone2") == "Celular"), "Ambos")
            .when((col("Tipo_Telefone1") == "Celular") & (col("Tipo_Telefone2") == "Fixo"), "Ambos")
            .when((col("Tipo_Telefone1") == "Fixo") & (col("Tipo_Telefone2") == "Fixo"), "Fixo")
            .when((col("Tipo_Telefone1") == "Celular") & (col("Tipo_Telefone2") == "Celular"), "Celular")
            .when((col("Tipo_Telefone1") == "Não existe") & (col("Tipo_Telefone2") == "Celular"), "Celular")
            .when((col("Tipo_Telefone1") == "Não existe") & (col("Tipo_Telefone2") == "Fixo"), "Fixo")
            .when((col("Tipo_Telefone1") == "Fixo") & (col("Tipo_Telefone2") == "Não existe"), "Fixo")
            .when((col("Tipo_Telefone1") == "Celular") & (col("Tipo_Telefone2") == "Não existe"), "Celular")
            .otherwise("Não existe ")
            ) \
.withColumn("Classificação M1 Novo Brasil + Produtivo", lit('*')) \
.withColumn("Percentual_Porte_Lean_Decimal", round(col("Percentual_Porte_Lean").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Percentual_NatJur_Lean_Decimal", round(col("Percentual_NatJur_Lean").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Percentual_CNAE_Lean_Decimal", round(col("Percentual_CNAE_Lean").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Classificação M3 - Lean Manufacturing Nota", col("Percentual_Porte_Lean_Decimal")* col("Percentual_NatJur_Lean_Decimal") * col("Percentual_CNAE_Lean_Decimal")) \
.withColumn(
        "Classificação M3 - Lean Manufacturing Final",
        regexp_replace(col("Classificação M3 - Lean Manufacturing Nota").cast("string"), "\.", ",")
    ) \
.withColumn("Percentual_Porte_Eficiencia_Decimal", round(col("Percentual_Porte_Eficiencia").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Percentual_NatJur_Eficiencia_Decimal", round(col("Percentual_NatJur_Eficiencia").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Percentual_CNAE_Eficiencia_Decimal", round(col("Percentual_CNAE_Eficiencia").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Classificação M3 - Eficiência Energética Nota", col("Percentual_Porte_Eficiencia_Decimal")* col("Percentual_NatJur_Eficiencia_Decimal") * col("Percentual_CNAE_Eficiencia_Decimal")) \
.withColumn(
        "Classificação M3 - Eficiência Energética Final",
        regexp_replace(col("Classificação M3 - Eficiência Energética Nota").cast("string"), "\.", ",")
    ) \
.withColumn("Percentual_Porte_Transformacao_Decimal", round(col("Percentual_Porte_Transformacao").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Percentual_NatJur_Transformacao_Decimal", round(col("Percentual_NatJur_Transformacao").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Percentual_CNAE_Transformacao_Decimal", round(col("Percentual_CNAE_Transformacao").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.withColumn("Classificação M4 - Transformação Digital Nota", col("Percentual_Porte_Transformacao_Decimal")* col("Percentual_NatJur_Transformacao_Decimal") * col("Percentual_CNAE_Transformacao_Decimal")) \
.withColumn(
        "Classificação M4 - Transformação Digital Final",
        regexp_replace(col("Classificação M4 - Transformação Digital Nota").cast("string"), "\.", ",")
    ) \
.withColumn("Classificação M4 - Smart Factory", lit("*")) \
.withColumn("Percentual_Porte_Eficiencia_Decimal", round(col("Percentual_Porte_Eficiencia").cast(FloatType()), 2).cast(DecimalType(10, 2))) \
.drop('Valor_Porte_Lean', 'Valor_NatJur_Lean', 'Valor_CNAE_Lean', 'Valor_Porte_Eficiencia', 'Valor_NatJur_Eficiencia', 'Valor_CNAE_Eficiencia', 'Smart_Factory_Micro_Pequeno_Maior4Vinc', 'Valor_Porte_Lean_Decimal','Valor_NatJur_Lean_Decimal','Valor_CNAE_Lean_Decimal','Valor_Porte_Eficiencia_Decimal','Valor_NatJur_Eficiencia_Decimal','Valor_CNAE_Eficiencia_Decimal') \
 .withColumn(
        "Classificação M3 - Lean Manufacturing",
        when(col("Classificação M3 - Lean Manufacturing Nota").between(0, 0.001134), "BAIXA")
        .when(col("Classificação M3 - Lean Manufacturing Nota").between(0.001135, 0.007182), "MÉDIA")
        .otherwise("ALTA")
    ) \
 .withColumn(
        "Classificação M3 - Eficiência Energética",
        when(col("Classificação M3 - Eficiência Energética Nota").between(0, 0.001590), "BAIXA")
        .when(col("Classificação M3 - Eficiência Energética Nota").between(0.001591, 0.008904), "MÉDIA")
        .otherwise("ALTA")
    ) \
 .withColumn(
        "Classificação M4 - Transformação Digital",
        when(col("Classificação M4 - Transformação Digital Nota").between(0, 0.001590), "BAIXA")
        .when(col("Classificação M4 - Transformação Digital Nota").between(0.001591, 0.008904), "MÉDIA")
        .otherwise("ALTA")
    ) \
.drop("Percentual_Porte_Lean", "Percentual_NatJur_Lean", "Percentual_CNAE_Lean", "Percentual_Porte_Lean_Decimal", "Percentual_NatJur_Lean_Decimal", "Percentual_CNAE_Lean_Decimal",  
      "Classificação M3 - Lean Manufacturing Nota", "Percentual_Porte_Eficiencia", "Percentual_NatJur_Eficiencia", "Percentual_CNAE_Eficiencia", "Percentual_Porte_Eficiencia_Decimal","Percentual_NatJur_Eficiencia_Decimal", "Percentual_CNAE_Eficiencia_Decimal", "Classificação M3 - Eficiência Energética Nota",    "Percentual_Porte_Transformacao", "Percentual_NatJur_Transformacao", "Percentual_CNAE_Transformacao","Percentual_Porte_Transformacao_Decimal", "Percentual_NatJur_Transformacao_Decimal", "Percentual_CNAE_Transformacao_Decimal", "Classificação M4 - Transformação Digital Nota")

# COMMAND ----------

# inclusão do campo DS_CNAE20_DIVISAO 
base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida \
    .join(cnaes20_divisao, base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida["CNAE_DIVISAO_CÓDIGO"] == cnaes20_divisao["CD_CNAE20_DIVISAO"], "left") \
    .drop("CD_CNAE20_DIVISAO") \
    .withColumnRenamed("DS_CNAE20_DIVISAO", "CNAE Divisão Nome")

# COMMAND ----------

# DBTITLE 1,BASE NACIONAL
empresas_base_nacional = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida \
  .select("CNPJ_EMPRESA", "UF") \
  .withColumnRenamed("CNPJ_EMPRESA","CNPJ_EMPRESA_BN") \
  .distinct() \
  .groupBy("CNPJ_EMPRESA_BN") \
  .agg(f.count("UF").alias("count_UF")) \
  .orderBy(f.col("count_UF").desc()) \
  .filter(f.col("count_UF") > 1)

# COMMAND ----------

# REMOVENDO AS EMPRESAS DE BASE NACIONAL

base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida_semBN = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida \
  .join(empresas_base_nacional, f.col('CNPJ_EMPRESA') == f.col('CNPJ_EMPRESA_BN'), how='anti')

# COMMAND ----------

# Filtro para listar as empresas quando CNPJ_EMPRESA > 1 e assim retornar somente a matriz na base principal

lista_cnpj8_remocao_filiais = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida_semBN \
  .select("CNPJ_EMPRESA", "CNPJ_ESTABELCIMENTO", "UF", "DS_PORTE_EMPRESA", "PORTE_SEBRAE", "DS_MATRIZ_FILIAL") \
  .withColumnRenamed("CNPJ_EMPRESA", "CNPJ_EMPRESA_Remove_Filial") \
  .filter((f.col("DS_PORTE_EMPRESA") == "Demais") & (f.col("PORTE_SEBRAE") == "Grande") & (f.col("DS_MATRIZ_FILIAL") == "Matriz"))

# COMMAND ----------

  lista_cnpj8_remocao_filiais_parte2 = lista_cnpj8_remocao_filiais \
  .join(base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida_semBN, lista_cnpj8_remocao_filiais["CNPJ_EMPRESA_Remove_Filial"] == base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida_semBN["CNPJ_EMPRESA"],"left")

# COMMAND ----------

lista_cnpj8_remocao_filiais_parte3 = lista_cnpj8_remocao_filiais_parte2 \
  .groupBy("CNPJ_EMPRESA_Remove_Filial") \
  .agg(f.count("CNPJ_EMPRESA_Remove_Filial").alias("count_CNPJ8")) \
  .orderBy(f.col("count_CNPJ8").desc()) \
  .filter(f.col("count_CNPJ8") > 1)

# COMMAND ----------

# Filtro para remover as filiais caso esteja nos critérios acima considerando somente a matriz e excluindo a filial

base_final_brasil_mais_produtivo = base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida_semBN \
  .join(lista_cnpj8_remocao_filiais_parte3, 
        lista_cnpj8_remocao_filiais_parte3["CNPJ_EMPRESA_Remove_Filial"] == base_unica_Industria_ativas_smei_vinc_sengajadas_filtrado_completa_td_final_saida_semBN["CNPJ_EMPRESA"], 
        "left")  

# COMMAND ----------

base_final_brasil_mais_produtivo_2 = base_final_brasil_mais_produtivo \
.withColumn("Remover filial?", f.when(f.col("count_CNPJ8").isNotNull(), "Sim").otherwise("Não")) \
  .withColumn("Apagar linha?",
    f.when(
        (f.col("Remover filial?") == "Sim") & (f.col("DS_MATRIZ_FILIAL") == "Filial"), "Sim"
    ).otherwise("Não")
  )

# COMMAND ----------

base_final_brasil_mais_produtivo_3 = base_final_brasil_mais_produtivo_2 \
  .filter(f.col("Apagar linha?") != "Sim")  

# COMMAND ----------

# DBTITLE 1,58. Seguir daqui com novas regras
# Limpeza e ajustes da base

base_final_brasil_mais_produtivo_tratada = base_final_brasil_mais_produtivo_3 \
  .filter(f.col("CNPJ_ESTABELCIMENTO") != "94890266000877") \
  .filter(f.col("UF") != "EX") \
  .withColumn(
        "DS_PORTE_EMPRESA",
        f.when(
            f.col("DS_PORTE_EMPRESA").isNotNull(),
            f.when(f.col("DS_PORTE_EMPRESA") == "Micro Empresa", "Micro")
             .when(f.col("DS_PORTE_EMPRESA") == "Empresa de Pequeno Porte", "Pequena")
             .otherwise(f.col("DS_PORTE_EMPRESA"))
    )) \
  .withColumnRenamed("CNPJ_ESTABELCIMENTO", "CNPJ Estabelecimento") \
  .withColumnRenamed("CNPJ_EMPRESA", "CNPJ Empresa") \
  .withColumnRenamed("RAZAO_SOCIAL", "Razão Social") \
  .withColumnRenamed("MESORREGIAO", "Mesorregião") \
  .withColumnRenamed("MICRORREGIAO", "Microrregião") \
  .withColumnRenamed("MUNICIPIO", "Município") \
  .withColumnRenamed("NATUREZA_JURIDICA", "Natureza Jurídica") \
  .withColumnRenamed("DS_PORTE_EMPRESA", "Porte RFB - Faturamento") \
  .withColumnRenamed("PORTE_SEBRAE", "Porte SEBRAE - Nº de funcionários") \
  .withColumnRenamed("CNAE_DIVISAO_CÓDIGO", "CNAE Divisão Código") \
  .withColumnRenamed("DS_CNAE20_DIVISAO", "CNAE Divisão Nome") \
  .withColumnRenamed("ENDERECO_LOGRADOURO", "Endereço Logradouro") \
  .withColumnRenamed("ENDEREÇO_NOME", "Endereço Nome") \
  .withColumnRenamed("ENDEREÇO_NUMERO", "Endereço Número") \
  .withColumnRenamed("ENDEREÇO_COMPLEMENTO", "Endereço Complemento") \
  .withColumnRenamed("ENDEREÇO_BAIRRO", "Endereço Bairro") \
  .withColumnRenamed("TELEFONE_DDD", "Telefone DDD") \
  .withColumnRenamed("TELEFONE_NÚMERO", "Telefone Número") \
  .withColumnRenamed("TELEFONE_DDD2", "Telefone DDD2") \
  .withColumnRenamed("TELEFONE_NÚMERO2", "Telefone Número2") \
  .withColumnRenamed("ENDEREÇO_CEP", "Endereço CEP") \
  .withColumnRenamed("EMAIL", "E-mail") \
  .withColumnRenamed("DS_MATRIZ_FILIAL", "Matriz/Filial") \
  .withColumnRenamed("CAPITAL?", "Capital?") \
  .select('UF', 'CNPJ Estabelecimento', 'CNPJ Empresa', 'Razão Social', 'Classificação M1 Novo Brasil + Produtivo', 'Classificação M3 - Lean Manufacturing',  'Classificação M3 - Eficiência Energética', 'Classificação M4 - Transformação Digital', 'Classificação M4 - Smart Factory', 'Mesorregião', 'Microrregião','Município', 'Natureza Jurídica', 'Porte RFB - Faturamento', 'Porte SEBRAE - Nº de funcionários', 'CNAE Divisão Código', 'CNAE Divisão Nome', 'Endereço Logradouro', 'Endereço Nome', 'Endereço Número', 'Endereço Complemento', 'Endereço Bairro', 'Endereço CEP', 'Telefone DDD','Telefone Número', 'Telefone DDD2', 'Telefone Número2', 'E-mail', 'Matriz/Filial','Capital?','Tipo_Telefone', 'Já é cliente?', 'Possui e-mail')

# COMMAND ----------

base_final_brasil_mais_produtivo_tratada_impressao = base_final_brasil_mais_produtivo_tratada \
.withColumn("Elegível",
    when(
        (col("Porte RFB - Faturamento") == "Demais") & (col("Porte SEBRAE - Nº de funcionários") == "Grande"),"Não"
    ).otherwise("Sim")
)

# COMMAND ----------

base_final_brasil_mais_produtivo_tratada_impressao.count()

# COMMAND ----------

# DBTITLE 1,Gravação da Base  UDS
# base_final_brasil_mais_produtivo_tratada_impressao.coalesce(1) \
#   .write \
#   .mode('overwrite') \
#   .option("delimiter", ";") \
#   .option("header", "true") \
#   .option("encoding", "ISO-8859-1") \
#   .csv("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/unimercado/saida_Brasil_Mais_Produtivo/listaEmpresas_v4_2025/")

# COMMAND ----------

# DBTITLE 1,Gravação da Base em Parquet
# # Salva na UDS
# base_final_brasil_mais_produtivo_tratada_impressao.write.mode('overwrite').parquet(var_adls_uri + '/uds/oni/observatorio_nacional/dimensionamento_sesi/base_industrias/', compression='snappy')