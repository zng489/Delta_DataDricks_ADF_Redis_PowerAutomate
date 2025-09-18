# Databricks notebook source
# 1.1.1 Geral

ANO_REFERENCIA = 2021

# COMMAND ----------

# 1.1.2 Polo regional

reg_quantidade_empresas = 3
reg_multiplo_emprego_regional_intensivo = 0.0018
reg_multiplo_emprego_regional_nao_intensivo = 0.0012

# COMMAND ----------

# 1.1.3 Polo estadual

est_multiplo_emprego_regional_nao_intensivo = 0.0012
est_multiplo_emprego_regional_intensivo = 0.0018
est_min_emp_micro_media = 20
est_min_emp_grande = 1
est_part_emp_estado = 0.01
est_ql_estadual_emp = 0.7

# COMMAND ----------

# 1.1.4 Polo nacional

nac_multiplo_emprego_regional_intens = 0.0018
nac_multiplo_emprego_regional_nao_intens = 0.0012
nac_multiplo_emprego_nacional_intens = 0.0003
nac_multiplo_emprego_nacional_nao_intens = 0.0003
nac_min_emp_micro_media = 30
nac_min_emp_grande = 2
nac_ql_nac_emp = 0.8

# COMMAND ----------

# 1.1.5 Polo internacional

perc_insercao_int = 0.001


# COMMAND ----------

# 1.2 Listas e Dicionários

# 1.2.1 Código estados sul /sudeste

lista_sul_sudeste = ['31','41','33','43','42','35']

# COMMAND ----------

# 1.2.2 lista de cnaes para o estudo

lista_cnaes = ['10',    '11',   '22',   '41',   '42',   '43',   '05',   '06',   '09',   '19',   '35',   '27',   '07',   '21',   '16','31',  '28',   '33',   '24',   '25',   '08',   '23',   '17',   '12',   '18',   '32',   '20',   '36',   '37',   '38','39',  '13',   '14',   '15',   '26',   '61',   '62',   '63',   '49',   '50',   '51',   '52',   '53',   '29',   '30']

# COMMAND ----------

# 1.2.3 Dicionário estados

dic_estados = {
    12: 'Acre',
    27: 'Alagoas',
    16: 'Amapá',
    13: 'Amazonas',
    29: 'Bahia',
    23: 'Ceará',
    53: 'Distrito Federal',
    32: 'Espírito Santo',
    52: 'Goiás',
    21: 'Maranhão',
    51: 'Mato Grosso',
    50: 'Mato Grosso do Sul',
    31: 'Minas Gerais',
    15: 'Pará',
    25: 'Paraíba',
    41: 'Paraná',
    26: 'Pernambuco',
    22: 'Piauí',
    33: 'Rio de Janeiro',
    24: 'Rio Grande do Norte',
    43: 'Rio Grande do Sul',
    11: 'Rondônia',
    14: 'Roraima',
    42: 'Santa Catarina',
    35: 'São Paulo',
    28: 'Sergipe',
    17: 'Tocantins'
}

dic_sg_uf = {
'AC': 'Acre',
'AL': 'Alagoas',
'AP': 'Amapá',
'AM': 'Amazonas',
'BA': 'Bahia',
'CE': 'Ceará',
'DF': 'Distrito Federal',
'ES': 'Espírito Santo',
'GO': 'Goiás',
'MA': 'Maranhão',
'MT': 'Mato Grosso',
'MS': 'Mato Grosso do Sul',
'MG': 'Minas Gerais',
'PA': 'Pará',
'PB': 'Paraíba',
'PR': 'Paraná',
'PE': 'Pernambuco',
'PI': 'Piauí',
'RJ': 'Rio de Janeiro',
'RN': 'Rio Grande do Norte',
'RS': 'Rio Grande do Sul',
'RO': 'Rondônia',
'RR': 'Roraima',
'SC': 'Santa Catarina',
'SP': 'São Paulo',
'SE': 'Sergipe',
'TO': 'Tocantins' }

# COMMAND ----------

# 1.3 Importação bibliotecas

import pandas as pd
import requests as res
import os
import time
from pyspark.sql.types import *
from itertools import chain
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# 1.4 Criação de função para usar o dicionário para estados

mapping_expr = create_map([lit(x) for x in chain(*dic_estados.items())])

# COMMAND ----------

# 1.5 Importação dimensão município

# leitura da base
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/biz/oni/bases_referencia/municipios'.format(uri=var_adls_uri)
dim_mun_cni = spark.read.format("parquet").load(path)

# seleção das variáveis

dim_mun_cni = dim_mun_cni.select("cd_uf",dim_mun_cni["cd_ibge_6"].alias("cd_municipio"),"nm_mesorregiao")

dim_mun_cni = dim_mun_cni.withColumn(
    "cd_municipio",
    when(dim_mun_cni["cd_municipio"] == 430145, 431454).otherwise(dim_mun_cni["cd_municipio"])
)

# COMMAND ----------

# 1.6 Importação dimensão CNAE

# Leitura da base
url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRsq1XpeHYTD-CxaIEQie8FJRK2oGwpAU3NQgQMPAbTeybUvOqWzxsRBsQwfR9w6jA42yfXnY1h5dyl/pub?output=csv'
dim_cnae = spark.createDataFrame(pd.read_csv(url, encoding="UTF-8", sep=",")).select('cd_div','cd_grupo', 'nm_grupo','cd_subclasse')

dim_cnae = dim_cnae.withColumn('cd_subclasse', format_string('%07d', col('cd_subclasse').cast('int'))) \
                    .withColumn('cd_div', format_string('%02d', col('cd_div').cast('int'))) \
                    .withColumn('cd_grupo', format_string('%03d', col('cd_grupo').cast('int')))

dim_cnae = dim_cnae.withColumn('setor_intensivo', when((col('cd_div') == '17') | 
                                                        (col('cd_div') == '20') | 
                                                        (col('cd_div') == '22') | 
                                                        (col('cd_div') == '05') | 
                                                        (col('cd_div') == '09') | 
                                                        (col('cd_div') == '26') | 
                                                        (col('cd_div') == '27') | 
                                                        (col('cd_div') == '28') |
                                                        (col('cd_div') == '05') | 
                                                        (col('cd_div') == '06') | 
                                                        (col('cd_div') == '07') | 
                                                        (col('cd_div') == '08') | 
                                                        (col('cd_div') == '09'), lit('Não')).otherwise(lit('Sim')))

# COMMAND ----------

# 1.7 Leitura rais vínculos

# Leitura da base
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/trs/me/rais_vinculo_publica/'.format(uri=var_adls_uri)
df_rais = spark.read.format("parquet").load(path).filter(col("CD_CNAE20_CLASSE").substr(1, 2).isin(lista_cnaes))
df_rais = df_rais.withColumnRenamed('CD_UF', 'cd_uf') \
                  .withColumnRenamed('CD_MUNICIPIO', 'cd_municipio') \
                  .withColumnRenamed('CD_CNAE20_SUBCLASSE', 'cd_subclasse') \
                  .withColumnRenamed('FL_VINCULO_ATIVO_3112', 'in_vinculo_ativo') 

# COMMAND ----------

# 1.8 Filtros rais e seleção de colunas necessárias

## Filtrando para vinculos ativos
df_rais = df_rais.filter(col('in_vinculo_ativo') == 1)

## Filtrando para o ano de referencia
df_rais = df_rais.filter(col('ANO') == ANO_REFERENCIA)

## Selecionando colunas necessárias
df_rais = df_rais.select('cd_uf','cd_municipio', 'cd_subclasse', 'in_vinculo_ativo')
df_rais = df_rais.withColumn('nm_uf', mapping_expr[col('cd_uf')])

# COMMAND ----------

# 1.9 Criação indicador participação de empregos na mesorregião
## Join com mesoregião
df_rais = df_rais.join(dim_mun_cni, ['cd_municipio', 'cd_uf'], how='left')

## Join com cnae
df_rais = df_rais.join(dim_cnae, ['cd_subclasse'], how='left')

## Count da sc_competitiva na região
df_cnae_meso = df_rais.groupBy('nm_uf', 'cd_uf', 'nm_mesorregiao', 'cd_grupo', 'nm_grupo', 'cd_div', 'setor_intensivo').agg(count('in_vinculo_ativo').alias('qtd_emprego_meso_setor'))

## Count da região
df_meso = df_rais.groupBy('cd_uf','nm_uf', 'nm_mesorregiao').agg(sum('in_vinculo_ativo').alias('qtd_emprego_meso'))

## Join total com local
df_polo_regional = df_cnae_meso.join(df_meso, ['cd_uf','nm_uf', 'nm_mesorregiao'], how = 'left')

## Porcentagem para polo regional
df_polo_regional = df_polo_regional.withColumn('participacao_emprego_meso', round((col('qtd_emprego_meso_setor')/col('qtd_emprego_meso')),4))

# COMMAND ----------

# 1.10 Cálculo do QL estadual

## Count da qtd de empregos no setor no estado
df_emprego_setor_estado = df_rais.groupBy('nm_uf', 'cd_uf', 'cd_grupo', 'nm_grupo', 'cd_div').agg(sum('in_vinculo_ativo').alias('qtd_emprego_setor_estado'))

## Count da qtd de empregos no estado
df_emprego_estado = df_rais.groupBy('nm_uf', 'cd_uf').agg(sum('in_vinculo_ativo').alias('qtd_emprego_estado'))

## Join para divisao
df_divisor_ql_regional = df_emprego_setor_estado.join(df_emprego_estado, ['nm_uf', 'cd_uf'], how = 'left')

## Join com o numerador do QL
df_ql_regional = df_polo_regional.join(df_divisor_ql_regional, ['nm_uf', 'cd_uf', 'cd_grupo', 'nm_grupo', 'cd_div'], how = 'left')
df_ql_regional = df_ql_regional.withColumn('participacao_emprego_estado', round((col('qtd_emprego_setor_estado')/col('qtd_emprego_estado')),4))
df_ql_regional = df_ql_regional.withColumn('QL_regional', round(col('participacao_emprego_meso')/col('participacao_emprego_estado'),4))

# COMMAND ----------

# 1.11 Cálculo do índice de GINI - Regional
window_spec = Window.partitionBy("nm_uf", "nm_mesorregiao").orderBy(desc(col("QL_regional")), rand())
df_ql_regional = df_ql_regional.withColumn(
    "participacao_acumulada",
    sum("participacao_emprego_meso").over(window_spec)
)
df_ql_regional = df_ql_regional.withColumn("participacao_acumulada_anterior", lag("participacao_acumulada").over(window_spec))
window_spec_2 = Window.partitionBy("nm_uf", "nm_mesorregiao")
df_ql_regional = df_ql_regional.withColumn(
    "GINI",
    (sum((col("participacao_acumulada") + col("participacao_acumulada_anterior")) * col("participacao_emprego_estado")).over(window_spec_2) 
     -(2*(sum(col("participacao_emprego_estado")).over(window_spec_2))) 
     + 1)
)
df_ql_regional = df_ql_regional.withColumn('QL_regional', round(col('QL_regional')*(1-col('GINI')),4)).drop('participacao_acumulada').drop('participacao_acumulada_anterior')

# COMMAND ----------

# 1.12 Cálculo do QL Nacional

## Count da qtd de empregos no setor no brasil
df_emprego_setor_brasil = df_rais.groupBy('cd_grupo', 'nm_grupo', 'cd_div').agg(sum('in_vinculo_ativo').alias('qtd_emprego_setor_brasil'))

## Count da qtd de empregos no brasil
df_emprego_brasil = df_rais.agg(sum('in_vinculo_ativo').alias('qtd_emprego_brasil')).collect()[0][0]

## Join para divisao
df_divisor_ql_nacional = df_emprego_setor_brasil.withColumn('qtd_emprego_brasil', lit(df_emprego_brasil))

## Join com o numerador do QL
df_empregos = df_ql_regional.join(df_divisor_ql_nacional, ['cd_grupo', 'nm_grupo', 'cd_div'], how = 'left')
df_empregos = df_empregos.withColumn('QL_nacional', round(col('participacao_emprego_meso')/(col('qtd_emprego_setor_brasil')/col('qtd_emprego_brasil')),4))
df_empregos = df_empregos.withColumn("participacao_emprego_estado",round(col('qtd_emprego_setor_estado')/col('qtd_emprego_estado'),4)) \
                          .withColumn("participacao_emprego_brasil",round(col('qtd_emprego_setor_brasil')/col('qtd_emprego_brasil'),4))

# COMMAND ----------

# 1.13 Cálculo do índice de GINI - Nacional

window_spec_3 = Window.partitionBy("nm_uf","nm_mesorregiao").orderBy(desc(col("QL_nacional")), rand())
df_empregos = df_empregos.withColumn(
    "participacao_acumulada",
    sum("participacao_emprego_meso").over(window_spec_3)
)
df_empregos = df_empregos.withColumn("participacao_acumulada_anterior", lag("participacao_acumulada").over(window_spec_3))

window_spec_4 = Window.partitionBy("nm_uf","nm_mesorregiao")
df_empregos = df_empregos.withColumn(
    "GINI_nac",
    (sum((col("participacao_acumulada") + col("participacao_acumulada_anterior")) * col("participacao_emprego_brasil")).over(window_spec_4) 
     -(2*(sum(col("participacao_emprego_brasil")).over(window_spec_4))) 
     + 1)
)
df_empregos = df_empregos.withColumn('QL_nacional', round(col('QL_nacional')*(1-col('GINI_nac')),4)).drop('participacao_acumulada').drop('participacao_acumulada_anterior')

# COMMAND ----------

# 1.14 Carregar RAIS estabelecimentos
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/trs/oni/mte/rais/publica/estabelecimento'.format(uri=var_adls_uri)
df_rais_estab = spark.read.format("parquet").load(path).filter(col("NR_ANO") == ANO_REFERENCIA).filter(col("CD_CNAE20_CLASSE").substr(1, 2).isin(lista_cnaes)).withColumn("CD_IBGE_SUBSETOR", col("CD_IBGE_SUBSETOR").cast("integer")).filter(col('FL_IND_RAIS_NEGATIVA') == 'false')

df_rais_estab = df_rais_estab.withColumn(
    "nm_porte",
    when((col("CD_TAMANHO_ESTABELECIMENTO").isin('01', '02', '03', '04')) & (col("CD_IBGE_SUBSETOR") < 16), "Micro")
    .when((col("CD_TAMANHO_ESTABELECIMENTO").isin('05', '06')) & (col("CD_IBGE_SUBSETOR") < 16), "Pequena")
    .when((col("CD_TAMANHO_ESTABELECIMENTO").isin('07', '08')) & (col("CD_IBGE_SUBSETOR") < 16), "Média")
    .when((col("CD_TAMANHO_ESTABELECIMENTO").isin('09', '10')) & (col("CD_IBGE_SUBSETOR") < 16), "Grande")
    .when((col("CD_TAMANHO_ESTABELECIMENTO").isin('01', '02', '03')) & (col("CD_IBGE_SUBSETOR") > 15), "Micro")
    .when((col("CD_TAMANHO_ESTABELECIMENTO").isin('04', '05')) & (col("CD_IBGE_SUBSETOR") > 15), "Pequena")
    .when((col("CD_TAMANHO_ESTABELECIMENTO") == '06') & (col("CD_IBGE_SUBSETOR") > 15), "Média")
    .when((col("CD_TAMANHO_ESTABELECIMENTO").isin('07', '08', '09', '10')) & (col("CD_IBGE_SUBSETOR") > 15), "Grande")
    .otherwise(None)
)

df_rais_estab = df_rais_estab.select(col("CD_MUNICIPIO").alias("cd_municipio"),col("CD_CNAE20_SUBCLASSE").alias("cd_subclasse"),col("CD_UF").alias("cd_uf"),col('nm_porte'))
df_rais_estab = df_rais_estab.withColumn('nm_uf', mapping_expr[col('cd_uf')])
df_rais_estab = df_rais_estab.withColumn('n', lit(1))

# COMMAND ----------

# 1.15 Junção com mesorregião e cálculo de indicadores
## Join com mesoregião
df_rais_estab = df_rais_estab.join(dim_mun_cni, ['cd_municipio', 'cd_uf'], how='left').drop('cd_municipio')

## Join com dim_cnae
df_rais_estab = df_rais_estab.join(dim_cnae, ['cd_subclasse'], how='left').drop('cd_subclasse')

## Count do setor na região
df_empresas_meso = df_rais_estab.groupBy('nm_uf', 'cd_uf', 'nm_mesorregiao', 'cd_grupo', 'nm_grupo', 'cd_div', 'setor_intensivo').agg(sum('n').alias('qtd_empresas_meso'))

## Count do setor na região micro-média
df_empresas_meso_mcr_md = df_rais_estab.filter((col('nm_porte') == 'Micro')|(col('nm_porte') == 'Média')|(col('nm_porte') == 'Pequena')).groupBy('nm_uf', 'cd_uf', 'nm_mesorregiao', 'cd_grupo', 'nm_grupo', 'cd_div', 'setor_intensivo').agg(sum('n').alias('qtd_empresas_micro_media'))

## Count do setor na região grande
df_empresas_meso_grd = df_rais_estab.filter((col('nm_porte') == 'Grande')).groupBy('nm_uf', 'cd_uf', 'nm_mesorregiao', 'cd_grupo', 'nm_grupo','cd_div', 'setor_intensivo').agg(sum('n').alias('qtd_empresas_grande'))

## Count do setor no estado
df_empresas_estado = df_rais_estab.groupBy('nm_uf', 'cd_uf', 'cd_grupo', 'nm_grupo','cd_div', 'setor_intensivo').agg(sum('n').alias('qtd_empresas_estado'))

## Count do setor no brasil
df_empresas_brasil = df_rais_estab.groupBy('cd_grupo', 'nm_grupo', 'cd_div','setor_intensivo').agg(sum('n').alias('qtd_empresas_brasil'))

# COMMAND ----------

# 1.16 Junção das tabelas anteriores em um único dataframe
df_empregos_empresas = df_empregos.join(df_empresas_meso_mcr_md, ['nm_uf', 'cd_uf', 'nm_mesorregiao', 'cd_grupo', 'nm_grupo','cd_div', 'setor_intensivo'], how = 'outer')
df_empregos_empresas = df_empregos_empresas.join(df_empresas_meso_grd, ['nm_uf', 'cd_uf', 'nm_mesorregiao', 'cd_grupo', 'nm_grupo','cd_div', 'setor_intensivo'], how = 'outer')
df_empregos_empresas = df_empregos_empresas.join(df_empresas_meso, ['nm_uf', 'cd_uf', 'nm_mesorregiao', 'cd_grupo', 'nm_grupo','cd_div', 'setor_intensivo'], how = 'outer')
df_empregos_empresas = df_empregos_empresas.join(df_empresas_estado, ['nm_uf', 'cd_uf', 'cd_grupo', 'nm_grupo','cd_div', 'setor_intensivo'], how = 'outer')
df_empregos_empresas = df_empregos_empresas.join(df_empresas_brasil, ['cd_grupo', 'nm_grupo','cd_div', 'setor_intensivo'], how = 'outer')
df_empregos_empresas = df_empregos_empresas.fillna(0)
df_empregos_empresas = df_empregos_empresas.withColumn('participacao_empresa_estado', round((col('qtd_empresas_meso')/col('qtd_empresas_estado')),4))
df_empregos_empresas = df_empregos_empresas.withColumn('participacao_empresa_nacional', round((col('qtd_empresas_meso')/col('qtd_empresas_brasil')),4))

# COMMAND ----------

# 1.17 Cálculo Variável de emprego Estadual com múltiplo
df_empregos_empresas = df_empregos_empresas.withColumn('multiplo_emprego_regional', round((col('qtd_emprego_meso_setor')/col('qtd_emprego_meso'))*(col('qtd_emprego_meso_setor')/(col('qtd_emprego_setor_estado'))),4))

df_empregos_empresas = df_empregos_empresas.withColumn('participacao_emprego_br', round((col('qtd_emprego_meso_setor')/col('qtd_emprego_setor_brasil')),4))


df_empregos_empresas = df_empregos_empresas.withColumn('multiplo_emprego_nacional', round((col('qtd_emprego_meso_setor')*col('qtd_emprego_meso_setor'))/(col('qtd_emprego_setor_brasil')*col('qtd_emprego_meso')),4))

# COMMAND ----------

# 1.18 Filtro para Sul e Sudeste
df_final_sul_sudeste = df_empregos_empresas.filter(col('cd_uf').isin(lista_sul_sudeste)).filter(col('cd_div').isin(lista_cnaes))

# COMMAND ----------

# MAGIC %md
# MAGIC # HTTPError: HTTP Error 410: Gone
# MAGIC # Arquivo Excluido

# COMMAND ----------

# 1.19 Importação dimensão CNAE - SH4 para Sul e Sudeste
url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQxiaqjbw1ELj6ZFZVlT3xwXvYCQ4gEMaXcEyzsvqq70leDG9f3X9KiDNDQkRYrEg/pub?output=csv'
dim_sh4 = spark.createDataFrame(pd.read_csv(url, encoding="UTF-8", sep=","))

dim_sh4 = dim_sh4.withColumn("CD_SH4", lpad(dim_sh4["SH4"], 4, '0').cast("string")) \
                .withColumn("cd_grupo", lpad(dim_sh4["cnae_grupo"], 3, '0'))

dim_sh4 = dim_sh4.withColumn("cd_div", lpad(dim_sh4["cd_grupo"], 2, '0')) \
                .select("CD_SH4","cd_div","cd_grupo")

# COMMAND ----------

# 1.20 Leitura Comex MUN
## COMEX MUN
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/trs/me/comex/mun_exp_p/'.format(uri=var_adls_uri)
df_comex_mun = spark.read.format("parquet").option("header","true").load(path).filter(col('cd_ano').isin([ANO_REFERENCIA]))

# COMMAND ----------

# 1.21 Ajuste de códigos da base da COMEX para alguns estados que estão divergentes do IBGE

# Usar a função `when` para aplicar a mudança
# Definir as condições para as mudanças de prefixo
cond_sp = (col('SG_UF') == 'SP') & (col('CD_MUN').substr(1, 2) == '34')
cond_ms = (col('SG_UF') == 'MS') & (col('CD_MUN').substr(1, 2) == '52')
cond_go = (col('SG_UF') == 'GO') & (col('CD_MUN').substr(1, 2) == '53')
cond_df = (col('SG_UF') == 'DF') & (col('CD_MUN').substr(1, 2) == '54')

# Aplicar as alterações de prefixo com base nas condições
df_comex_mun = df_comex_mun.withColumn(
    'CD_MUN',
    when(cond_sp, expr("concat('35', substring(CD_MUN, 3, length(CD_MUN)-2))"))
    .when(cond_ms, expr("concat('50', substring(CD_MUN, 3, length(CD_MUN)-2))"))
    .when(cond_go, expr("concat('52', substring(CD_MUN, 3, length(CD_MUN)-2))"))
    .when(cond_df, expr("concat('53', substring(CD_MUN, 3, length(CD_MUN)-2))"))
    .otherwise(col('CD_MUN'))  # Mantém o valor original se nenhuma condição for atendida
)

# COMMAND ----------

# MAGIC %md
# MAGIC # HTTPError: HTTP Error 410: Gone
# MAGIC # Arquivo Excluido

# COMMAND ----------

# 1.22 Tratamento COMEX MUN
df_comex_mun = df_comex_mun.withColumnRenamed('CD_ANO', 'cd_ano') \
                            .withColumn('cd_municipio', expr("substring(CD_MUN, 1, length(CD_MUN) - 1)")) \
                            .withColumnRenamed('CD_CNAE20_SUBCLASSE', 'cd_subclasse') \
                            .withColumnRenamed('SG_UF', 'sg_uf') \
                            .withColumnRenamed('VL_FOB', 'nu_fob')

df_comex_mun = df_comex_mun.join(dim_sh4, "CD_SH4", how = "left").filter((col('cd_div').isin(lista_cnaes)))

df_comex_mun = df_comex_mun.join(dim_mun_cni, ['cd_municipio'], how='left')

df_comex_mun = df_comex_mun.groupBy('nm_mesorregiao', 'cd_grupo', 'sg_uf').agg(sum('nu_fob').alias('valor_exp'))

# COMMAND ----------

# 1.23 Leitura comex ncm para pegar exportação por UF
## COMEX NCM
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/trs/me/comex/ncm_exp_p/'.format(uri=var_adls_uri)
df_comex_ncm = spark.read.format("parquet").option("header","true").load(path)

df_comex_ncm = df_comex_ncm.withColumnRenamed('CD_ANO', 'cd_ano') \
                            .withColumnRenamed('CD_CNAE20_SUBCLASSE', 'cd_subclasse') \
                            .withColumnRenamed('SG_UF_NCM', 'sg_uf') \
                            .withColumn("CD_SH4", lpad(df_comex_ncm["CD_NCM"], 4, '0')) \
                            .withColumnRenamed('VL_FOB', 'nu_fob')

# COMMAND ----------

# MAGIC %md
# MAGIC # HTTPError: HTTP Error 410: Gone
# MAGIC # Arquivo Excluido

# COMMAND ----------

# 1.24 Tratamentos na base COMEX NCM

df_comex_ncm = df_comex_ncm.join(dim_sh4, "CD_SH4", how = "left").filter((col('cd_div').isin(lista_cnaes))).filter(col('cd_ano').isin([ANO_REFERENCIA]))

df_comex_ncm = df_comex_ncm.withColumn("nm_uf", col("sg_uf").cast("string"))
for sigla, nome in dic_sg_uf.items():
    df_comex_ncm = df_comex_ncm.withColumn("nm_uf", when(col("sg_uf") == sigla, nome).otherwise(col("nm_uf")))

df_comex_ncm_uf = df_comex_ncm.groupBy('nm_uf','cd_grupo').agg(sum('nu_fob').alias('valor_exportado_uf'))
df_comex_ncm_br = df_comex_ncm.groupBy('cd_grupo').agg(sum('nu_fob').alias('valor_exportado_br'))
df_comex_ncm_uf = df_comex_ncm_br.join(df_comex_ncm_uf, ["cd_grupo"], how = "left")
df_comex_ncm_uf = df_comex_ncm_uf.withColumn('part_uf_setor_exportacao',round(col("valor_exportado_uf")/col("valor_exportado_br"),6))

# COMMAND ----------

# 1.25 Cálculo de indicadores para exportação

#### CRIANDO EXP.SETOR_UF(valor_exportado_uf)/EXP.TOTAL_UF(valor_exportado_uf_total) #######

df_comex_exp_setor = df_comex_ncm_uf.groupBy('nm_uf').agg(sum('valor_exportado_uf').alias('valor_exportado_uf_total'))
df_comex_exp_setor = df_comex_ncm_uf.select('nm_uf', 'cd_grupo', 'valor_exportado_uf').join(df_comex_exp_setor, ['nm_uf'], how='left')
df_comex_exp_setor = df_comex_exp_setor.withColumn('exp_setor_uf/exp_total_uf', round(col('valor_exportado_uf')/col('valor_exportado_uf_total'),6)).drop('valor_exportado_uf').drop('valor_exportado_uf_total')

# COMMAND ----------

# 1.26 Usando dicionário de UF para pegar o nome dos estados pela sigla
# Inicializa a expressão com a primeira condição
expression = when(col("sg_uf") == list(dic_sg_uf.keys())[0], list(dic_sg_uf.values())[0])

# Adiciona as condições subsequentes
for sigla, nome in list(dic_sg_uf.items())[1:]:
    expression = expression.when(col("sg_uf") == sigla, nome)

# Caso nenhuma condição seja atendida, mantém o valor original
df_comex_ncm = df_comex_ncm.withColumn("nm_uf", expression.otherwise(col("sg_uf")))

# Caso nenhuma condição seja atendida, mantém o valor original
df_comex_mun = df_comex_mun.withColumn("nm_uf", expression.otherwise(col("sg_uf")))

# COMMAND ----------

# 1.27 Valor do dolar considerado médio para o ano do estudo e link de extração das informações da produção industrial para o estudo
valor_dolar = 5.3944
# 2021
url = 'https://apisidra.ibge.gov.br/values/t/1848/n1/all/n3/all/v/810/p/2021/c12762/allxt'

# COMMAND ----------

# 1.28 Coleta dos dados da Produção industrial por UF
# Importa os dados do SIDRA quantidade 
request = res.get(url)
df_pia = pd.DataFrame(request.json())

# Substitui as colunas pela primeira observação
df_pia.columns = df_pia.iloc[0]

# Retira a primeira observação
df_pia = df_pia.iloc[1:, :]
df_pia = spark.createDataFrame(df_pia).select(col('Brasil e Unidade da Federação (Código)').alias('cd_uf'), col('Brasil e Unidade da Federação').alias('nm_uf'), col('Ano').alias('nu_ano'), col('`Classificação Nacional de Atividades Econômicas (CNAE 2.0)`').alias('cnae_2_0'), col('Valor').alias('valor_vbpi'))
df_pia = df_pia.filter((col('cnae_2_0').contains('.'))&(col('valor_vbpi') >= 0))

# Separando codigo cnae de nome
df_pia = df_pia.withColumn('cd_grupo', regexp_replace(substring('cnae_2_0', 1, 4), "\.", ""))
df_pia = df_pia.withColumn('nm_grupo', substring('cnae_2_0', 6, 100)).drop('cnae_2_0')

## Join com valor do cambio
#df_pia = df_pia.join(cambio_valor, ['nu_ano'], how='left')

## Ajuste da coluna de valor
df_pia = df_pia.withColumn('valor_vbpi', (col('valor_vbpi').cast('long')*1000)/valor_dolar).filter(col('valor_vbpi').isNotNull())

df_pia = df_pia.withColumn('valor_vbpi', df_pia['valor_vbpi'].cast('long'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Faltando um pedação do código, que le df_participacao_mun_setor

# COMMAND ----------

# 1.29 Otimização da produção industrial por município, através dos dados de emprego
df_pia_participacao = df_participacao_mun_setor.join(df_pia, ['cd_uf', 'cd_grupo'], how='left')

df_pia_participacao = df_pia_participacao.withColumn('vbpi_mun', col('participacao_mun_setor')*col('valor_vbpi')).drop('qtd_emprego_estado_setor_mun').drop('qtd_emprego_estado_setor')

df_pia_participacao = df_pia_participacao.groupBy('cd_uf', 'cd_grupo', 'nm_mesorregiao').agg(sum('vbpi_mun').alias('vbpi_meso'), sum('participacao_mun_setor').alias('participacao_mun_setor'))

df_pia_participacao = df_pia_participacao.withColumn('vbpi_meso', round(col('vbpi_meso'), 4))

# COMMAND ----------

# 1.30 União comex com o df final
df_comex_ncm_uf = df_comex_ncm_uf.select('cd_grupo', 'nm_uf', 'part_uf_setor_exportacao','valor_exportado_br')

df_final_sul_sudeste = df_final_sul_sudeste.join(df_comex_ncm_uf, [df_comex_ncm_uf.cd_grupo==df_final_sul_sudeste.cd_grupo, df_comex_ncm_uf.nm_uf == df_final_sul_sudeste.nm_uf], how = "left").drop(df_comex_ncm_uf.nm_uf)

# COMMAND ----------

# 1.31 União internacional com o df final
## Join com RAIS
df_final_sul_sudeste_internacional = df_final_sul_sudeste.join(df_pia_participacao, ['cd_uf', 'cd_grupo', 'nm_mesorregiao'], how='left')
## Join comex exportação
df_final_sul_sudeste_internacional = df_final_sul_sudeste_internacional.join(df_comex_mun, ['nm_uf', 'nm_mesorregiao', 'cd_grupo'], how='left')

## JOIN COM NOVO MULTIPLICADOR [EXP.SETOR_UF(valor_exportado_uf)/EXP.TOTAL_UF(valor_exportado_uf_total)]
df_final_sul_sudeste_internacional = df_final_sul_sudeste_internacional.join(df_comex_exp_setor, ['nm_uf', 'cd_grupo'], how='left')

df_final_sul_sudeste_internacional = df_final_sul_sudeste_internacional.withColumn('insercao_internacional', round(col('exp_setor_uf/exp_total_uf')*(col('valor_exp')/col('vbpi_meso'))*col('part_uf_setor_exportacao'),3)).dropDuplicates()

# COMMAND ----------

# 1.32 Classificação dos polos
df_polo = df_final_sul_sudeste_internacional.withColumn('Polo_regional', when((col('setor_intensivo') == 'Não') &
                                                                (col('multiplo_emprego_regional') >= reg_multiplo_emprego_regional_nao_intensivo) &
                                                                ((col('qtd_empresas_meso') >= reg_quantidade_empresas)), 'Sim')
                                                          .when((col('setor_intensivo') == 'Sim') &
                                                                (col('multiplo_emprego_regional') >= reg_multiplo_emprego_regional_intensivo) &
                                                                (col('qtd_empresas_meso') >= reg_quantidade_empresas), 'Sim')
                                                          .otherwise(lit('Não'))) \
                              .withColumn('Polo_estadual', when((col('setor_intensivo') == 'Não') &
                                                                (col('multiplo_emprego_regional') >= est_multiplo_emprego_regional_nao_intensivo) &
                                                                ((col('qtd_empresas_micro_media') >= est_min_emp_micro_media) |
                                                                (col('qtd_empresas_grande') >= est_min_emp_grande)) &
                                                                (col('QL_regional') >= est_ql_estadual_emp) &
                                                                (col('Polo_regional') == 'Sim'), 'Sim')
                                                          .when((col('setor_intensivo') == 'Sim') &
                                                                (col('multiplo_emprego_regional') >= est_multiplo_emprego_regional_intensivo) &
                                                                ((col('qtd_empresas_micro_media') >= est_min_emp_micro_media) |
                                                                (col('qtd_empresas_grande') >= est_min_emp_grande)) &
                                                                (col('QL_regional') >= est_ql_estadual_emp) &
                                                                (col('Polo_regional') == 'Sim'), 'Sim')
                                                          .otherwise(lit('Não'))) \
                              .withColumn('Polo_nacional', when((col('setor_intensivo') == 'Não') &
                                                                (col('participacao_emprego_br') >= 0.015) &
                                                                (col('QL_nacional') >= nac_ql_nac_emp) &
                                                                (col('Polo_regional') == 'Sim'), 'Sim')
                                                          .when((col('setor_intensivo') == 'Sim') &
                                                                (col('participacao_emprego_br') >= 0.02) &
                                                                (col('QL_nacional') >= nac_ql_nac_emp) &
                                                                (col('Polo_regional') == 'Sim'), 'Sim')
                                                          .otherwise(lit('Não'))) \
                              .withColumn('Polo_internacional', when((col('insercao_internacional') >= perc_insercao_int) &
                                                                     (col('Polo_nacional') == 'Sim'), 'Sim').otherwise('Não'))

# COMMAND ----------

# 1.33 Filtro para pegar apenas linhas com empregos maior que 0
df_polo = df_polo.filter(col('qtd_emprego_meso_setor') > 0)

# COMMAND ----------

# 1.34 Filtro colunas relevantes
df_novo = df_polo.select("nm_uf",
                         "nm_mesorregiao",
                         "cd_div",
                         "nm_grupo",
                         "setor_intensivo",
                         "qtd_emprego_meso_setor",
                         "qtd_emprego_meso",
                         "participacao_emprego_meso",
                         "qtd_emprego_setor_estado",
                         "multiplo_emprego_regional",
                         "qtd_empresas_meso",
                         "Polo_regional",
                         "qtd_emprego_estado",
                         "participacao_emprego_estado",
                         "GINI",
                         "QL_regional",
                         "qtd_empresas_micro_media",
                         "qtd_empresas_grande",
                         "qtd_empresas_estado",
                         "participacao_empresa_estado",
                         "Polo_estadual",
                         "qtd_emprego_setor_brasil",
                         "multiplo_emprego_nacional",
                         "qtd_emprego_brasil",
                         "participacao_emprego_br",
                         "GINI_nac",
                         "QL_nacional",
                         "qtd_empresas_brasil",
                         "participacao_empresa_nacional",
                         "Polo_nacional",
                         "valor_exp",
                         "vbpi_meso",
                         "insercao_internacional",
                         "exp_setor_uf/exp_total_uf",
                         "part_uf_setor_exportacao",
                         "Polo_internacional")

# COMMAND ----------

# 1.35 Alteração nome das colunas para melhor análise
colunas = ["UF",
           "Mesorregiao",
           "cd_div",
           "CNAE grupo",
           "Intensivo em mao de obra",
           "Quantidade de empregos do setor na mesorregião (1)",
           "Quantidade de empregos totais na mesorregião (2)",
           "Participacao de empregos do setor na mesorregião (PSM = 1/2)",
           "Quantidade de empregos do setor na UF (3)",
           "Múltiplo de emprego regional (MER = PSM*1/3)",
           "Quantidade total de empresas na mesorregião (4)",
           "Polo Regional",
           "Quantidade de empregos totais na UF (5)",
           "Participacao de empregos do setor na UF (PSE = 3/5)",
           "Gini locacional estadual (GLE)",
           "Quoeficiente locacional estadual (QLE = (1-GLE)*PSM/PSE)",
           "Quantidade de micro, pequenas e medias empresas na mesorregião",
           "Quantidade de grandes empresas na mesorregião",
           "Quantidade de empresas totais na UF (6)",
           "Participacao de empresas na UF (PEE = 4/6)",
           "Polo Estadual",
           "Quantidade de empregos do setor no Brasil (7)",
           "Múltiplo de emprego nacional (MEN = PSM*1/7)",
           "Quantidade de empregos totais no Brasil (8)",
           "Participaçcao de empregos do setor no Brasil (PSP = 7/8)",
           "Gini locacional nacional (GLN)",
           "Quoeficiente locacional nacional (QLE = (1-GLN)*PSM/PSP)",
           "Quantidade de empresas totais no Brasil (9)",
           "Participacao de empresas no pais (PEP = 4/9)",
           "Polo Nacional",
           "Valor exportado (10)",
           "Valor bruto da producao industrial (VBPI)",
           "Índice de insercao internacional (10/VBPI)",
           "Participação do setor nas exportações da UF",
           "Participação da UF nas exportações nacionais do setor",
           "Polo Internacional"] 
new_df = df_novo.toDF(*colunas)

# COMMAND ----------

# 1.36 Garantir que todos os polos nacionais também sejam polos estaduais
df_novo = df_novo.withColumn("Polo_estadual", when(df_novo["Polo_nacional"] == "Sim", "Sim").otherwise(df_novo["Polo_estadual"]))

# COMMAND ----------

#1.37 Salvando a tabela
path = '/Volumes/oni_rede/obs_fiesc/observatorio_fiesc_projetos/polos_industriais/polos_sul_sudeste'
df_novo.write.mode("overwrite").option('overwriteSchema', 'true').format("delta").save(path)

# COMMAND ----------

#2 Script Outros
#Em relação ao script Sul/Sudeste o script para os outros estados muda apenas o que segue:

#2.1 Listas e Dicionários
# código estados outros
lista_outros = ['12','27','16','13','29', '32', '23','52','21','51','50','15','25','26','22','24','11','14','28','17']

#2.2 Ligação CNAE - SH4 por divisão CNAE
url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQxiaqjbw1ELj6ZFZVlT3xwXvYCQ4gEMaXcEyzsvqq70leDG9f3X9KiDNDQkRYrEg/pub?output=csv'
dim_sh4 = spark.createDataFrame(pd.read_csv(url, encoding="UTF-8", sep=","))

dim_sh4 = dim_sh4.withColumn("CD_SH4", lpad(dim_sh4["SH4"], 4, '0').cast("string")) \
                .withColumn("cd_grupo", lpad(dim_sh4["cnae_grupo"], 3, '0'))

dim_sh4 = dim_sh4.withColumn("cd_div", lpad(dim_sh4["cd_grupo"], 2, '0')) \
                .select("CD_SH4","cd_div")

# COMMAND ----------

# 2021
url = 'https://apisidra.ibge.gov.br/values/t/1849/n3/all/v/810/p/2021/c12762/116881,116884,116887,116897,116905,116911,116952,116960,116965,116985,116994,117007,117015,117029,117039,117048,117082,117089,117099,117116,117136,117159,117179,117196,117229,117245,117261,117267,117283'

# COMMAND ----------

