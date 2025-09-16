# Databricks notebook source
# MAGIC %md
# MAGIC # Painel de Cotas PcD
# MAGIC - Tabela Agregada por CNPJ raiz
# MAGIC
# MAGIC O seguinte notebook traz os dados de total de vínculos e PcDs (exceto para aprendizes) para o painel de cotas PcD, agregando os valores pelo CNPJ raiz (8 dígitos) e utilizando a base da RFB para obtenção da Razão Social, CNAE, Município e Situação cadastral da Matriz. 
# MAGIC
# MAGIC Data: 13/8/24
# MAGIC Autor: Maria Cecília Ramalho
# MAGIC
# MAGIC Edit - Dez 2024: Anaely Machado
# MAGIC
# MAGIC Card referência: https://trello.com/c/b3qLN9wB/850-painel-de-cotas-pcd

# COMMAND ----------

# MAGIC %md
# MAGIC # Leitura e preparação das bases de dados

# COMMAND ----------

# DBTITLE 1,Bibliotecas
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import when, ceil

adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'


# COMMAND ----------

# DBTITLE 1,Paths e carga das bases
# Importa dados da base do Data Lake
# Obs: sempre vai ser usada a base da receita federal mais recente e com o mes de dezembro
# no caso, foi usada 2024/12

source_rfb = '{adl_path}/trs/oni/rfb_cnpj/cadastro_empresa_f/ANO_ARQUIVO=2024/MES_ARQUIVO=12'.format(adl_path=adls_uri)
source_rfb_estbl = '{adl_path}/trs/oni/rfb_cnpj/cadastro_estbl_f/ANO_ARQUIVO=2024/MES_ARQUIVO=12'.format(adl_path=adls_uri)

source_rfb_muni = '{adl_path}/uds/oni/observatorio_nacional/BACKUP/cota_empresa_pcd'.format(adl_path=adls_uri)
source_ibge_muni = '{adl_path}/biz/oni/bases_referencia/municipios'.format(adl_path=adls_uri)
source_cnae_div = '{adl_path}/biz/oni/bases_referencia/cnae/cnae_20/cnae_divisao'.format(adl_path=adls_uri)

df_rfb_empresa_ori = spark.read.parquet(source_rfb)
df_rfb_estbl_ori = spark.read.parquet(source_rfb_estbl)
df_munic_0 = spark.read.parquet(source_rfb_muni)
df_ibge_munic = spark.read.parquet(source_ibge_muni)
df_cnae_div = spark.read.parquet(source_cnae_div)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality - retira os registros com inconsistência no CNPJ (erro na fonte dos dados originais)

# COMMAND ----------

# DBTITLE 1,Prepara base de municipios
# DATA QUALITY - retira os registros com inconsistência no CNPJ (erro na fonte dos dados originais)

df_munic = (df_munic_0.withColumn("cd_uf", f.substring("cd_mun_ibge", 1, 2))
            .withColumn('nm_mun_ibge', f.when(f.col('cd_mun_rfb') == '9997', 'Coronel Sapucaia').otherwise(f.col('nm_mun_ibge')))
            .withColumn('CD_MUN_RFB', f.lpad(f.trim(df_munic_0['CD_MUN_RFB']), 4, '0'))
           )

## Preparação dos dados RFB
# PREPARA A BASE DE EMPRESAS DA RFB 
df_rfb_empresa = (df_rfb_empresa_ori
                  .select("CD_CNPJ_BASICO", "NM_RAZAO_SOCIAL",'CD_NATUREZA_JURIDICA')
                  .withColumnRenamed('CD_CNPJ_BASICO','CD_CNPJ_BASICO_RFB_EMP')
                  .withColumnRenamed('NM_RAZAO_SOCIAL','NM_RAZAO_SOCIAL_RFB_EMP')
                  .withColumnRenamed('CD_NATUREZA_JURIDICA','CD_NATUREZA_JURIDICA_RFB_EMP')
                  )

# PREPARA A BASE DE ESTABELECIMENTOS DA RFB
df_rfb_estb_aux1 = (df_rfb_estbl_ori
                     .withColumn("CD_CNAE20_DIV_RFB_EST", f.substring("CD_CNAE20_SUBCLASSE_PRINCIPAL", 1, 2))
                     .withColumn("FL_SIT_ATIVA_RFB_EST", f.when(f.col('DS_SIT_CADASTRAL') == "Ativa",f.lit(1)).otherwise(f.lit(0)))
                     .withColumn("FL_SETOR_ADMIN_RFB_EST", f.when(f.col('CD_CNAE20_DIV_RFB_EST') == "84",f.lit(1)).otherwise(f.lit(0)))
                     .withColumnRenamed('CD_CNPJ','CD_CNPJ_RFB_EST')
                     .withColumnRenamed('CD_CNPJ_BASICO','CD_CNPJ_BASICO_RFB')
                     .withColumnRenamed('CD_MATRIZ_FILIAL','CD_MATRIZ_FILIAL_RFB_EST')
                     .withColumnRenamed('CD_SIT_CADASTRAL','CD_SIT_CADASTRAL_RFB_EST')
                     .withColumnRenamed('CD_CEP','CD_CEP_RFB_EST')
                     .withColumnRenamed('SG_UF','SG_UF_RFB_EST')
                     .withColumnRenamed('CD_MUN','CD_MUN_RFB_EST')
                     .select('CD_CNPJ_RFB_EST','CD_CNPJ_BASICO_RFB','CD_MATRIZ_FILIAL_RFB_EST', 'CD_SIT_CADASTRAL_RFB_EST','CD_CEP_RFB_EST','SG_UF_RFB_EST','CD_MUN_RFB_EST','CD_CNAE20_DIV_RFB_EST', 'FL_SIT_ATIVA_RFB_EST', 'FL_SETOR_ADMIN_RFB_EST')
                     )

# COMMAND ----------

# MAGIC %md
# MAGIC # INICIO DA ROTINA DO CALCULO - LEITURA DA RAIS PARA O ANO SELECIONADO 

# COMMAND ----------

# Precisa DEFINIR UMA FUNÇÃO para executar para 2023 e 2024

ano = 2024

# Define os caminhos dos arquivos parquet

source_rais_vinculo = '{adl_path}/trs/oni/mte/rais/identificada/rais_vinculo/ANO={ano}'.format(adl_path=adls_uri, ano=ano)
source_rais_estabel = '{adl_path}/trs/oni/mte/rais/identificada/rais_estabelecimento/NR_ANO={ano}'.format(adl_path=adls_uri, ano=ano)
df_rais_vinculo_0 = spark.read.parquet(source_rais_vinculo)
df_rais_estabelecimento = spark.read.parquet(source_rais_estabel)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratamento RAIS vínculo
# MAGIC - Data quality
# MAGIC - Identificação do estoque de referência para o cálculo da cota
# MAGIC - Filtra variáveis relevantes
# MAGIC - Seleciona apenas vínculos ativos, empresas ativas, etc
# MAGIC - agregar por EMPRESA (CNPJ raiz) para calcular as faixas dos % de cotas

# COMMAND ----------

# DBTITLE 1,Cria as variáveis para cálculo dos  indicadores
##### ATENÇÃO: CORRIGIR NA RAIS VINCULOS PROBLEMAS COM CNPJ_BASICO DIFERENTE DO CNPJ14

df_vinculos_trs = (df_rais_vinculo_0.withColumn('ID_CNPJ_RAIZ_CORR', f.substring("ID_CNPJ_CEI", 1, 8))
                                    .withColumn('conta_cnpj_raiz', f.length(f.trim('ID_CNPJ_RAIZ')))
                                    .withColumn('conta_cnpj_14', f.length(f.trim('ID_CNPJ_CEI')))
                                    .filter((f.col('conta_cnpj_14') == 14) & (f.col('conta_cnpj_raiz') == 8) & (f.col('ID_CNPJ_RAIZ') == f.substring("ID_CNPJ_CEI", 1, 8)))
                    )

# SELECIONA OS FILTROS DE NEGÓCIO PARA PCD

variaveis_vinc = ['ID_CNPJ_RAIZ','ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_VINCULO_ATIVO_3112',
                  'FL_IND_PORTADOR_DEFIC','ID_PIS','ID_CPF', 'CD_TIPO_VINCULO',
                  'FL_IND_TRAB_INTERMITENTE','CD_CNAE20_DIVISAO','CD_TIPO_DEFIC',
                  'DT_DIA_MES_ANO_DATA_ADMISSAO','FL_IND_CEI_VINCULADO','CD_TIPO_ESTAB','CD_UF','CD_MUNICIPIO_TRAB','CD_MUNICIPIO']

filtro_radar = [ '10', '15', '20', '25', '50', '60', '65', '70', '75', '90']

df_vinculos = (df_vinculos_trs
              .select(*variaveis_vinc)
              .filter((f.col('CD_TIPO_ESTAB') == f.lit('01')) &
                       (f.col('FL_VINCULO_ATIVO_3112') == 1) &
                       (f.col('FL_IND_TRAB_INTERMITENTE') != 1)
                     )
              .withColumn('DT_ADMISSAO',f.concat_ws('-',
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2),
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2)))
              .withColumn('DT_ADMISSAO', f.to_date(f.col('DT_ADMISSAO'),"yyyy-MM-dd"))
              .orderBy("ID_CNPJ_RAIZ", "ID_PIS","DT_ADMISSAO")
              .dropDuplicates(["ID_CNPJ_RAIZ", "ID_PIS"])
              .withColumn('TOTAL_VINC', f.when(f.col('CD_TIPO_VINCULO') != "55",f.lit(1)).otherwise(f.lit(0)))  
              .withColumn('TOTAL_VINC_RADAR', f.when(f.col('CD_TIPO_VINCULO').isin(filtro_radar),f.lit(1))
                                .otherwise(f.lit(0)))
              .withColumn('TOTAL_VINC_PCD', f.when((f.col('TOTAL_VINC') == 1) & 
                                                  (f.col('FL_IND_PORTADOR_DEFIC') == 1),f.lit(1))
                                .otherwise(f.lit(0))) 
              .withColumn('TOTAL_VINC_RADAR_PCD', f.when((f.col('TOTAL_VINC_RADAR') == 1) & (f.col('FL_IND_PORTADOR_DEFIC') == 1),f.lit(1)).otherwise(f.lit(0)))   
              .withColumn('TOTAL_APRENDIZ', f.when(f.col('CD_TIPO_VINCULO') == 55,f.lit(1)).otherwise(f.lit(0)))
              .withColumn('TOTAL_APRENDIZ_PCD', f.when((f.col('CD_TIPO_VINCULO') == 55) & (f.col('FL_IND_PORTADOR_DEFIC') == 1),f.lit(1)).otherwise(f.lit(0)))
              .withColumn("FL_SETOR_ADMIN_RAIS", f.when(f.col('CD_CNAE20_DIVISAO') == "84",f.lit(1)).otherwise(f.lit(0)))  
              .withColumn('ANO', f.lit(ano))                   
              )

## cria o arquivo com divisão CNAE da RAIS para cruzar com os dados da RFB
# a partir da base df_vinculos_trs, agrupa por CNPJ, divisão CNAE e flag de setor administrativo criando df_divisao_rais, que não pode ter CNPJ repetido, ou seja, com CNPJ com mais de uma divisão CNAE.

# cria a base de CNPJ's da base da RAIS de trabalho (df_vinculos)
df_divisao_rais = df_vinculos.withColumnRenamed('ID_CNPJ_CEI','ID_CNPJ_CEI_AUX').groupBy('ID_CNPJ_CEI_AUX','CD_CNAE20_DIVISAO','FL_SETOR_ADMIN_RAIS').count()

# faz o JOIN de RFB-estabelecimento com df_divisao_rais para pegar o CNAE do estabelecimento
df_rfb_estb_aux2 = (df_rfb_estb_aux1.join(df_divisao_rais,
    df_rfb_estb_aux1.CD_CNPJ_RFB_EST == df_divisao_rais.ID_CNPJ_CEI_AUX,'right')
    .drop('count'))

# faz o JOIN de RFB-estabelecimento com RFB-empresa pelo CNPJ matriz para buscar os dados da MATRIZ
df_rfb_estb_aux3 = (df_rfb_estb_aux2.join(df_rfb_empresa,
    df_rfb_estb_aux2.CD_CNPJ_BASICO_RFB == df_rfb_empresa.CD_CNPJ_BASICO_RFB_EMP,'left'))

# faz o JOIN de RFB-estabelecimento com MUNICIPIO
df_rfb_estb = (df_rfb_estb_aux3
                .join(df_munic, df_rfb_estb_aux3.CD_MUN_RFB_EST == df_munic.CD_MUN_RFB,'left')
                .withColumnRenamed('cd_uf','CD_UF')
                .withColumnRenamed('sg_uf','SG_UF_ESTB')
                .withColumnRenamed('cd_mun_ibge','CD_MUNICIPIO')
                .withColumnRenamed('nm_mun_ibge','NM_MUNIC_ESTB')
                .drop('CD_MUN_RFB','NM_MUN_RFB')
                ) 

##
df_rfb_estb_0 = df_rfb_estb.orderBy(f.col('CD_CNPJ_BASICO_RFB'),f.col('CD_MATRIZ_FILIAL_RFB_EST'),f.col('ID_CNPJ_CEI_AUX'))
df_rfb_estb_0 = df_rfb_estb_0.dropDuplicates(['CD_CNPJ_BASICO_RFB'])  

# seleciona os estabelecimentos que são matrizes
df_rfb_matriz = (df_rfb_estb_0
                 .withColumnRenamed('CD_MATRIZ_FILIAL_RFB_EST','CD_MATRIZ_FILIAL_RFB_MTZ')
                 .withColumnRenamed('CD_CNPJ_RFB_EST','CD_CNPJ_RFB_MTZ')
                 .withColumnRenamed('CD_CNAE20_DIV_RFB_EST','CD_CNAE20_DIV_RFB_MTZ')
                 .withColumnRenamed('FL_SIT_ATIVA_RFB_EST','FL_SIT_ATIVA_RFB_MTZ')
                 .withColumnRenamed('CD_SIT_CADASTRAL_RFB_EST','CD_SIT_CADASTRAL_RFB_MTZ')
                 .withColumnRenamed('CD_MUN_RFB_EST','CD_MUN_RFB_MTZ')
                 .withColumnRenamed('CD_MUNICIPIO','CD_MUN_IBGE_MTZ')
                 .withColumnRenamed('NM_MUNIC_ESTB','NM_MUN_IBGE_MTZ')
                 .withColumnRenamed('SG_UF_RFB_EST','SG_UF_RFB_MTZ')
                 .withColumnRenamed('SG_UF_ESTB','SG_UF_IBGE_MTZ')
                 .withColumnRenamed('CD_UF','CD_UF_RFB_MTZ')
                 .withColumnRenamed('CD_CEP_RFB_EST','CD_CEP_RFB_MTZ')
                 .withColumnRenamed('FL_SETOR_ADMIN_RFB_EST','FL_SETOR_ADMIN_RFB_MTZ')
                 )


# COMMAND ----------

# MAGIC %md
# MAGIC ## PARTE 1 - Vínculos PCD por matriz

# COMMAND ----------

# SOMA OS TOTAIS POR EMPRESA (CNPJ RAIZ)
# 8 observações são de CNPJs duplicados, que aparecem em muninipios diferentes.  
# A ideia é excliuir uma das linhas, ficando apenas com a que tem maior número de vínculos

#  .groupBy('ID_CNPJ_RAIZ','CD_MUNICIPIO','CD_UF','CD_CNAE20_DIVISAO')  <==== este agg foi feito pela ANAELY
#            'CD_CNPJ_BASICO_RFB','SG_UF_RFB_EST','CD_MUN_RFB_EST'

df_vinc_matriz = (df_vinculos
            .groupBy('ID_CNPJ_RAIZ','ANO')
            .agg(f.count('ID_CPF').alias('TOTAL_TRAB'),  
                 f.sum('TOTAL_VINC').alias('TOTAL_VINC'),
                 f.sum('TOTAL_VINC_PCD').alias('TOTAL_VINC_PCD'),
                 f.sum('TOTAL_VINC_RADAR').alias('TOTAL_VINC_RADAR'),
                 f.sum('TOTAL_VINC_RADAR_PCD').alias('TOTAL_VINC_RADAR_PCD'),
                 f.sum('TOTAL_APRENDIZ').alias('TOTAL_APRENDIZ'),
                 f.sum('TOTAL_APRENDIZ_PCD').alias('TOTAL_APRENDIZ_PCD')
                 ))

# CRIA AS FAIXAS DE TRABALHADORES PARA ATRIBUIR O PERCENTUAL DE PCD DESEJADO
df_vinc_matriz_faixa = (df_vinc_matriz
            .withColumn("PERC_DESEJADO", when(f.col("TOTAL_VINC") < 100, 0)
                .when((f.col("TOTAL_VINC") >= 100) & (f.col("TOTAL_VINC") <= 200), 0.02)
                .when((f.col("TOTAL_VINC") >= 201) & (f.col("TOTAL_VINC") <= 500), 0.03)
                .when((f.col("TOTAL_VINC") >= 501) & (f.col("TOTAL_VINC") <= 1000), 0.04)
                .otherwise(0.05))
            .withColumn("TAMANHO_AGREG",when(f.col("PERC_DESEJADO") == 0, "Até 99 empregados")
                .when(f.col("PERC_DESEJADO") == 0.02, "De 100 a 200 empregados")
                .when(f.col("PERC_DESEJADO") == 0.03, "De 201 a 500 empregados")
                .when(f.col("PERC_DESEJADO") == 0.04, "De 501 a 1.000 empregados")
                .otherwise("Mais de 1.000 empregados"))
            .withColumn("PERC_DESEJADO_RADAR", when(f.col("TOTAL_VINC_RADAR") < 100, 0)
                .when((f.col("TOTAL_VINC_RADAR") >= 100) & (f.col("TOTAL_VINC_RADAR") <= 200), 0.02)
                .when((f.col("TOTAL_VINC_RADAR") >= 201) & (f.col("TOTAL_VINC_RADAR") <= 500), 0.03)
                .when((f.col("TOTAL_VINC_RADAR") >= 501) & (f.col("TOTAL_VINC_RADAR") <= 1000), 0.04)
                .otherwise(0.05))
            .withColumn("TAMANHO_AGREG_RADAR",when(f.col("PERC_DESEJADO_RADAR") == 0, "Até 99 empregados")
                .when(f.col("PERC_DESEJADO_RADAR") == 0.02, "De 100 a 200 empregados")
                .when(f.col("PERC_DESEJADO_RADAR") == 0.03, "De 201 a 500 empregados")
                .when(f.col("PERC_DESEJADO_RADAR") == 0.04, "De 501 a 1.000 empregados")
                .otherwise("Mais de 1.000 empregados"))
            ) 

# CALCULA O TOTAL DE PCD (TOTAL DEMANDA), FLAG DE EMPRESAS QUE CUMPRIRAM A COTA DE PCD E DÉFICIT DE PCD PARA CUMPRIR A COTA
df_vinc_matriz_demanda = (df_vinc_matriz_faixa
           .withColumn("TOTAL_DEMANDA_RADAR", f.ceil(f.col("TOTAL_VINC_RADAR") * f.col("PERC_DESEJADO_RADAR")))
           .withColumn("TOTAL_DEMANDA", f.ceil(f.col("TOTAL_VINC") * f.col("PERC_DESEJADO")))
           .withColumn("FL_EMPRESA_COTA", f.when(f.col('TOTAL_VINC_PCD') >= f.col('TOTAL_DEMANDA'),f.lit(1)).otherwise(f.lit(0)))
           .withColumn("FL_EMPRESA_RADAR_COTA", f.when(f.col('TOTAL_VINC_RADAR_PCD') >= f.col('TOTAL_DEMANDA_RADAR'),f.lit(1)).otherwise(f.lit(0)))
           .withColumn("SALDO_COTA", (f.col('TOTAL_DEMANDA') - (f.col('TOTAL_VINC_PCD'))))
           .withColumn("SALDO_RADAR_COTA", (f.col('TOTAL_DEMANDA_RADAR') - (f.col('TOTAL_VINC_RADAR_PCD'))))
           .withColumn("DEFICIT_COTA", f.when(f.col('FL_EMPRESA_COTA') == 0, (f.col('TOTAL_DEMANDA') - (f.col('TOTAL_VINC_PCD')))).otherwise(f.lit(0)))
           .withColumn("DEFICIT_RADAR_COTA", f.when(f.col('FL_EMPRESA_RADAR_COTA') == 0, (f.col('TOTAL_DEMANDA_RADAR') - (f.col('TOTAL_VINC_RADAR_PCD')))).otherwise(f.lit(0)))
           )

# junta base de PCD com a RFB e salva tabela por empresa
# # Realizar o join entre df_vinc_matriz_demanda e df_rfb_matriz
# df_vinc_matriz_demanda.count()

df_demanda_empresa = (df_vinc_matriz_demanda
                    .join(df_rfb_matriz,df_vinc_matriz_demanda.ID_CNPJ_RAIZ == df_rfb_matriz.CD_CNPJ_BASICO_RFB,"left")
                     )            
            

# Cria o DataFrame para o ano e armazena no dicionário
#dataframes_por_ano = {}
#dataframes_por_ano[f'df_demanda_empresa_{ano}'] = df_demanda_empresa

# COMMAND ----------

# MAGIC %md
# MAGIC ## PARTE 2 - Vínculos PCD por estabelecimento
# MAGIC - agregar por ESTABELECIMENTO (CNPJ completo) para calcular as distribuições de PCDs nas diferentes dimensões
# MAGIC

# COMMAND ----------

### Calcula o total de vínculos por estabelecimento

df_vinc_estab = (df_vinculos
            .groupBy('ID_CNPJ_CEI','ID_CNPJ_RAIZ','ANO')
            .agg(f.count('ID_CPF').alias('TOTAL_TRAB_ESTB'),  
                 f.sum('TOTAL_VINC').alias('TOTAL_VINC_ESTB'),
                 f.sum('TOTAL_VINC_PCD').alias('TOTAL_VINC_ESTB_PCD'),
                 f.sum('TOTAL_VINC_RADAR').alias('TOTAL_VINC_RADAR_ESTB'),
                 f.sum('TOTAL_VINC_RADAR_PCD').alias('TOTAL_VINC_RADAR_ESTB_PCD'),
                 f.sum('TOTAL_APRENDIZ').alias('TOTAL_APRENDIZ_ESTB'),
                 f.sum('TOTAL_APRENDIZ_PCD').alias('TOTAL_APRENDIZ_ESTB_PCD')
                 )
                )
                    
### Juntar base de estabelecimentos com base de empresas para pegar o total de vinculos 
# prepara o arquivo de empresas para fazer join 
df_demanda_empresa_aux = (df_demanda_empresa
                         .select(f.col('ID_CNPJ_RAIZ').alias('ID_CNPJ_RAIZ_aux'),
                                f.col('CD_CNAE20_DIV_RFB_MTZ').alias("CD_CNAE20_DIV_MATRIZ"),
                                f.col('FL_SIT_ATIVA_RFB_MTZ').alias('FL_SIT_ATIVA_MATRIZ'),
                                f.col('SG_UF_RFB_MTZ').alias('SG_UF_RFB_MATRIZ'),
                                f.col('CD_UF_RFB_MTZ').alias('CD_UF_MATRIZ'),
                                f.col('CD_MUN_IBGE_MTZ').alias('CD_MUN_MATRIZ'),
                                f.col('NM_MUN_IBGE_MTZ').alias('NM_MUN_MATRIZ'),
                                f.col('SG_UF_IBGE_MTZ').alias('SG_UF_IBGE_MATRIZ'),
                                f.col('TOTAL_VINC').alias('TOTAL_VINC_MATRIZ'),
                                f.col('TOTAL_VINC_PCD').alias('TOTAL_VINC_PCD_MATRIZ'),
                                f.col('TOTAL_VINC_RADAR').alias('TOTAL_VINC_RADAR_MATRIZ'),
                                f.col('TOTAL_VINC_RADAR_PCD').alias('TOTAL_VINC_RADAR_PCD_MATRIZ'),
                                f.col('TAMANHO_AGREG').alias('TAMANHO_AGREG_MATRIZ'),
                                f.col('TAMANHO_AGREG_RADAR').alias('TAMANHO_AGREG_RADAR_MATRIZ'),
                                )
                         )
df_vinc_estab_aux = (df_vinc_estab
            .join(df_demanda_empresa_aux,(df_vinc_estab.ID_CNPJ_RAIZ == df_demanda_empresa_aux.ID_CNPJ_RAIZ_aux),"left")
            .drop('ID_CNPJ_RAIZ_aux'))

df_demanda_estab = (df_vinc_estab_aux
                    .join(df_rfb_estb,(df_vinc_estab_aux.ID_CNPJ_CEI == df_rfb_estb.ID_CNPJ_CEI_AUX),"left")
                    .drop('ID_CNPJ_CEI_AUX')
                   )

                     

# COMMAND ----------

# ATENÇÃO: APÓS RODAR PARA CADA ANO É PRECISO EXECUTAR O COMANDO ABAIXO ATUALIZANDO O ANO
# para 2023 e 2024

df_empresa2024 = df_demanda_empresa
df_estab2024 = df_demanda_estab


# COMMAND ----------

# MAGIC %md 
# MAGIC ### FIM da rotina

# COMMAND ----------

# MAGIC %md
# MAGIC # EXECUTAR daqui pra baixo só depois de rodar para os anos de interesse

# COMMAND ----------

# MAGIC %md
# MAGIC ### junta as tabelas de empresa e de estabelecimento para 2023 e 2024
# MAGIC ### salva essas tabelas na UDS do Datalake

# COMMAND ----------

# DBTITLE 1,ATENÇÃO:  só gravar a tabela se estiver OK
# unir as tabelas de empresas para os anos de 2023 e 2024 e salvar na uds
# Para acessar depois:

#df_empresa = df_empresa2023.union(df_empresa2024)
#df_empresa.count()
df_empresa.write.parquet(adls_uri + '/uds/oni/observatorio_nacional/cota_empresa_pcd/tabela_agreg_empresa', mode='overwrite')



# COMMAND ----------

# unir as tabelas de estabelecimentos para os anos de 2023 e 2024 e salvar na uds
# Para acessar depois:

#df_estab = df_estab2023.union(df_estab2024)
#df_estab.count()
df_estab.write.parquet(adls_uri + '/uds/oni/observatorio_nacional/cota_empresa_pcd/tabela_agreg_estab', mode='overwrite')




# COMMAND ----------

# MAGIC %md
# MAGIC ## fim do programa

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABELAS PARA VALIDAÇÃO DOS TOTAIS

# COMMAND ----------

#df_demanda_radar = df_demanda_empresa.filter((f.col('TAMANHO_AGREG_RADAR') != 'Até 99 empregados') & (f.col('SG_UF_RFB_MTZ') != 'SP'))
#df_demanda_radar = df_demanda_empresa.filter((f.col('TAMANHO_AGREG_RADAR') != 'Até 99 empregados'))

df_empresa.groupBy('ANO','TAMANHO_AGREG').agg(
        f.count('ID_CNPJ_RAIZ').alias('total de EMPRESAS'),  
        f.sum('TOTAL_VINC').alias("total de vinculos"),
        f.sum('TOTAL_DEMANDA').alias("Reserva de PCD (DEMANDA)"),
        f.sum('TOTAL_VINC_PCD').alias("total de vinculos PCD"),
        f.sum('DEFICIT_COTA').alias("Deficit de PCDs"),
        f.sum('SALDO_COTA').alias("Saldo de PCDs"),
        f.sum('FL_EMPRESA_COTA').alias("Empresas que cumprem a cota de PCD"),
        f.sum('TOTAL_APRENDIZ').alias("total de aprendizes"),
        f.sum('TOTAL_APRENDIZ_PCD').alias("total de aprendizes PCD")
).orderBy('ANO','TAMANHO_AGREG').display()

# COMMAND ----------


df_estab.groupBy('ANO','TAMANHO_AGREG_MATRIZ').agg(
        f.count('ID_CNPJ_RAIZ').alias('total de estabelecimentos'),  
        f.countDistinct('ID_CNPJ_RAIZ').alias('total de EMPRESAS'),  
        f.sum('TOTAL_VINC_ESTB').alias("total de vinculos estabelecimento"),
        f.sum('TOTAL_VINC_ESTB_PCD').alias("total de vinculos matriz"),
        f.sum('TOTAL_VINC_PCD_MATRIZ').alias("Deficit de PCDs matriz"),
        f.sum('TOTAL_APRENDIZ_ESTB').alias("total de aprendizes"),
        f.sum('TOTAL_APRENDIZ_ESTB_PCD').alias("total de aprendizes PCD")
).orderBy('ANO','TAMANHO_AGREG_MATRIZ').display()