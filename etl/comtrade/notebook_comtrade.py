# Databricks notebook source
# MAGIC %md
# MAGIC # Comércio Internacional - Comtrade
# MAGIC Este notebook tem como objetivo estruturar os dados do Comtrade/UN para subsidiar um painel de análise de tendências, avaliação da integração internacional de países e blocos econômicos, exame de diferentes segmentos produtivos e identificação de oportunidades estratégicas de mercado para a Gerência de Comércio e Integração Internacional.
# MAGIC
# MAGIC **Cientista de dados:** Beatriz S. Bonato<br>
# MAGIC **Card no Trello:** https://trello.com/c/7lhxJO7U/980-com%C3%A9rcio-exterior-dashboard-comtrade<br>
# MAGIC **GCII:** Gabriella P. Santos<br>
# MAGIC **Produto em dev (A2M):** https://cni.reportload.com/cca959ff-1eef-4ea5-8daf-9ebda1a54519/reports/view

# COMMAND ----------

# Define livrarias de pyspark
import pyspark.sql.functions as f

# Define caminho do data lake
caminho_data_lake = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

# DBTITLE 1,Carrega os dados
commodity_anual = spark.read.parquet(caminho_data_lake + '/trs/oni/un/comtrade/comer_wld/commodity_annual')
tabela_continentes = spark.read.csv(caminho_data_lake + '/uds/oni/observatorio_nacional/comercio_comtrade/tab_auxiliar/tabela_continentes.csv', header=True, inferSchema=True, sep=';')

# COMMAND ----------

# DBTITLE 1,Correção de nº de dígitos
commodity_anual = commodity_anual.withColumn("CD_PARTNER", f.lpad(f.col("CD_PARTNER"), 3, '0')) \
                                 .withColumn("CD_REPORTER", f.lpad(f.col("CD_REPORTER"), 3, '0'))

# COMMAND ----------

# DBTITLE 1,Filtros
commodity_anual1 = commodity_anual.filter(
    (f.length(commodity_anual.CD_CMD) == 6) & #tem que ser SH6
    (commodity_anual["CD_CUSTOMS"] == "C00") & #procedimento aduaneiro
    (commodity_anual["CD_MOT"] == 0) & #transporte
    ((commodity_anual["CD_FLOW"] == "X") | (commodity_anual["CD_FLOW"] == "M")) & #fluxo
    (commodity_anual["CD_PARTNER"] != 0) & #primeiro país parceiro
    (commodity_anual["CD_PARTNER2"] == 0) & #segundo país parceiro
    (commodity_anual["CD_TYPE"] == "C") & #tipo de produto (bens ou serviços)
    (commodity_anual["NR_YEAR"] >= 2014)) 

# COMMAND ----------

commodity_anual2 = commodity_anual1.withColumn("DS_FLOW", f.when(f.col("CD_FLOW") == "X", "Exportação").when(f.col("CD_FLOW") == "M", "Importação").otherwise("Desconhecido"))

# COMMAND ----------

commodity_anual3 = commodity_anual2.join(tabela_continentes, commodity_anual2["CD_REPORTER"] == tabela_continentes["cd_pais"], "left").select(
    commodity_anual2["*"], f.when(f.col("nm_pais_port").isNull(), "outro").otherwise(f.col("nm_pais_port")).alias("DS_REPORTER"))

# COMMAND ----------

commodity_anual4 = commodity_anual3.join(tabela_continentes, commodity_anual3["CD_PARTNER"] == tabela_continentes["cd_pais"], "left").select(
    commodity_anual3["*"], f.when(f.col("nm_pais_port").isNull(), "outro").otherwise(f.col("nm_pais_port")).alias("DS_PARTNER"))

# COMMAND ----------

commodity_anual5 = commodity_anual4.select("NR_YEAR", "DS_FLOW", "DS_PARTNER", "DS_REPORTER", "CD_CMD", "VL_PRIMARY", "VL_WGT_NET")

# COMMAND ----------

commodity_anual6 = commodity_anual5.select(
    f.when(f.col("DS_REPORTER") == 'Outra Ásia, não especificada em outro lugar', 'Taiwan')
    .otherwise(f.col("DS_REPORTER")).alias("DS_REPORTER"),
    f.when(f.col("DS_PARTNER") == 'Outra Ásia, não especificada em outro lugar', 'Taiwan')
    .otherwise(f.col("DS_PARTNER")).alias("DS_PARTNER"),
    "NR_YEAR", "DS_FLOW", "CD_CMD", "VL_PRIMARY", "VL_WGT_NET"
).where((f.col("DS_REPORTER") != 'ASEAN') & (f.col("DS_REPORTER") != 'União Europeia'))


# COMMAND ----------

commodity_anual6.write.partitionBy("NR_YEAR").parquet(caminho_data_lake + '/uds/oni/observatorio_nacional/comercio_comtrade/painel_comercio', mode='overwrite', compression='snappy')