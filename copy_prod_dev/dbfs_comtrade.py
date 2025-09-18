# Databricks notebook source
display(dbutils.fs)

# COMMAND ----------

spark.sql("create table if not exists `lab_oni`.`default`.`table_test` USING `parquet` LOCATION 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/data_hub_innovation/pia_empresa_investimento_tab7243/'")

spark.sql("create table if not exists `oni_lab` USING `csv` LOCATION 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/Danilo/cnpj_contrib_direta/CNPJ_COntrib_DIRETA.csv'")

spark.sql("create schema if not exists `oni_lab`.`default`.`cnpj_contrib_direta`")

spark.sql("create table if not exists `oni_lab`.`projecao_pacotes_r`.`projecao_setorial` USING `csv` OPTIONS (header 'true', inferSchema 'true') LOCATION 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/projecao_pacotes_r/projecao_setorial.csv'")

spark.sql("create table if not exists `oni_lab`.`default`.`cnpj_contrib_direta` USING `csv` OPTIONS (header 'true', inferSchema 'true') LOCATION 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/Danilo/cnpj_contrib_direta/CNPJ_COntrib_DIRETA.csv'")

# COMMAND ----------



# COMMAND ----------

# Databricks notebook source
!pip install comtradeapicall


import comtradeapicall
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType

# COMMAND ----------

subscription_key = '295eafe10c3e473cbe11634b34cb3245'

var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net"

# COMMAND ----------

schema = StructType([
    StructField("datasetCode", LongType(), nullable=True),
    StructField("typeCode", StringType(), nullable=True),
    StructField("freqCode", StringType(), nullable=True),
    StructField("period", LongType(), nullable=True),
    StructField("reporterCode", LongType(), nullable=True),
    StructField("reporterISO", StringType(), nullable=True),
    StructField("reporterDesc", StringType(), nullable=True),
    StructField("classificationCode", StringType(), nullable=True),
    StructField("classificationSearchCode", StringType(), nullable=True),
    StructField("isOriginalClassification", BooleanType(), nullable=True),
    StructField("isExtendedFlowCode", BooleanType(), nullable=True),
    StructField("isExtendedPartnerCode", BooleanType(), nullable=True),
    StructField("isExtendedPartner2Code", BooleanType(), nullable=True),
    StructField("isExtendedCmdCode", BooleanType(), nullable=True),
    StructField("isExtendedCustomsCode", BooleanType(), nullable=True),
    StructField("isExtendedMotCode", BooleanType(), nullable=True),
    StructField("totalRecords", LongType(), nullable=True),
    StructField("datasetChecksum", LongType(), nullable=True),
    StructField("firstReleased", StringType(), nullable=True),
    StructField("lastReleased", StringType(), nullable=True)
])


# COMMAND ----------

df_anual = spark.createDataFrame([], schema)
df_mensal = spark.createDataFrame([], schema)



var_sink = var_adls_uri+'/uds/oni/observatorio_nacional/uncomtrade/'

dfC_anual = comtradeapicall.getFinalDataAvailability(subscription_key, typeCode='C', freqCode='M', clCode='H6', period='202401', reporterCode=None)

