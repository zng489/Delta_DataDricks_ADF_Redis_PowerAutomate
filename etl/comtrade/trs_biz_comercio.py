# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


tables = {"schema":"","table":"","trusted_path_1":"/oni/observatorio_nacional/comercio_comtrade/tabela_continentes/",
          "trusted_path_2":"/oni/un/comtrade/comer_wld/commodity_annual/","destination":"/oni/painel_comercio_comtrade/comercio/","databricks":{"notebook":"/biz/oni/painel_comercio_comtrade/trs_biz_comercio"},"prm_path":""}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

# COMMAND ----------

# Configurações iniciais
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import (
    col, substring, lpad, when, lit, sum, trim, concat, 
    regexp_replace, round, format_number
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType
from trs_control_field import trs_control_field as tcf
from functools import reduce
import pandas as pd
import re
import os

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

trusted = dls['folders']['trusted']
business = dls['folders']['business']
sink = dls['folders']['business']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'
print(adl_trusted_1)

trusted_path_2 = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_2'])
adl_trusted_2 = f'{var_adls_uri}{trusted_path_2}'
print(adl_trusted_2)

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination'])
adl_destination_path = f'{var_adls_uri}{destination_path}'
print(adl_destination_path)

# COMMAND ----------


def read_parquet(file_path: str):
    return spark.read.format("parquet").load(file_path)

# COMMAND ----------

tabela_continentes = spark.read.format("parquet").load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/oni_trs/comercio_comtrade/tabela_continentes/')
commodity_anual = spark.read.table('datalake__trs.oni.oni_un_comtrade_comer_wld__commodity_annual')

# COMMAND ----------

# Configurações iniciais
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import (
    col, substring, lpad, when, lit, sum, trim, concat, 
    regexp_replace, round, format_number
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType
from functools import reduce
import pandas as pd
import re
import os

# COMMAND ----------


tabela_continentes = read_parquet(adl_trusted_1)

commodity_anual = read_parquet(adl_trusted_2)

# COMMAND ----------

tabela_continentes = spark.read.format("parquet").load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/oni_trs/comercio_comtrade/tabela_continentes/')
commodity_anual = spark.read.table('datalake__trs.oni.oni_un_comtrade_comer_wld__commodity_annual')

# COMMAND ----------

commodity_anual.select('NR_YEAR').distinct().display()

# COMMAND ----------



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

adl_destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/BACKUP/tmp/dev/biz/oni/painel_comercio_comtrade/comercio/'

# COMMAND ----------

commodity_anual6.write.format("parquet").mode("overwrite").option("compression", "snappy").save(adl_destination_path)

# COMMAND ----------

print(adl_destination_path)

# COMMAND ----------

df = commodity_anual6.drop('dh_insercao_trs')
df  = tcf.add_control_fields(df, adf, layer="biz")

df.write.format("parquet").mode("overwrite").option("compression", "snappy").save(adl_destination_path)
#final_df.write.mode('overwrite').parquet(adl_destination_path, compression='snappy')

# COMMAND ----------

df.display()

# COMMAND ----------

BIA = spark.read.format("parquet").load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/BACKUP/tmp/dev/biz/oni/painel_comercio_comtrade/comercio/')

# COMMAND ----------

BIA.display()

# COMMAND ----------

BIA.select('NR_YEAR').distinct().display()

# COMMAND ----------

