# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.window import Window
# MAGIC import pyspark.sql.functions as f
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import datediff,col,when,greatest
# MAGIC import re
# MAGIC
# MAGIC var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
# MAGIC path = '{uri}/tmp/dev/lnd/crw/mtp_aeat__motivo_ida_sex/2021/'.format(uri=var_adls_uri)
# MAGIC
# MAGIC df = spark.read.format("parquet").option("delimiter", ";").option("header","true").load(path)
# MAGIC df.columns

# COMMAND ----------

 ['CD_ANO_MES_COMPETENCIA_MOVIMENTACAO',
 'CD_REGIAO',
 'CD_UF',
 'CD_MUNICIPIO',
 'CD_SECAO',
 'CD_CNAE20_SUBCLASSE',
 'CD_SALDO_MOV',
 'CD_CBO',
 'CD_CATEGORIA',
 'CD_GRAU_INSTRUCAO',
 'VL_IDADE',
 'QT_HORA_CONTRAT',
 'COD_RACA_COR',
 'CD_SEXO',
 'CD_TP_EMPREGADOR',
 'CD_TIPO_ESTAB',
 'CD_TIPO_MOV_DESAGREGADO',
 'CD_TIPO_DEFIC',
 'FL_IND_TRAB_INTERMITENTE',
 'FL_IND_TRAB_PARCIAL',
 'VL_SALARIO_MENSAL',
 'CD_FAIXA_EMPR_INICIO_JAN',
 'FL_IND_APRENDIZ',
 'CD_FONTE',
 'CD_COMPETENCIA_DECL',
 'FL_IND_FORA_PRAZO',
 'CD_COMPETENCIA_EXCL',
 'FL_IND_EXCLUSAO',
 'NM_ORIGEM',
 'ORIGEM']

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.window import Window
# MAGIC import pyspark.sql.functions as f
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import datediff,col,when,greatest
# MAGIC import re
# MAGIC
# MAGIC var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
# MAGIC path = '{uri}/tmp/dev/lnd/crw/mtp_aeat__motivo_div/2022/'.format(uri=var_adls_uri)
# MAGIC
# MAGIC df = spark.read.format("parquet").option("delimiter", ";").option("header","true").load(path)
# MAGIC df.display()

# COMMAND ----------



# COMMAND ----------

# install.packages("remotes")
remotes::install_github("rfsaldanha/microdatasus")
library(microdatasus)
sum_function <- function(value) {
  #x <- c(2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022)
  x <- c(2000, 2001)
  for (y in x) {
    year <- y
    print(year)
    dados_sinasc <- fetch_datasus(year_start=year, year_end=year, uf="SP", information_system=value)
    #write.csv(dados_sinasc, file = paste0("SIM_DO_",year, ".csv"), row.names = FALSE)
    write.csv(dados_sinasc, file = paste0(value, "_",year, ".csv"), row.names = FALSE)
  }
  return(NULL)
}

sum_function("SIM-DO")


# COMMAND ----------


install.packages("PNADcIBGE")
library(PNADcIBGE)
pnad = get_pnadc(2022, topic = 4, design = FALSE)
write.csv(pnad)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.window import Window
# MAGIC import pyspark.sql.functions as f
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import datediff,col,when,greatest
# MAGIC import re
# MAGIC
# MAGIC var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
# MAGIC path = '{uri}/uds/oni/observatorio_nacional/saude/aeat/MOTIVO_CLAS/'.format(uri=var_adls_uri)
# MAGIC
# MAGIC df = spark.read.format("csv").option("delimiter", ";").option("header","true").load(path)
# MAGIC df.display()
# MAGIC #df.write.format('delta').mode("append").save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/especialistas_negocios/delta/', header = True, mode='overwrite')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.window import Window
# MAGIC import pyspark.sql.functions as f
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import datediff,col,when,greatest
# MAGIC import re

# COMMAND ----------

# MAGIC %python
# MAGIC var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
# MAGIC path = '{uri}/uds/oni/observatorio_nacional/avaliacao_impacto_teletrabalho/pnad_csv/'.format(uri=var_adls_uri)
# MAGIC
# MAGIC df = spark.read.format("csv").option("header","true").load(path)
# MAGIC
# MAGIC #df.write.format('delta').mode("append").save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/especialistas_negocios/delta/', header = True, mode='overwrite')

# COMMAND ----------

# MAGIC %python
# MAGIC df.display()

# COMMAND ----------

# MAGIC %python
# MAGIC df.write.format('parquet').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/avaliacao_impacto_teletrabalho/pnad_parquet/', header = True, mode='overwrite')

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.format("parquet").option("header","true").load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/avaliacao_impacto_teletrabalho/pnad_parquet/')

# COMMAND ----------



# COMMAND ----------

