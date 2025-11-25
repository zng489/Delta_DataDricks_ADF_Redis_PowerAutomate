# Databricks notebook source
-- Monitoramento ADF (Mês a Mês)

-- Em caso de consumo do mês corrente, descer até a granularidade de dia. Mês fechado, pode ser consultado indo apenas até o nível de mês
 
-- Consulta dia

SELECT * FROM 

json.`/Volumes/oni_lab/default/adf_oni_log/resourcegroups/rg-oni/providers/microsoft.operationalinsights/workspaces/law-oni/y=2025/m=01/d=01/h=00/m=00/`
 
-- Consulta Mês

select * from

json.`/Volumes/oni_lab/default/adf_oni_log/resourcegroups/rg-oni/providers/microsoft.operationalinsights/workspaces/law-oni/y=2024/m=11/`
 

# COMMAND ----------

from datetime import datetime

from datetime import datetime
from datetime import datetime, timedelta

#now = datetime.now()
#ano = now.year
#mes = now.month
#dia = now.day

now = datetime.now() - timedelta(days=6)
ano = now.year
mes = now.month
dia = now.day

print(f"Ano: {ano}, Mês: {mes}, Dia: {dia}")
print(f'{ano}')
print(f'{mes:02d}')
print(f'{dia:02d}')

# COMMAND ----------

from datetime import datetime, timedelta

# Subtrair 6 dias da data atual
data_ref = datetime.now() - timedelta(days=6)

# Extrair ano, mês e dia com dois dígitos
ano = data_ref.year
mes = data_ref.month
dia = data_ref.day

# Mostrar as partes da data
print(f"Ano: {ano}, Mês: {mes:02d}, Dia: {dia:02d}")

# Montar caminho corretamente com f-string
caminho = f"/Volumes/oni_lab/default/adf_oni_log/resourcegroups/rg-oni/providers/microsoft.operationalinsights/workspaces/law-oni/y={ano}/m={mes:02d}/d={dia:02d}/"

print(f"Caminho final: {caminho}")

# Verificar se o caminho existe antes de tentar ler
if dbutils.fs.exists(caminho):
    # Leitura dos arquivos JSON
    df = spark.read \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .json(caminho)

    # Mostrar dados
    display(df)
else:
    print("❌ Caminho não existe ou não é acessível.")


# COMMAND ----------

from datetime import datetime

from datetime import datetime
from datetime import datetime, timedelta

#now = datetime.now()
#ano = now.year
#mes = now.month
#dia = now.day

now = datetime.now() - timedelta(days=6)
ano = now.year
mes = now.month
dia = now.day

print(f"Ano: {ano}, Mês: {mes}, Dia: {dia}")
print(f'{ano}')
print(f'{mes:02d}')
print(f'{dia:02d}')

# Caminho do diretório
caminho = f"/Volumes/oni_lab/default/adf_oni_log/resourcegroups/rg-oni/providers/microsoft.operationalinsights/workspaces/law-oni/y={ano}/m={mes:02d}/d={dia:02d}/"

# Leitura com opções
df = spark.read \
    .option("inferSchema", "true") \
    .option("mode", "PERMISSIVE") \
    .json(caminho)

# Mostrar dados
#display(df)

from pyspark.sql.functions import col

df_filtered = df.filter(~col("PipelineName").startswith("wkf"))\
.filter(~col("PipelineName").startswith("adf"))\
.filter(~col("PipelineName").startswith("trusted"))\
.filter(~col("PipelineName").startswith("business"))\
.filter(~col("PipelineName").startswith("import"))

df_filtered.display()

# COMMAND ----------


from pyspark.sql.functions import col

pipeline_list = ["PipelineName"]  # <- Evite usar o nome "list"
df_filtered = df.filter(~col("PipelineName").isin(pipeline_list))

# COMMAND ----------

from pyspark.sql.functions import col

# Leitura
df = spark.read \
    .option("inferSchema", "true") \
    .option("mode", "PERMISSIVE") \
    .json("/Volumes/oni_lab/default/adf_oni_log/resourcegroups/rg-oni/providers/microsoft.operationalinsights/workspaces/law-oni/")

# Mostrar esquema
print("Esquema inferido:")
df.printSchema()

# Filtrar por ano
df_2025 = df.filter(col("y") == 2025)

# Mostrar dados
display(df_2025.limit(10))

# COMMAND ----------

from databricks.sdk import sql
import os

connection = sql.connect(
                        server_hostname = "adb-6523536500265509.9.azuredatabricks.net",
                        http_path = "/sql/1.0/warehouses/5eeec6f85950ced2",
                        access_token = "dapifc4e498f71be8912e5da8dce4642bf1d")

cursor = connection.cursor()

cursor.execute("SELECT * FROM json.`/Volumes/oni_lab/default/adf_oni_log/resourcegroups/rg-oni/providers/microsoft.operationalinsights/workspaces/law-oni/y=2025/m=01/d=01/h=00/m=00/`")
print(cursor.fetchall())

cursor.close()
connection.close()

# COMMAND ----------

lista_de_strings = [('','')]

# Convertendo a lista de strings em um DataFrame
df_manutencao = spark.createDataFrame(lista_de_strings, ['NOME_DA_ATIVIDADE','MAN'])
dia_selecionado = "2025-07-07"


import os
import re
import crawler.functions as cf
import pyspark.sql.functions as f
from cni_connectors import adls_gen1_connector as adls_conn
from datetime import datetime

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import math
from pyspark.sql.functions import col, current_date, date_sub
from pyspark.sql.functions import max as max_, date_sub, col, lit

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2")

camadas = ["LND", "ULD", "RAW", "TRS", "BIZ"]
file_path = "/tmp/dev/raw/usr/oni/monitoramento_bkp_adf_oni/ano=2025"
adl_file_path = f"{var_adls_uri}{file_path}"

df_full = spark.read.parquet(adl_file_path)

'''
df = (
    df_full.select("DATA_EXECUCAO")
    .distinct()
    .filter(f"DATA_EXECUCAO <= '{dia_selecionado}'")
    .orderBy(df_full["DATA_EXECUCAO"].desc())
)
'''

"""
# Buscar a maior data da coluna
max_date = df_full.select(max_("DATA_EXECUCAO")).collect()[0][0]
print(max_date)

df_filtered = df_full.filter(col("DATA_EXECUCAO") <= date_sub(lit(max_date), 3))
df_filtered.display()

df_filtered.select('DATA_EXECUCAO').distinct().show()
"""



from pyspark.sql.functions import max as max_, date_sub, col, lit

# Pegar a maior data do DataFrame
max_date = df_full.select(max_("DATA_EXECUCAO")).first()[0]

# Filtrar registros que estejam no período de 3 dias até a data máxima
df_filtered = df_full.filter(
    (col("DATA_EXECUCAO") >= date_sub(lit(max_date), 3)) &
    (col("DATA_EXECUCAO") <= lit(max_date))
)

from pyspark.sql.functions import to_timestamp

# Converter para timestamp
df_filtered = df_filtered.withColumn("DH_EXECUCAO_INI", to_timestamp("DH_EXECUCAO_INI", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSX"))
df_filtered = df_filtered.withColumn("DH_EXECUCAO_FIN", to_timestamp("DH_EXECUCAO_FIN", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSX"))


from pyspark.sql.functions import col, unix_timestamp, expr

# Exemplo usando unix_timestamp (para Spark < 3.4)
df_com_tempo = df_filtered.withColumn(
    "TEMPO_EXECUCAO_MINUTOS",
    (unix_timestamp(col("DH_EXECUCAO_FIN")) - unix_timestamp(col("DH_EXECUCAO_INI"))) / 60
)



# Exibir o resultado
df_com_tempo.display()


df_suceeded = df_com_tempo.filter(df_com_tempo.STATUS_DA_ATIVIDADE == "Succeeded")
df_failed = df_com_tempo.filter(df_com_tempo.STATUS_DA_ATIVIDADE == "Failed")
df_inactive = df_com_tempo.filter(df_com_tempo.STATUS_DA_ATIVIDADE == "inactived")

df_suceeded.display()
df_failed.display()
df_inactive.display()

# COMMAND ----------

lista_de_strings = [('','')]

# Convertendo a lista de strings em um DataFrame
df_manutencao = spark.createDataFrame(lista_de_strings, ['NOME_DA_ATIVIDADE','MAN'])
dia_selecionado = "2025-06-26"


import os
import re
import crawler.functions as cf
import pyspark.sql.functions as f
from cni_connectors import adls_gen1_connector as adls_conn
from datetime import datetime

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import math


var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2")

camadas = ["LND", "ULD", "RAW", "TRS", "BIZ"]
file_path = "/tmp/dev/raw/usr/oni/monitoramento_bkp_adf_oni/ano=2025"
adl_file_path = f"{var_adls_uri}{file_path}"

df_full = spark.read.parquet(adl_file_path)


df = (
    df_full.select("DATA_EXECUCAO")
    .distinct()
    .filter(f"DATA_EXECUCAO <= '{dia_selecionado}'")
    .orderBy(df_full["DATA_EXECUCAO"].desc())
)


df = (
    df_full.select("DATA_EXECUCAO")
    .distinct()
    .filter(f"DATA_EXECUCAO <= '{dia_selecionado}'")
    .orderBy(df_full["DATA_EXECUCAO"].desc())
)
date_ini = df.select("DATA_EXECUCAO").collect()[4][0]

str_date_ini = datetime.strptime(str(date_ini), "%Y-%m-%d").strftime("%d/%m/%Y")
str_dia_selecionado = datetime.strptime(dia_selecionado, "%Y-%m-%d").strftime("%d/%m/%Y")
print(f"Dados das ultimas 5 execuções, entre os dias {str_date_ini} e {str_dia_selecionado}")

# Adiciona coluna de manutenção
df_full = df_full.join(df_manutencao, "NOME_DA_ATIVIDADE", "LEFT").filter(f.col('DATA_EXECUCAO').isNotNull())

# Substituir valores na coluna STATUS_DA_ATIVIDADE
df_full = df_full.withColumn("STATUS_DA_ATIVIDADE", 
                   f.when(f.col("STATUS_DA_ATIVIDADE") == "Inactive", "Inativo")
                   .when(f.col("STATUS_DA_ATIVIDADE") == "Succeeded", "Bem-sucedido")
                   .when(f.col("STATUS_DA_ATIVIDADE") == "Failed", "Falha")
                   .otherwise(f.col("STATUS_DA_ATIVIDADE")))

# # # Registrar o DataFrame como uma tabela temporária
df_full.select("NOME_DA_ATIVIDADE", "DATA_EXECUCAO", "STATUS_DA_ATIVIDADE","CAMADA","MAN").filter(
    df.DATA_EXECUCAO.between(date_ini, dia_selecionado)
)#.createOrReplaceTempView("TABLE_LOGS")

df_full.select("NOME_DA_ATIVIDADE", "DATA_EXECUCAO", "STATUS_DA_ATIVIDADE","CAMADA","MAN").filter(
    df.DATA_EXECUCAO.between(date_ini, dia_selecionado)
)

df.write.parquet("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/adf/monitoramento", mode="overwrite")

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

#var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from trs_control_field import trs_control_field as tcf
import pyspark.sql.functions as f
import crawler.functions as cf
from pyspark.sql import SparkSession
import time
import pandas as pd
from pyspark.sql.functions import col, when, explode, lit
import json
from unicodedata import normalize 
import datetime
import re
from core.string_utils import normalize_replace
from pyspark.sql.functions import concat, lit, col

#df = spark.read.csv("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/uld/oni/mte/novo_caged/identificada/2025/202504/CAGEDEXC202504ID/CAGEDEXC202504ID.txt")
#df


# COMMAND ----------

uld_path = "raw/usr/oni/ceis"
for path in cf.list_subdirectory(dbutils, uld_path):
  print(path)
  for x in cf.list_subdirectory(dbutils, uld_path):
    print(x)

# COMMAND ----------

