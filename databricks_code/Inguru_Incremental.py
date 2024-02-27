# Databricks notebook source
import json
import re
import cni_connectors.adls_gen1_connector as connector
import pyspark.sql.functions as f
import crawler.functions as cf
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

# COMMAND ----------

url = 'https://app.inguru.me/api/v1/taxonomies/nodes/930'

headers = {
'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
'accept':'application/json',
'authorization': '6f64d6a047fad369d35c3806f8f8e0560475075a432585973f91caae35b4ad74'
}

params = {
'per_page': 1,
'start_date': '2023-03-06 00:00',
'end_date': datetime.now().strftime("%d/%m/%Y %H:%M"),
'sort': '1'
}

response = requests.get(url, headers=headers, params=params)
response.json()

# COMMAND ----------

headers = {
  "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
  "accept" : "application/json",
  "authorization" : "6f64d6a047fad369d35c3806f8f8e0560475075a432585973f91caae35b4ad74"
}    

params = {'per_page': 1,
          'start_date': '2023-03-06 00:00',        
          'end_date': datetime.now().strtime("%d/%m/%Y %H:%M"),        
          'sort': '1'}

# COMMAND ----------

response = requests.get(url, headers=headers, params=params)    
response.json()

# COMMAND ----------

headers = {
  "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
  "accept" : "application/json",
  "authorization" : "6f64d6a047fad369d35c3806f8f8e0560475075a432585973f91caae35b4ad74"
}    

params = {'per_page': 1,
          'start_date': '2023-03-06 00:0',        
          'end_date': datetime.now().strtime("%d/%m/%Y %H:%M"),        
          'sort': '1' }    

url = 'https://app.inguru.me/api/v1/taxonomies/nodes/930'

response = requests.get(url, headers=headers, params=params)    
response.json()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/uld/API_InGuru/inguro_vig_acidente_trab_Prevenção_de_Acidentes_de_Trabalho/'.format(uri=var_adls_uri)
df_inguro_vig_acidente_trab_Prevenção_de_Acidentes_de_Trabalho = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)

#df_inguro_vig_acidente_trab_Prevenção_de_Acidentes_de_Trabalho.display()

df_inguro_vig_acidente_trab_Prevenção_de_Acidentes_de_Trabalho.sort(
  df_inguro_vig_acidente_trab_Prevenção_de_Acidentes_de_Trabalho.published_date.desc()
).display()

#df.sort(col("department").asc(),col("state").asc()).show(truncate=False)

# 2023-03-06

# COMMAND ----------

