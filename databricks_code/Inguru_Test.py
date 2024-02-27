# Databricks notebook source
import pandas as pd
import requests

url = 'https://app.inguru.me/api/v1/taxonomies/nodes/news/8789'

headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'accept':'application/json',
        'authorization': '6f64d6a047fad369d35c3806f8f8e0560475075a432585973f91caae35b4ad74'
        } 
                
params = {'per_page': 1000,
      'start_date': '04/07/2023 00:00',
      'end_date': '',
      'sort': '2',
      'group_similiar':'1'}



response = requests.get(url=url, headers=headers, params=params)
df = pd.json_normalize(response.json())

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
reading_path = '{uri}/tmp/dev/biz/oni/monitor_de_acidentes/acidentes_de_trabalho/crawled_year=2023/crawled_month=3/crawled_day=12/'.format(uri=var_adls_uri)
df_ = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(reading_path)
df_

# COMMAND ----------

df_.display()

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
df_.write.format('parquet').save(var_adls_uri + '/uds/uniepro/sdasdasdas/', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

df_.write.format('parquet').save(var_adls_uri + '/uds/uniepro/sdasdasdas/', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

var_adls_uri
df_.write.format("parquet").mode("append").mode("overwrite").save(var_adls_uri+"/tmp/dev/uld/testing")


(df_rais____.coalesce(1).write.format('parquet').save(var_adls_uri + '/uds/uniepro/sdasdasdas/', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1'))

# COMMAND ----------

CNO_TOTAIS.coalesce(1).write.option("encoding", "ISO-8859-1").option('header','true').parquet(Saving_path, mode='overwrite')

# COMMAND ----------

.option("header",true)

# COMMAND ----------

(df_rais____.coalesce(1).write..option("header",true)format('csv').save(var_adls_uri + '/uds/uniepro/Estabelecimentos_Amapa_2018_621/', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1'))

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
reading_path = '{uri}/tmp/dev/biz/oni/monitor_de_acidentes/acidentes_de_trabalho/crawled_year=2023/crawled_month=3/crawled_day=12/'.format(uri=var_adls_uri)
df_ = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(reading_path)
df_

# COMMAND ----------

#from datetime import datetime, timedelta

# COMMAND ----------

#from pyspark.sql.window import Window
#import pyspark.sql.functions as f
#from pyspark.sql.functions import *
#from pyspark.sql.types import *
#from pyspark.sql.functions import datediff,col,when,greatest

#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#path__ = '{uri}/uds/uniepro/API_InGuru/Teste/API_InGuru/inguro_acidente_trab_com_obito_Nodo_Raiz_data=21032023/'.format(uri=var_adls_uri)
#cadastro_cbo = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path__)
#cadastro_cbo.display()

# COMMAND ----------

import json
import re
#import cni_connectors.adls_gen1_connector as connector
import pyspark.sql.functions as f
#import crawler.functions as cf
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
reading_path = '{uri}/tmp/dev/biz/oni/monitor_de_acidentes/acidentes_de_trabalho/crawled_year=2023/crawled_month=3/crawled_day=12/'.format(uri=var_adls_uri)
cadastro_cbo = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(reading_path)
#cadastro_cbo.display()

# COMMAND ----------

def API_INGURU(url, per_page, start_date, end_date):
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    'accept':'application/json',
    'authorization': '6f64d6a047fad369d35c3806f8f8e0560475075a432585973f91caae35b4ad74'
    } 

    params = {
        'per_page': per_page,
        'start_date': start_date,
        'end_date': end_date,
        'sort': '1'
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

# COMMAND ----------

def divide_data(data_inicial, data_final):
    formato_data = '%d/%m/%Y %H:%M'

    data_inicial = datetime.strptime(data_inicial, formato_data)
    data_final = datetime.strptime(data_final, formato_data)
    intervalo = data_final - data_inicial
    data_final = data_inicial + intervalo // 2
    data_final = data_final.strftime(formato_data)
    data_inicial = data_inicial.strftime(formato_data)
    return (data_inicial, data_final)

# COMMAND ----------

def load_big_df(path):
  schema = StructType([
    StructField("id", LongType(), nullable=True),
    StructField("domain", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("subtitle", StringType(), nullable=True),
    StructField("author", StringType(), nullable=True),
    StructField("content", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("source_country", StringType(), nullable=True),
    StructField("source_state", StringType(), nullable=True),
    StructField("crawled_date", StringType(), nullable=True),
    StructField("published_date", StringType(), nullable=True),
    StructField("dh_insertion_raw", StringType(), nullable=True)
  ])

  df = spark.read.schema(schema) \
             .option("mergeSchema", "true") \
             .option("timestampFormat", "INT96") \
             .parquet(path) \
             .withColumn("dh_insertion_raw", f.col("dh_insertion_raw").cast("timestamp"))\
             .withColumn("crawled_date", f.col("crawled_date").cast("timestamp"))
  
  return df

# COMMAND ----------

def get_news(data_inicial, data_final, id):
    url = 'https://app.inguru.me/api/v1/taxonomies/nodes/news/'+str(id)

    array_dfs = []

    getted_news = 0
    
    total_news = API_INGURU(url, 1, data_inicial, data_final)['pagination']['total']

    
    if not total_news:
      parcial_data_inicial = data_inicial
      parcial_data_final = data_final
      dados = {
        'author': np.nan,
        'content': np.nan,
        'crawled_date': np.nan,
        'domain': np.nan,
        'id': np.nan,
        'published_date': np.nan,
        'source': np.nan,
        'source_country': np.nan,
        'source_state': np.nan,
        'subtitle': np.nan,
        'title': np.nan,
        'url': np.nan,
        'dh_insertion_raw': np.nan
      }
      fail_df = pd.DataFrame(dados, index=[0])
      array_dfs.append(fail_df)
      parcial_data_inicial = parcial_data_final
      parcial_data_final = data_final
    else:
      
      print('Total de notícias: ', total_news)

      parcial_data_inicial = data_inicial
      parcial_data_final = data_final

      while getted_news < total_news:

          parcial_news = total_news

          while parcial_news > 10000:
              print('Parcial de notícias: ', parcial_news)
              parcial_data_inicial, parcial_data_final = divide_data(parcial_data_inicial, parcial_data_final)
              parcial_news = API_INGURU(url, 1, parcial_data_inicial, parcial_data_final)['pagination']['total']

          print("fazendo requisicao")
          print('Data inicial: ', parcial_data_inicial)
          print('Data final: ', parcial_data_final)
          news = API_INGURU(url, parcial_news, parcial_data_inicial, parcial_data_final)['data']

          #CRIA UM DATAFRAME COM AS NOTICIAS
          noticias = pd.DataFrame(news)
          #ADICIONA NUM ARRAY DE DATAFRAME
          array_dfs.append(noticias)

          #corrige as datas

          parcial_data_inicial = parcial_data_final
          parcial_data_final = data_final

          getted_news += parcial_news
          

    return pd.concat(array_dfs)

# COMMAND ----------

#Parametros de entrado do ADF
var_file = {'namespace':'api_inguru',
'file_folder':'inguru_test'}
            
            
#'extension':'csv',
#'column_delimiter':',',
#'encoding':'UTF-8',
#'null_value':''}

#Caminho de Dev
var_dls = {
    "folders":{"landing":"/tmp/dev/uld",
    "error":"/tmp/dev/err",
    "staging":"/tmp/dev/stg",
    "log":"/tmp/dev/log",
    "raw":"/tmp/dev/raw",
    "trusted":"/tmp/dev/trs",
    "business":"/tmp/dev/biz",
    "landing":"/tmp/dev/uld",
    "uds":"/uds/uniepro"}}
#Parametros do ADF (Nativo)
var_adf = {
    "adf_factory_name": "cnibigdatafactory",
    "adf_pipeline_name": "org_raw_investimento",
    "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
    "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
    "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
    "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
    "adf_trigger_type": "PipelineActivity"}

# COMMAND ----------

#Acesso ao json do CMD
lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']
uds = var_dls['folders']['uds']

# COMMAND ----------

#SALVANDO EM: /tmp/dev/uld/API_InGuru/inguro_vig_acidente_trab_NOME_DO_NO
# /tmp/dev/uld/API_InGuru/inguro_vig_acidente_trab_NODO_RAIZ
# /tmp/dev/uld/API_InGuru/inguro_vig_acidente_trab_Acidentes_de_Trabalho_Judicializados
# /tmp/dev/uld/API_I

graph_ids = []
graph_names = []
graph_dfs = []

nodeID = '927' #Declaração do ID

url_nodeID = 'https://app.inguru.me/api/v1/taxonomies/nodes/'+str(nodeID)
IDs = API_INGURU(url = url_nodeID, per_page=1, start_date='', end_date='')['data'][0]
graph_ids.append(IDs['id'])
graph_names.append(IDs['name'].replace(" ", "_"))

#Passa por cada "filho" no nó principal, caso não tenha nenhum, nada acontece.
for node_children in IDs['children']:
  graph_ids.append(node_children['id'])
  graph_names.append(node_children['name'].replace(" ", "_"))

#Obtém as notícias de cada nó encontrado começando pelo nó principal.
#Cria um df com as notícias obtidas e salva em um array de dfs.
end_date = datetime.now().strftime("%d/%m/%Y %H:%M")
start_date = (datetime.now() - timedelta(days=7)).strftime("%d/%m/%Y %H:%M")
for id in graph_ids:
  url = 'https://app.inguru.me/api/v1/taxonomies/nodes/news/'+str(id)
  temp_df = get_news(start_date,end_date, id).fillna("Não informado")
  graph_dfs.append(spark.createDataFrame(temp_df))


# COMMAND ----------

lnd

# COMMAND ----------

raw

# COMMAND ----------

uds

# COMMAND ----------

#var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
#var_source

# COMMAND ----------

#Origem e destino
source_lnd = "{adl_path}/{uds}/{namespace}/{file_folder}".format(adl_path=var_adls_uri, uds=uds, namespace=var_file["namespace"],file_folder=var_file["file_folder"])
source_lnd

# COMMAND ----------

#Inserção de hora nas tabelas.
dh_insertion_raw = var_adf["adf_trigger_time"].split(".")[0] #Obtendo hora do sistema

for i in range(len(graph_dfs)):
  graph_dfs[i] = graph_dfs[i].withColumn("dh_insertion_raw", f.lit(dh_insertion_raw).cast("string")) #Gravando hora em cada um dos dataframes

# COMMAND ----------

#Adição de novos registros

for i in range(len(graph_dfs)):
  
  # path where the file will be save it.
  path = source_lnd + "_" + str(graph_names[i])
  
  
  # reading_path 
  read_path = reading_path
  old_df = load_big_df(read_path)
  
  max_date = old_df.select(f.max("crawled_date")).collect()[0][0]
  
  min_date = max_date - timedelta(days=7)
  print(max_date, min_date)
  old_df.filter(old_df["crawled_date"] > min_date)
  
  diff_df = graph_dfs[i].join(old_df, on='id', how='left_anti')
  
  date = datetime.today().strftime("%d%m%Y")
  write_path = path + str(f"_data={date}")
  print(write_path)
  diff_df.write.parquet(write_path, 'append')

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_PATH = '{uri}/uds/uniepro/api_inguru/inguru_test_NODO_ACIDENTES_DE_TRABALHO_COM_ÓBITOS_data=22032023/'.format(uri=var_adls_uri)
Q = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(PATH_PATH)
Q.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_PATH = '{uri}/uds/uniepro/api_inguru/inguru_test_Nodo_Raiz_data=22032023/'.format(uri=var_adls_uri)
Q = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(PATH_PATH)
Q.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

# tmp/dev/uld/API_InGuru/inguro_acidente_trab_com_obito_NODO_ACIDENTES_DE_TRABALHO_COM_ÓBITOS/
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
PATH_PATH = '{uri}/tmp/dev/uld/API_InGuru/inguro_acidente_trab_com_obito_NODO_ACIDENTES_DE_TRABALHO_COM_ÓBITOS//'.format(uri=var_adls_uri)
Q = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(PATH_PATH)
Q.display()

# COMMAND ----------

