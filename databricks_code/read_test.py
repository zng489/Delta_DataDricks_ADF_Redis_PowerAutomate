# Databricks notebook source
dbutils.fs.ls("dbfs:/FileStore/tables")

# COMMAND ----------

dbutils.fs.ls("dbfs")

# COMMAND ----------

dbutils.fs.ls("ZY/zhang")

# COMMAND ----------

dbutils.fs.rm('dbfs:/ZY/prmm')
#dbfs:/ZY/prmm
#rm dbfs:/FileStore/tables/temp_dir/databricks1.jpg

# COMMAND ----------

/dbfs/ZY/prmm

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/trs/me/rais_vinculo/'.format(uri=var_adls_uri)
observatorio_fiesc = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)


# COMMAND ----------

observatorio_fiesc

# COMMAND ----------

account_name='onipublicdata'
account_key='zgeZwZDkkI0fn1nXqm1RkKOY9Ms8z4PgyHNILmfIhuHCUk+ufzgNuUppS/n1+583qt8TvET6hmGX+AStxg7FIg=='
container_name = 'public'

connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
#
#container_client = blob_service_client.get_container_client(container_name)
#container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

/dbfs/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt

# COMMAND ----------

blob_client = blob_service_client.get_blob_client(container_name, 'part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt')

# COMMAND ----------

with open('/dbfs/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt', 'rb') as f:
    blob_client.upload_blob(f)

# COMMAND ----------

dbfs:/ZY/airport_codes

# COMMAND ----------


blob_name = observatorio_fiesc

# Crie um BlobClient
blob_client = blob_service_client.get_blob_client(container_name, blob_name)

# Faça upload do arquivo CSV para o blob storage
with open('df', 'rb') as f:
  blob_client.upload_blob(f)

# COMMAND ----------

dir(container_clien)

# COMMAND ----------



# COMMAND ----------


# Configure blob storage account access key globally
#spark.conf.set(
#  "fs.azure.account.key.%s.blob.core.windows.net" % storage_name,
#  sas_key)

#output_container_path = "wasbs://%s@%s.blob.core.windows.net" % (output_container_name, storage_name)
#output_blob_folder = "%s/wrangled_data_folder" % output_container_path
copied_blob = blob_service_client('public', 'flourish/')

# write the dataframe as a single file to blob storage
observatorio_fiesc.write.mode("overwrite").option("header", "true").format("csv").save(copied_blob)

# COMMAND ----------

path

# COMMAND ----------

import os
import shutil
import pandas as pd
import requests
#from bs4 import BeautifulSoup
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from azure.storage.filedatalake import FileSystemClient

from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd

# COMMAND ----------

pip install azure-storage-file-datalake

# COMMAND ----------

pip install azure-storage-blob azure-identity

# COMMAND ----------

categorias = [
    "ds_agente_causador",
    "dt_acidente",
    "ds_cbo",
    "ds_emitente_cat",
    "hora_acidente",
    "ds_natureza_lesao",
    "ds_parte_corpo_atingida",
    "ds_tipo_acidente",
    "ds_tipo_local_acidente",
    "cd_indica_obito",
    "st_dia_semana_acidente",
    "ano_cat",
    "cd_municipio_ibge_cat",
    "cd_numero_cat",
    "idade_cat",
    "cd_tipo_sexo_empregado_cat",
    "ds_cnae_classe_cat",
    "st_acidente_feriado",
    "ds_grupo_agcausadores_cat",
    "cd_municipio_ibge_dv",
    "cd_municipio_ibge",
    "nm_municipio",
    "nm_municipio_sem_acento",
    "cd_uf",
    "latitude",
    "longitude",
    "nm_uf",
    "sg_uf",
    "nm_municipio_uf",
    "cd_unidade",
    "cd_prt",
    "nm_prt",
    "nm_unidade",
    "tp_unidade",
    "sg_unidade",
    "cd_mesorregiao",
    "nm_mesorregiao",
    "cd_microrregiao",
    "nm_microrregiao",
    "cd_regiao",
    "nm_regiao"]

querystring = lambda from_date, to_date: {
    "agregacao": "COUNT",
    "categorias": ",".join(categorias),
    "filtros": f"ge-dt_acidente-{from_date},and,le-dt_acidente-{to_date}"}

# COMMAND ----------

querystring

# COMMAND ----------

params=querystring('dasdasdas', 'dsadasdasdasda')
params

# COMMAND ----------

account_name='onipublicdata'
account_key='zgeZwZDkkI0fn1nXqm1RkKOY9Ms8z4PgyHNILmfIhuHCUk+ufzgNuUppS/n1+583qt8TvET6hmGX+AStxg7FIg=='
container_name = 'public'

connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_client = blob_service_client.get_container_client(container_name)
container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

account_name='onipublicdata'
account_key='zgeZwZDkkI0fn1nXqm1RkKOY9Ms8z4PgyHNILmfIhuHCUk+ufzgNuUppS/n1+583qt8TvET6hmGX+AStxg7FIg=='
container_name = 'public'

# COMMAND ----------

connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

print(container_client)

# COMMAND ----------

print(container_client)

# COMMAND ----------

blob_list = []
for blob_i in container_client.list_blobs():
    print(blob_i)
#    blob_i[0]


#        for blob in blob_list:
#        print(f"Name: {blob.name}")

# COMMAND ----------

account_name   = os.environ.get('account_name')
# Source
source_container_name = "<source container name>"
source_file_path = "<generated text file name>"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
source_blob = (f"https://{account_name}.blob.core.windows.net/{source_container_name}/{source_file_path}")
# Target
target_container_name = "<destination container name>"
target_file_path = "<destination text file name>"
copied_blob = blob_service_client.get_blob_client(target_container_name, target_file_path)
copied_blob.start_copy_from_url(source_blob)

# COMMAND ----------


with open(observatorio_fiesc, "rb") as data:
    container_client.upload_blob(data)

# COMMAND ----------

copied_blob = blob_service_client.get_blob_client('public', 'flourish/')

# COMMAND ----------

copied_blob.start_copy_from_url('https://cnibigdatadlsgen2.blob.core.windows.net/datalake/uds/uniepro/FIEMT/Mauro/mauro.parquet/')

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/lnd/crw/ibge__pop_estimada/2023/'.format(uri=var_adls_uri)

df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep', ';').option('mergeSchema', 'False').load(path)

# COMMAND ----------

df.count()

# COMMAND ----------

df.coalesce(1).write.format('csv').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/test_env_Yuan/folder_1/', header=True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------



# COMMAND ----------

df

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/lnd/crw/mpt__sst_cat/'.format(uri=var_adls_uri)
years = ['2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022']
for x in years:
  print(path+x)
  df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep', ';').load(path+x)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep', ';').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/lnd/crw/mpt__sst_cat/2012')



# COMMAND ----------

df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep', ';').load(path)

# COMMAND ----------

!pip install azure-storage-blob~=1.3

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

# COMMAND ----------

account_name='onipublicdata'
account_key='zgeZwZDkkI0fn1nXqm1RkKOY9Ms8z4PgyHNILmfIhuHCUk+ufzgNuUppS/n1+583qt8TvET6hmGX+AStxg7FIg=='
container_name = 'public'

# COMMAND ----------


connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

# COMMAND ----------

!pip install --upgrade azure-storage-blob

# COMMAND ----------

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# COMMAND ----------

import os
import shutil
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
#from azure.storage.filedatalake import FileSystemClient
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd

# COMMAND ----------



# COMMAND ----------

!pip install azure-keyvault-secrets

# COMMAND ----------

!pip install azure

# COMMAND ----------

spark.read.format("parquet").load("abfss://public@onipublicdata.dfs.core.windows.net/flourish/ONI/")

# COMMAND ----------

dbutils.fs.ls("abfss://container@storageAccount.dfs.core.windows.net/flourish/ONI/")

spark.read.format("parquet").load("abfss://public@onipublicdata.dfs.core.windows.net/flourish/ONI/")

spark.sql("SELECT * FROM parquet.`abfss://container@storageAccount.dfs.core.windows.net/external-location/path/to/data`")

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/biz/oni/bases_referencia/municipios/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep', ';').load(path)
df.columns

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/biz/oni/base_unica_empresas_brasileiras/class_inten_tec/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep', ';').load(path)
#df.columns
df.display()

# COMMAND ----------

#MauJun*9!@#$

# COMMAND ----------

df.count()

# COMMAND ----------

df.select("nm_setor_tema").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

df.nm_setor_tema

# COMMAND ----------

df.display()

# COMMAND ----------

def kinetic_energy(m:'in KG', v:'in M/S')->'Joules': 
    return 1/2*m*v**2

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


class DATA_LAKE(object):
  
 # -------------------------------------------------------------------------------------------------------
  # class attribute
  # self.AZURE_PATH
  AZURE_PATH = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

  def __str__(self):
    return 'okay'
  
  def __repr__(self):
    return print('__repr__') 

if __name__ == '__main__':
  ONI = DATA_LAKE()
  print(ONI)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


class DATA_LAKE(object):
  
 # -------------------------------------------------------------------------------------------------------
  
  # class attribute
  # self.AZURE_PATH
  AZURE_PATH = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  

 # ------------------------------------------------------------------------------------------------------- 
  
 #  methods in python are used for string representation of a string
  def __str__(self):
    return 'okay'
  
  #  methods in python are used for string representation of a string
  def __repr__(self):
    return 'okay' 
  
  # method
  # instance method to access instance variable          
  def __init__(self):
               #, value_0):
    #self.method_value = value_0
    #print(self.method_value)
  
    #print(self.AZURE_PATH) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    
    self.value_1 = DATA_LAKE.AZURE_PATH
    #print(self.value_1) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    
  # -------------------------------------------------------------------------------------------------------
  
  # method
  # instance method to access instance variable              
  def DADOS(self, datalake_path, file_format, delimiter ):
    self.DADOS_datalake_path = datalake_path
    self.DADOS_file_format = file_format
    self.DADOS_delimiter = delimiter
    df =  spark.read.format(self.DADOS_file_format).option("header","true").option('sep',self.DADOS_delimiter).option('encoding',"ISO-8859-1").load( self.value_1 + self.DADOS_datalake_path)
    #df = spark.read.format("parquet").option("encoding", "ISO-8859-1").option("header","true").option('sep',';').load(path)
    #df = spark.read.format('json').load("dbfs:/mnt/diags/logs/resourceId=/SUBSCRIPTIONS/<subscription>/RESOURCEGROUPS/<resource group>/PROVIDERS/MICROSOFT.APIMANAGEMENT/SERVICE/<api service>/y=*/m=*/d=*/h=*/m=00/PT1H.json")
    print(self.AZURE_PATH)
    return df
  
 # -------------------------------------------------------------------------------------------------------
  
  # function
  def Definir():
    pass
  
if __name__ == '__main__':
  ONI = DATA_LAKE()
  ONI.DADOS('biz/oni/prospectiva_mundo_do_trabalho/msp_cnae/','parquet','').display()

# COMMAND ----------

ONI = DATA_LAKE()
ONI.DADOS('biz/oni/prospectiva_mundo_do_trabalho/msp_cnae/','parquet').display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/lnd/crw/rfb_cnpj__cadastro_empresa/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)
df.count()

# COMMAND ----------

import pandas as pd
import requests

# COMMAND ----------

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

df

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/raw/usr/oni/prospectiva/modelo_mundo_do_trabalho/msp/msp_cnae/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)
df.display()
df.count()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/lnd/crw/inguru__prevencao_acidentes/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)

#.option("encoding", "utf-8").option('sep',';')
,df = df.withColumn("CRAWLED_DATE", df['CRAWLED_DATE'].cast(DateType()))
df.select(max ('CRAWLED_DATE')).show()
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

# path = '{uri}/tmp/dev/lnd/crw/inguru__judicializados/'.
#df.count()
# 2594

# COMMAND ----------

df = df.withColumn("CRAWLED_DATE", df['CRAWLED_DATE'].cast(DateType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(max ('CRAWLED_DATE')).show()

# COMMAND ----------

df.printSchema

# COMMAND ----------

/base_unica_empresas_brasileiras/base/unificada_cnpjs/

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
pnadc_a_visita5_f_ANO_2021_path = '{uri}/trs/ibge/pnadc_a_visita5_f/ANO=2021/'.format(uri=var_adls_uri)
pnadc_a_visita5_f_ANO_2021 = spark.read.format("parquet").option("header","true").load(pnadc_a_visita5_f_ANO_2021_path)


# COMMAND ----------

len(pnadc_a_visita5_f_ANO_2021.columns)

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
pnadc_a_visita5_f_ANO_2022_path = '{uri}/trs/ibge/pnadc_a_visita5_f/ANO=2022/'.format(uri=var_adls_uri)
pnadc_a_visita5_f_ANO_2022 = spark.read.format("parquet").option("header","true").load(pnadc_a_visita5_f_ANO_2022_path)

# COMMAND ----------

len(pnadc_a_visita5_f_ANO_2022.columns)

# COMMAND ----------



import pandas as pd
import logging
from google.cloud import bigquery
from google.auth import compute_engine
import google.auth
import _locale
import datetime as dt
from datetime import datetime, date, timedelta
import sys

# Cria as credencias do Drive & BigQuery.
credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/bigquery",
    ]
)

# BigQuery client.
client_bq = bigquery.Client(credentials=credentials, project=project)
job_config = bigquery.QueryJobConfig(use_query_cache=False)

# Configura o encode
_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

def nome_coluna(tabela):
    if 1 == 1:
        sql = f"""SELECT
                    column_name 
                    FROM
                    data_set.INFORMATION_SCHEMA.COLUMNS
                    WHERE
                    table_name = '{tabela}';"""
    df = pd.read_gbq(sql)
    colunas = df['column_name'].tolist()
    return colunas

def main(self):

    # Log
    logging.info('Inicio da rotina de carga da tabela CONTRATO')
    logging.info('Realizando carga full.')
    #data_atual = date.today()
    try:
        tabela = 'CONTRATO'
        colunas_tabela = nome_coluna(tabela)
        print(colunas_tabela)
    except Exception as e:
        logging.error('Erro ao coletar o nome das colunas - {}'.format(e))
        sys.exit(0)    
    df_origem = pd.read_csv(f'gs://gcp_bucket_datalake/gregory/sp_2023-06-26_CONTRATO.csv', sep=';', encoding='utf-8-sig', names=colunas_tabela, skiprows=1, on_bad_lines='skip', low_memory=False)
    
    for col in df_origem.columns:
        df_origem[col] = df_origem[col].astype(str)
    
    try:
        if 1 == 1:
            table = 'CONTRATO'
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client_bq.load_table_from_dataframe(df_origem, table, job_config=job_config)
            job.result()
            logging.info('Registros inseridos com sucesso.')
        else:
            logging.info('Não existem registros para serem inseridos.')
    except Exception as e:
        logging.error('Erro ao inserir os dados do dataframe no BigQuery - {}'.format(e))
        sys.exit(0)                        
            
    
    # Grava as informações no arquivo de log
    fim = dt.datetime.now()
    logging.info('Fim da rotina' + '\n')

# COMMAND ----------

ano_uf_cnae = spark.read.format("parquet").load("dbfs:/user/hive/warehouse/absenteismo.db/ano_uf_cnae")
ano_uf_cnae.display()

# COMMAND ----------

custo_cnae = spark.read.format("parquet").load("dbfs:/user/hive/warehouse/absenteismo.db/custo_cnae")
custo_cnae.display()

# COMMAND ----------


cond = [
    (tabela_interna_criada.dominio_== df_grupodestino.dominio)
        ]

df1 = tabela_interna_criada.join(df_grupodestino, cond, how='left')

# COMMAND ----------

cond = [
    (ano_uf_cnae.CD_UF == custo_cnae.CD_UF) & (ano_uf_cnae.CD_CNAE20_DIVISAO == custo_cnae.CD_CNAE20_DIVISAO) & (ano_uf_cnae.ANO == custo_cnae.ANO) & (ano_uf_cnae.COUNT_ID_CPF == custo_cnae.count_ID_CPF)
        ]

        

DF = ano_uf_cnae.join(custo_cnae , cond , how='inner').drop(ano_uf_cnae.CD_UF).drop(ano_uf_cnae.CD_CNAE20_DIVISAO).drop(ano_uf_cnae.ANO)

# COMMAND ----------

DF.display()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/absenteismo.db/ano_ano'))

# COMMAND ----------

/custo_cnae

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
raw_cnae_industrial_path = '{uri}/tmp/dev/biz/oni/absenteismo/custo_direto_cd_cnae_20/'.format(uri=var_adls_uri)
raw_cnae_industrial = spark.read.format("parquet").option("header","true").load(raw_cnae_industrial_path)
raw_cnae_industrial.display()

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
raw_cnae_industrial_path = '{uri}/raw/usr/uniepro/cnae_industrial/'.format(uri=var_adls_uri)
raw_cnae_industrial = spark.read.format("parquet").option("header","true").load(raw_cnae_industrial_path)

raw_cnae_industrial = raw_cnae_industrial.select('divisao','denominacao','agrupamento')
df = raw_cnae_industrial.toPandas().to_dict('list')

# COMMAND ----------

df

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
cbo_classificacoes_path = '{uri}/uds/uniepro/cbo_classificacoes/'.format(uri=var_adls_uri)
cbo_classificacoes = spark.read.format("csv").option("header","true").option('sep',';').load(cbo_classificacoes_path)
cbo_classificacoes.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/biz/oni/bases_referencia/cbo/cbo4/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)
df.filter(col('CD_CBO4')==101)
df.display()

# COMMAND ----------

df.select(max('DT_REGISTRO')).show()

# COMMAND ----------

df.select(max('DT_SITUACAO_OBRA')).show()

# COMMAND ----------

df = spark.read.load("dbfs:/user/hive/warehouse/fato")
df.display()

# COMMAND ----------

len(df.columns)

# COMMAND ----------

df.count()

# COMMAND ----------

x = ['homem',
'mulher',
'rais_08_10_15_20_consolidada_painel_lancamento',
'rais_25_2008',
'rais_25_2010',
'rais_25_2015',
'rais_25_2020',
'rais_cbo4',
'rais_cnae',
'rais_municipio',
'rais_uf',
'todos_anos_rais_consolidada_painel_lancamento',
'vw_ano_sexo',
'vw_ano_sexo_uf',
'vw_ano_sexo_uf_cbo4',
'vw_ano_sexo_uf_cbo4_acumulada',
'vw_ano_sexo_uf_cdcnae20divisao',
'vw_ano_sexo_uf_cnae',
'vw_empregosporidade',
'vw_geo',
'vw_maioresocupacoesesetores',
'vw_maioresocupacoesesetoresmulheres',
'vw_maioresocupacoesmulheres',
'vw_maioressetoresmulheres',
'vw_uf']

# COMMAND ----------

spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

spark.read.format("delta").load("dbfs:/user/hive/warehouse/rais_25.db/homem")
  

# COMMAND ----------


df = spark.read.load(f"dbfs:/user/hive/warehouse/rais_25.db/homem")

# COMMAND ----------

for i in x:
    df = dbutils.fs.put(f"dbfs:/user/hive/warehouse/rais_25.db/{i}")
    #.format('csv').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/danilo/', header=True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------


'homem',
'mulher',
'rais_08_10_15_20_consolidada_painel_lancamento
'rais_25_2008',
'rais_25_2010',
'rais_25_2015',
'rais_25_2020',
'rais_cbo4',
'rais_cnae',
'rais_municipio',
'rais_uf',
'todos_anos_rais_consolidada_painel_lancamento',
'vw_ano_sexo',
'vw_ano_sexo_uf',
'vw_ano_sexo_uf_cbo4',
'vw_ano_sexo_uf_cbo4_acumulada',
'vw_ano_sexo_uf_cdcnae20divisao',
'vw_ano_sexo_uf_cnae',
'vw_empregosporidade',
'vw_geo',
'vw_maioresocupacoesesetores',
'vw_maioresocupacoesesetoresmulheres',
'vw_maioresocupacoesmulheres',
'vw_maioressetoresmulheres',
'vw_uf


# COMMAND ----------

uds/uniepro/rais_25/

# COMMAND ----------

FIESC.write.format('csv').save('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/danilo/', header=True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

df = spark.read.load("dbfs:/user/hive/warehouse/fato")

# COMMAND ----------

spark.read.format("delta").load("dbfs:/user/hive/warehouse/rais_25.db")

# COMMAND ----------

df = spark.read.load("dbfs:/user/hive/warehouse/fato")

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

from tqdm import tqdm_notebook

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/lnd/crw/ibge__pintec_tipo_inov_proj_po/2008/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)

for x in tqdm_notebook(df.display(), desc='dasda'):
  x

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/lnd/crw/ibge__pintec_tipo_inov_proj_po/2008/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)
#
df.display()
df.columns

# COMMAND ----------

len(df.columns)

# COMMAND ----------



# COMMAND ----------

  df.columns

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = '{uri}/tmp/dev/lnd/crw/ibge__pintec_tipo_inov_proj_po/2017/'.format(uri=var_adls_uri)
df = spark.read.format("parquet").option("header","true").load(path)
#
df.display()

# COMMAND ----------

/tmp/dev/lnd/crw/ibge__pintec_tipo_inov_proj_po/2014/

# COMMAND ----------

/tmp/dev/lnd/crw/ibge__pintec_tipo_inov_proj_po/2011/

# COMMAND ----------

/tmp/dev/lnd/crw/ibge__pintec_tipo_inov_proj_po/2008/

# COMMAND ----------

