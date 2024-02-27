# Databricks notebook source
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

observatorio_fiesc.write.format('csv').save('dbfs:/ZY/prmm', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------



# COMMAND ----------

pip install azure-storage-file-datalake

# COMMAND ----------

pip install azure-storage-blob azure-identity

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

account_name='onipublicdata'
account_key='zgeZwZDkkI0fn1nXqm1RkKOY9Ms8z4PgyHNILmfIhuHCUk+ufzgNuUppS/n1+583qt8TvET6hmGX+AStxg7FIg=='
container_name = 'public'

connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

blob_client = blob_service_client.get_blob_client(container_name, 'asdaskkkk/teste.csv')

# COMMAND ----------

blob_properties = blob_client.get_blob_properties()

# COMMAND ----------

print(blob_properties)

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

connection_string = "<your_connection_string>"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name = "<your_container_name>"
blob_name = "<your_blob_name>"

blob_client = blob_service_client.get_blob_client(container_name, blob_name)
blob_properties = blob_client.get_blob_properties()

metadata = blob_properties.metadata
print(metadata)

# COMMAND ----------

blob_client = blob_service_client.get_blob_client(container_name, 'asdaskkkk/teste.csv')

# COMMAND ----------

#dbutils.fs.cp("dbfs:/ZY/prmm")

# COMMAND ----------

with open('/dbfs/ZY/prmm/part-00000-tid-4124033457851439409-ca28e36f-79f3-4022-8c89-a0d2c0bc4fe6-695-1-c000.csv', 'rb') as f:
    blob_client.upload_blob(f)

# COMMAND ----------

dbutils.fs.rm('dbfs:/ZY/zhang', True)

# COMMAND ----------

