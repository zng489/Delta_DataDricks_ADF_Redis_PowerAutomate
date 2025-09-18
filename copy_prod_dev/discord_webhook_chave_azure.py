# Databricks notebook source
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
df = spark.read.parquet('{uri}/biz/oni/painel_da_agroindustria/imp'.format(uri=var_adls_uri))


# COMMAND ----------



# COMMAND ----------

import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# COMMAND ----------


# Configurar variáveis de ambiente
os.environ["AZURE_CLIENT_ID"] = 
#"63cf6f6b-b770-4819-a782-a6a89ab204e1"
os.environ["AZURE_CLIENT_SECRET"] = 
#"iA9~9Y..jdsZi9DECDwEF-c2N~k6Rs6fo9"
os.environ["AZURE_TENANT_ID"] = 
#"6d6bcc3f-bda1-4f54-af1d-86d4b7d4e6b8"
os.environ["AZURE_KEY_VAULT_URI"] = 
#"https://cnibigdatakeyvaultdev.vault.azure.net/"
os.environ["AZURE_ADLS_STORAGE"] = 
#"cnibigdatadlsgen2"
os.environ["AZURE_ADLS_CONTAINER"] = 
#"datalake"
os.environ["AZURE_ADLS_PATH"] = 
#"/tmp/dev/lnd/crw"

# Autenticação com Azure usando ClientSecretCredential
credential = ClientSecretCredential(
    tenant_id=os.environ["AZURE_TENANT_ID"],
    client_id=os.environ["AZURE_CLIENT_ID"],
    client_secret=os.environ["AZURE_CLIENT_SECRET"]
)

# Inicializar o BlobServiceClient
blob_service_client = BlobServiceClient(
    account_url=f"https://{os.environ['AZURE_ADLS_STORAGE']}.blob.core.windows.net/",
    credential=credential
)

# Obter o cliente do contêiner
container_client = blob_service_client.get_container_client(os.environ["AZURE_ADLS_CONTAINER"])

###################################################
# Listar blobs no contêiner
##### blobs = container_client.list_blobs()

# Imprimir os nomes dos blobs
##### for blob in blobs:
#####     print(blob.name)
###################################################

# tmp/dev/biz/oni

# Definir o prefixo para a pasta desejada
prefix = 'lnd/crw'

# Listar blobs no contêiner com o prefixo
blobs = container_client.list_blobs(name_starts_with=prefix)

# Imprimir os nomes dos blobs
for blob in blobs:
    print(blob.name)


# COMMAND ----------

import requests
import json
 
# URL do webhook do Discord
webhook_url = 'https://discord.com/api/webhooks/1285654063169540156/NnZO27R4tdxsJp-j9fDa8cOjcGIg1l7sGzm_sZs95E4KY7D360CR8UOVo6Omyyxk4zOv'
 
# Mensagem que você quer enviar
message = {
    "content": "Sua mensagem aqui!"
}
 
# Enviando a requisição POST para o webhook
response = requests.post(webhook_url, data=json.dumps(message), headers={'Content-Type': 'application/json'})
 
# Verificando se a requisição foi bem-sucedida
if response.status_code == 204:
    print("Mensagem enviada com sucesso!")
else:
    print(f"Falha ao enviar a mensagem: {response.status_code}")

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


# COMMAND ----------

from pyspark.sql import functions as f

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
df = spark.read.parquet('{uri}/tmp/dev/raw/usr/oni/mte/rais/publica/vinculos/ANO=2024/'.format(uri=var_adls_uri))
df = df.filter(f.col("FL_VINCULO_ATIVO_3112")== 1)

# COMMAND ----------

df.count()

# COMMAND ----------

df.filter(f.col("Elegível")== "Sim").count()