# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

import os
import requests, zipfile
import shutil
import pandas as pd
import glob
import subprocess
from threading import Timer
import shlex
import logging
import json
from core.bot import log_status
# drop_directory is not allowed in the databricks
from core.adls import upload_file
import pyspark.sql.functions as f
import re
import shlex
import time
import pyarrow.parquet as pa
from time import strftime
from datetime import datetime
from datetime import datetime, date
from unicodedata import normalize
from collections import namedtuple
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
import warnings
warnings.simplefilter('ignore')

# COMMAND ----------

!pip install pymongo

# COMMAND ----------

!pip install motor

# COMMAND ----------

adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"lnd_org_raw_trello_enterprise_board_organizations","adf_pipeline_run_id":"3fdd924e-d24d-4644-bc5e-68cb000d7849","adf_trigger_id":"6e3289dc8859437888605e22c1ba7e40","adf_trigger_name":"Sandbox","adf_trigger_time":"2024-01-08T13:22:21.2585028Z","adf_trigger_type":"Manual"}

bot_name = 'org_raw_mongodb_monitor_de_investimento'

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

env = "{\"env\":\"dev\"}"

# COMMAND ----------

dbutils.widgets.text("user_parameters", '{"null": "null"}')
dbutils.widgets.text("env", 'dev')
dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

### params = json.loads(re.sub("\'", '\"', dbutils.widgets.get("params")))
### dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
### adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import pandas as pd
from pyspark.sql import SparkSession

connection_string = "mongodb+srv://produtooni:Pvkhg05I1gOT0FWc@monitorinvest.nttgenh.mongodb.net/monitorinvest?retryWrites=true&w=majority"

async def fetch_documents():
    client = AsyncIOMotorClient(connection_string)
    db = client['monitor_investimento']
    collection = db['cadastro']

    documents = await collection.find().to_list(None)  # Fetch all documents
    client.close()
    return documents

async def process_and_save():
    documents = await fetch_documents()
    df = pd.DataFrame(documents)
    df['_id'] = df['_id'].astype(str)
    df['historico'] = df['historico'].apply(lambda x: str(x) if isinstance(x, list) else x)
    df['municipios'] = df['municipios'].apply(lambda x: str(x) if not isinstance(x, str) else x)
    df['cnpj_basico'] = df['cnpj_basico'].astype(str)
    df['setor_empresa'] = df['setor_empresa'].apply(lambda x: str(x) if not isinstance(x, str) else x)

    if not df.empty:
        pyspark_df = spark.createDataFrame(df)
    return df


# COMMAND ----------

schema = 'oni/mongodb_monitor'
table = 'investimento'
upload_file(spark=spark, dbutils=dbutils, df=spark.createDataFrame(await process_and_save()), schema=schema, table=table)
