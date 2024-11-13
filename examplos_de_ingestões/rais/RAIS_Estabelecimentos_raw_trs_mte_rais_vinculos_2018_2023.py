# Databricks notebook source
from cni_connectors import adls_connector as adls_conn
var_adls_uri, notebook_params = adls_conn.connect_adls()

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
import os
from core.string_utils import normalize_replace

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
trusted = dls['folders']['trusted']
prm_path = tables['prm_path']

# COMMAND ----------

raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
adl_raw

# COMMAND ----------

trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
adl_trusted

# COMMAND ----------

df = spark.read.parquet(adl_raw)

# COMMAND ----------


headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path, headers=headers, sheet_names='TRS')


def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)


df = df.select(*__select(var_prm_dict, 'TRS'))

# COMMAND ----------

df.filter(df["ANO"] == 2018).display()
df.filter(df["ANO"] == 2019).display()
df.filter(df["ANO"] == 2020).display()
df.filter(df["ANO"] == 2021).display()
df.filter(df["ANO"] == 2022).display()
df.filter(df["ANO"] == 2023).display()

# COMMAND ----------

df = tcf.add_control_fields(df, adf)
df.write.partitionBy('ANO').parquet(path=adl_trusted, mode='overwrite')
