dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')



from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from trs_control_field import trs_control_field as tcf
import pyspark.sql.functions as f
import crawler.functions as cf#
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


df = spark.read.format('parquet').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/bases_referencia/cnae/cnae_21')



uld_path = "trs/oni/midr/"
for path in cf.list_subdirectory(dbutils, uld_path):
  print(path)