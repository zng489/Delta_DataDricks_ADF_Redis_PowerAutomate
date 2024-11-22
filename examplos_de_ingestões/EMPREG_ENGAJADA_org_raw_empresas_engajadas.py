# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

file = {"namespace":"/oni", "file_folder":"/observatorio_nacional", 
        "file_subfolder":"/empresas_engajadas", 
        "raw_path":"/usr/oni/observatorio_nacional/plataforma_produtividade/empresas_engajadas/",
        "prm_path": "", 
        "extension":"csv","column_delimiter":"","encoding":"iso-8859-1","null_value":""}
        
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }

# COMMAND ----------


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
from core.string_utils import normalize_replace
from pyspark.sql.functions import concat, lit, col

# COMMAND ----------

file = notebook_params.var_file
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
adl_uld = f"{var_adls_uri}{uld_path}"
print(adl_uld)

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
print(adl_raw)

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
print(prm_path)

# COMMAND ----------

if not cf.directory_exists(dbutils, uld_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % uld_path)


def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())

# COMMAND ----------

import asyncio
import re
from pyspark.sql import functions as f
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BinaryType, BooleanType, ArrayType, MapType


# Assuming cf, dbutils, spark, uld_path, var_adls_uri, prm_path, __normalize_str, and headers are already defined.

# Define the async function to list subdirectories
async def list_subdirectories(path):
    # Assuming cf.list_subdirectory can be modified to work asynchronously
    return cf.list_subdirectory(dbutils, path)

# Define an async function to process files
async def process_file(file_path, path, var_adls_uri, prm_path):
    if file_path.endswith('.txt'):
        print(f'Exploring subdirectory: {file_path}')
        
        # Extract numbers from path
        numbers_list = re.findall(r'\d+', path)
        numbers = ''.join(numbers_list)

        # Read the CSV file into a Spark DataFrame (this part is still blocking)
        df = (spark.read
              .option("delimiter", ";")
              .option("header", "true")
              .option("encoding", "ISO-8859-1")
              .csv(f"{var_adls_uri}/{file_path}")
        )

        headers = {'name_header': 'Campo Origem', 'pos_header': 'C', 'pos_org': 'C', 'pos_dst': 'E', 'pos_type': 'F'}

        print("It's set to '2018' as the default for all years.")
        var_prm_dict = cf.parse_ba_doc_spark(dbutils, prm_path, headers=headers, sheet_names=['2018'])

        for column in df.columns:
            df = df.withColumnRenamed(column, __normalize_str(column))

        # Select and transform columns
        def __select(parse_ba_doc, sheet_name):
            for org, dst, _type in parse_ba_doc[sheet_name]:
                if org == 'N/A' and dst not in df.columns:
                    yield f.lit(None).cast(_type).alias(dst)
                else:
                    _col = f.col(org)
                    if _type.lower() == 'double':
                        _col = f.regexp_replace(org, ',', '.')
                    yield _col.cast(_type).alias(dst)

        print(numbers)
        df = df.select(*__select(var_prm_dict, '2018'))
        df = df.withColumn("ANO", lit(f"{numbers}"))
        
        return df
    else:
        print(f'There is no .txt file in {path}')
        return None

# Main async function to handle all tasks
async def main():
    dataframes = []
    
    # List subdirectories asynchronously
    paths = await list_subdirectories(uld_path)
    
    # Process each path asynchronously
    tasks = []
    for path in paths:
        file_paths = await list_subdirectories(path)  # List files in the subdirectory asynchronously
        
        for file_path in file_paths:
            # Process each file asynchronously
            tasks.append(process_file(file_path, path, var_adls_uri, prm_path))
    
    # Gather results from all tasks
    results = await asyncio.gather(*tasks)

    # Filter out None values (if there were no .txt files)
    dataframes = [df for df in results if df is not None]
    
    return dataframes

# Run the asynchronous main function
dataframes = asyncio.run(main())



# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BinaryType, BooleanType, ArrayType, MapType

dataframes = []
for path in cf.list_subdirectory(dbutils, uld_path):
  for file_path in cf.list_subdirectory(dbutils, path):
    for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
      if arquivos_txt.endswith('.csv'):
        print(f'  Explorando subdiretório: {arquivos_txt}')
        df = (spark.read
                      .option("delimiter", ";")
                      .option("header", "true")
                      .option("encoding", "utf-8")
                      .csv(f"{var_adls_uri}/{file_path}")
                      )

        for column in df.columns:
          df = df.withColumnRenamed(column, __normalize_str(column))
        dataframes.append(df)
      else:
        print(f'There is no file txt in {path}') 

# COMMAND ----------

if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

final_df.display()

# COMMAND ----------

df = cf.append_control_columns(final_df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
dh_insercao_raw = datetime.datetime.now()
df = df.withColumn('dh_insercao_raw', f.lit(dh_insercao_raw).cast('timestamp'))

df.write.partitionBy('ANO').parquet(path=adl_raw, mode='overwrite')
