# Biblioteca cni_connectors, que dá acesso aos dados no datalake
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
adl_uld

raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
adl_raw

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
prm_path

adl_raw

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

from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BinaryType, BooleanType, ArrayType, MapType
import re
from pyspark.sql import DataFrame

def load_csv_to_dataframe(file_path: str) -> DataFrame:
    return (spark.read
                .option("delimiter", ";")
                .option("header", "true")
                .option("encoding", "utf-8")
                .csv(file_path))

def process_files(base_path: str, parameter: str):
    dataframes = []  
    for path in cf.list_subdirectory(dbutils, base_path):
        for file_path in cf.list_subdirectory(dbutils, path):
            for arquivos_txt in cf.list_subdirectory(dbutils, file_path):
                if arquivos_txt.endswith('.csv') and parameter in file_path:
                    df = load_csv_to_dataframe(f"{var_adls_uri}/{arquivos_txt}")
                    #print('*************************************')
                    #print(parameter)
                    #print(df.columns)
                    #print(df.count())
                    dataframes.append(df)
    if dataframes:
        final_df = dataframes[0]
        for df in dataframes[1:]:
            final_df = final_df.unionByName(df, allowMissingColumns=True)
        return final_df
    else:
        print("No CSV files found.")
        return None

# COMMAND ----------

from collections import Counter
import datetime

parameters = ['FCO/Carteira', 'FCO/Contratações', 'FCO/Desembolsos', 'FNE/Carteira', 'FNE/Contratações', 'FNE/Desembolsos', 'FNO/Carteira', 'FNO/Contratações', 'FNO/Desembolsos']

for parameter in parameters:
    df = process_files(uld_path, parameter)
    for column in df.columns:
        df = df.withColumnRenamed(column, __normalize_str(column))


    cols = df.schema.names
    df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
    for i in range(len(cols)):
        if cols.count(cols[i])>1:
            df.schema[i].name = cols[i]+'_dup'
            cols[i] = cols[i]+'_dup'
    output = spark.createDataFrame([], df.schema).union(df)


    #print(f'{adl_raw}{parameter}')
    #print(output.columns)
    output.write.parquet(path=f'{adl_raw}{parameter}', mode='overwrite')