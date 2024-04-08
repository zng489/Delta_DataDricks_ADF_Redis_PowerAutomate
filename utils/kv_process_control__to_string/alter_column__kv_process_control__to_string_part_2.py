# Databricks notebook source
# MAGIC %md
# MAGIC # Alter column kv_process_control to string
# MAGIC 
# MAGIC ## Why?
# MAGIC This is required for the usage with catalog tool Azure purview. Currently \(december 2022\) it cannot deal with complex types (json, for example).
# MAGIC 
# MAGIC 
# MAGIC ------
# MAGIC 
# MAGIC ## How to do it?
# MAGIC This implementation inherits a lot from [bigdatadlsreporter](https://dev.azure.com/CNI-STI/ENSI-BIG%20DATA/_git/ENSI-BIGDATADLSREPORTER).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Part 2 : rewrite tables altering kv_process_control_ column to string
# MAGIC 
# MAGIC #### Step1:
# MAGIC Well, backup all the tables. Never change anything in data without a good backup

# COMMAND ----------

import os
import json
import logging
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

def json_to_object(layer:str) -> list:
  with open(f"/dbfs/temp/alter__kv_process_control__{layer}__tables.json", "r") as f:
    return json.loads(f.read())

# COMMAND ----------

# DBTITLE 1,Important definitions
var_path_bkp_base = "tmp/dev/bkp_kv_process_control"

# COMMAND ----------

# DBTITLE 1,cni_connectors
# ADLS Connect (DON'T KEEP THIS ON IN THE FINAL VERSION)
scope = 'adls_gen2'
var_adls_uri = dbutils.secrets.get(scope=scope, key="adls_uri")
store_name = dbutils.secrets.get(scope=scope, key="store_name")

spark.conf.set(
    "fs.azure.account.auth.type.%s.dfs.core.windows.net" % store_name, "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.%s.dfs.core.windows.net" % store_name,
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.%s.dfs.core.windows.net" % store_name,
    dbutils.secrets.get(scope=scope, key="client_id"),
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.%s.dfs.core.windows.net" % store_name,
    dbutils.secrets.get(scope=scope, key="service_credential"),
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.%s.dfs.core.windows.net" % store_name,
    "https://login.microsoftonline.com/%s/oauth2/token"
    % dbutils.secrets.get(scope=scope, key="directory_id"),
)

# COMMAND ----------

# DBTITLE 1,backup procedure
def backup_tables(var_tables:list, path_source:str = "", path_destination:str = var_path_bkp_base) -> None:
  for i in var_tables:

    table = os.path.join(var_adls_uri, path_source, i["name"])
    #table = os.path.join("/", path_source, i["name"])
    var_path_bkp_table = os.path.join(var_adls_uri, path_destination, i["name"])
    var_has_kv_column = False

    logging.warning(
        f"====================\nTable to write to destination: '{table}' -> '{var_path_bkp_table}'"
    )
    try:
      df_source = (
        spark
        .read
        .format(i["format"])
        .load(os.path.join(table))
      )
    except AnalysisException:
      logging.warning(f"table does not have kv_process_control, so was not backuped. SKIPPING...")
      continue

    var_source_columns = df_source.columns

    # Log column types
    logging.warning(f"dtypes: {df_source.dtypes}")

    # Cast column kv_process_control to string. If has the column, then we must change it. Else, skip it.
    if "kv_process_control" in var_source_columns:
      var_has_kv_column = True
      logging.warning(f"Found kv_process_control column in table: '{table}'")
      df_source = df_source.withColumn("kv_process_control", col("kv_process_control").cast("string"))
    else:
      logging.warning(f"Not found kv_process_control column in table: '{table}'. SKIPPING TABLE...")
      continue

    # Count rows if we've got kv_process_control
    var_count_origin = df_source.count()

    # Write backup
    # Partitions must be evaluated in this case
    if "partitions" in i:
      partitions = i["partitions"]
      logging.warning(f"Table '{table}' is partitioned by: '{partitions}'")
      (
        df_source
        .write
        .partitionBy(*i["partitions"])
        .save(path=var_path_bkp_table, format=i["format"], mode="overwrite")
      )
    else:
      (
        df_source
        .write
        .save(path=var_path_bkp_table, format=i["format"], mode="overwrite")
      )

    df_bkp = (
      spark
      .read
      .format(i["format"])
      .load(var_path_bkp_table)
    )

    # ### TESTS ###
    # Test counts
    var_count_bkp = df_bkp.count()
    logging.warning(f"Testing counts for backup operation. Before='{var_count_origin}'; After='{var_count_bkp}'")
    assert var_count_origin == var_count_bkp, "Row Counts differ after backup"

    # Test number of columns
    var_len_src_cols = len(var_source_columns)
    var_len_bkp_cols =len(df_bkp.columns)
    logging.warning(f"Testing number of columns for backup operation. Before='{var_len_src_cols}'; After='{var_len_bkp_cols}'")
    assert var_len_src_cols == var_len_bkp_cols, "Columns count differ after backup"

    #Test kv_process_control in string
    if var_has_kv_column is True:
      var_type_kv_column_bkp = df_bkp.select("kv_process_control").dtypes[0][1]
      logging.warning(f"Testing kv_process_control type after backup. type='{var_type_kv_column_bkp}'")
      assert var_type_kv_column_bkp == "string", "Column kv_process control is not type string after backup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write to backup area

# COMMAND ----------

var_raw_tables = json_to_object("raw")
backup_tables(var_raw_tables)

# COMMAND ----------

var_trs_tables = json_to_object("trs")
backup_tables(var_trs_tables)

# COMMAND ----------

var_biz_tables = json_to_object("biz")
backup_tables(var_biz_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overwrite production area

# COMMAND ----------

var_biz_tables = json_to_object("biz")
backup_tables(var_tables=var_biz_tables, path_source=var_path_bkp_base, path_destination="")

# COMMAND ----------

var_trs_tables = json_to_object("trs")
backup_tables(var_tables=var_trs_tables, path_source=var_path_bkp_base, path_destination="")

# COMMAND ----------

var_raw_tables = json_to_object("raw")
backup_tables(var_tables=var_raw_tables, path_source=var_path_bkp_base, path_destination="")

# COMMAND ----------


