# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, explode, lit
import json

df = spark.read.option("multiline", "true") \
    .json('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/uld/oni/observatorio_nacional/pipelines_adf/all_pipelines.json')

df.createOrReplaceTempView("table_1")

# COMMAND ----------

# result_ = spark.sql("SELECT properties.activities[1] FROM (SELECT * FROM table_1 WHERE name LIKE 'lnd_org_raw%' AND name NOT LIKE 'wkf%')")
# result_.display()

# result_ = spark.sql("SELECT DISTINCT properties.activities.name FROM table_1 WHERE array_contains(properties.activities.type, 'Inactive') = False")
# display(result_)

# COMMAND ----------

result_ = spark.sql("SELECT * FROM table_1 WHERE name LIKE 'lnd%' AND name NOT LIKE 'wkf%'")
result_.display()

# COMMAND ----------

result_ = spark.sql("SELECT * FROM table_1 WHERE name LIKE 'org_raw%' AND name NOT LIKE 'wkf%'")
result_.display()

# COMMAND ----------

#result_ = spark.sql("SELECT * FROM table_1 WHERE name LIKE 'raw_trs%' AND name NOT LIKE 'wkf%'")
#result_.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # lnd crawlers

# COMMAND ----------

lnd = spark.sql("""
    SELECT etag AS ETAG, name AS NAME_PIPELINE, properties.parameters.bot.defaultValue AS BOT,
           properties.folder.name AS FOLDER_NAME,
           CASE 
               WHEN array_contains(properties.activities.name, 'alerta_falha') THEN 'alerta_falha'
               ELSE NULL
           END AS ACTIVITY_STATUS
    FROM table_1 
    WHERE name LIKE 'lnd_org_raw%' 
      AND name NOT LIKE 'wkf%'
""")
lnd.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # org_raw

# COMMAND ----------

try:
    # Drop the existing table
    spark.sql("DROP TABLE IF EXISTS table_raw")
  
    # Create the new table
    # Define a list of specific pipelines to exclude (if needed)
    excluded_pipelines = ['']  # Populate this list if you have specific names to exclude

    # Define a list of folder patterns to exclude
    excluded_folders = [
        'raw/cda/%',
        'raw/test/%',
        'raw/usr/power_bi%',
        'raw/usr/sti%',
        'test/raw/%',
        'raw/usr/devops%',
        'raw/usr/azure_ad%',
        'raw/usr/datalake%',
        'raw/usr/time_sheet%',
        'raw/usr/unimercado%',
        'raw/usr/unigest%',
        'raw/usr/jira_service_management%']

    # Build the NOT LIKE conditions for folder exclusions
    not_like_conditions = ' AND '.join([f"properties.folder.name NOT LIKE '{pattern}'" for pattern in excluded_folders])
    # properties.parameters.databricks.defaultValue AS FOLDER_NOTEBOOK,
    spark.sql(f"""
        CREATE TABLE table_raw AS
        SELECT properties, etag AS ETAG, 
              name AS PIPELINES, 
              get_json_object(properties.parameters.databricks.defaultValue, '$.notebook') AS NOTEBOOKS,
              properties.folder.name AS FOLDERS,
              CASE 
                  WHEN array_contains(properties.activities.name, 'alerta_falha') THEN 'alerta_falha'
                  ELSE NULL
              END AS ACTIVITY_STATUS
        FROM table_1 
        WHERE name LIKE 'org_raw%' 
          AND name NOT LIKE 'wkf%'
          AND name NOT IN ({', '.join(f"'{pipeline}'" for pipeline in excluded_pipelines)})  -- This will do nothing if the list is empty
          AND properties.folder.name NOT LIKE 'raw/bdo/%'
          AND {not_like_conditions}
    """)
    ##org.display()
except Exception as e:
    print(f"An error occurred: {e}")



# COMMAND ----------

raw_usr = spark.sql("SELECT * FROM table_raw WHERE FOLDERS LIKE 'raw/usr%' ")
raw_usr.display()

# COMMAND ----------

raw_crw = spark.sql("SELECT * FROM table_raw WHERE FOLDERS LIKE 'raw/crw%' ")
raw_crw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # raw_trs

# COMMAND ----------

try:
    # Drop the existing table
    spark.sql("DROP TABLE IF EXISTS table_trs")
  
    # Create the new table
    # Define a list of specific pipelines to exclude (if needed)
    excluded_pipelines = ['']  # Populate this list if you have specific names to exclude

    # Define a list of folder patterns to exclude
    excluded_folders = ['']

    # Build the NOT LIKE conditions for folder exclusions
    not_like_conditions = ' AND '.join([f"properties.folder.name NOT LIKE '{pattern}'" for pattern in excluded_folders])
    # properties.parameters.databricks.defaultValue AS FOLDER_NOTEBOOK,
    spark.sql(f"""
        CREATE TABLE table_trs AS
        SELECT properties, etag AS ETAG, 
              name AS PIPELINES, 
              get_json_object(properties.parameters.databricks.defaultValue, '$.notebook') AS NOTEBOOKS,
              properties.folder.name AS FOLDERS,
              CASE 
                  WHEN array_contains(properties.activities.name, 'alerta_falha') THEN 'alerta_falha'
                  ELSE NULL
              END AS ACTIVITY_STATUS
        FROM table_1 
        WHERE name LIKE 'raw_trs%' 
          AND name NOT LIKE 'wkf%'
          AND name NOT IN ({', '.join(f"'{pipeline}'" for pipeline in excluded_pipelines)})  -- This will do nothing if the list is empty
          AND properties.folder.name NOT LIKE 'raw/bdo/%'
          AND {not_like_conditions}
    """)
    ##org.display()
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

raw_trs = spark.sql("SELECT * FROM table_trs")
raw_trs.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # trs_biz and biz_biz

# COMMAND ----------

#spark.sql("""
#  SELECT get_json_object(properties.parameters.tables.defaultValue, "$.databricks.notebook") AS NOTEBOOKS, etag AS ETAG, name AS NAME_PIPELINE, properties.activities AS BOT FROM table_1 
#  WHERE (name LIKE 'trs_biz%' OR name LIKE 'biz_biz%')
#    AND name NOT LIKE 'wkf%'
#""").display()

# COMMAND ----------

try:
    # Drop the existing table
    spark.sql("DROP TABLE IF EXISTS table_biz")
  
    # Create the new table
    # Define a list of specific pipelines to exclude (if needed)
    excluded_pipelines = ['']  # Populate this list if you have specific names to exclude

    # Define a list of folder patterns to exclude
    excluded_folders = ['']

    # Build the NOT LIKE conditions for folder exclusions
    not_like_conditions = ' AND '.join([f"properties.folder.name NOT LIKE '{pattern}'" for pattern in excluded_folders])
    # properties.parameters.databricks.defaultValue AS FOLDER_NOTEBOOK,
    spark.sql(f"""
        CREATE TABLE table_biz AS
        SELECT properties, etag AS ETAG, 
              name AS PIPELINES, 
              get_json_object(properties.parameters.tables.defaultValue, "$.databricks.notebook") AS NOTEBOOKS,
              properties.folder.name AS FOLDERS,
              CASE 
                  WHEN array_contains(properties.activities.name, 'alerta_falha') THEN 'alerta_falha'
                  ELSE NULL
              END AS ACTIVITY_STATUS
        FROM table_1 
        WHERE (name LIKE 'trs_biz%' OR name LIKE 'biz_biz%')
          AND name NOT LIKE 'wkf%'
          AND name NOT IN ({', '.join(f"'{pipeline}'" for pipeline in excluded_pipelines)})  -- This will do nothing if the list is empty
          AND properties.folder.name NOT LIKE 'raw/bdo/%'
          AND {not_like_conditions}
    """)
    ##org.display()
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

biz = spark.sql("SELECT * FROM table_biz")
biz.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# !pip install fsspec

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, explode, lit
import json


df = spark.read.option("multiline", "true") \
    .json('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/uld/oni/observatorio_nacional/pipelines_adf/all_pipelines.json')

# Create SparkSessionspark = SparkSession.builder.appName("JSON Processing").getOrCreate()# Assuming json_string is already loaded as a DataFrame# If not, you'll need to load it first:# 
# 
def process_json_pyspark(df, layer):
  if isinstance(df.schema[0].dataType, ArrayType):
    df = df.select(explode(col("*")).alias("data")).select("data.*")
    result_df = df.filter(        (col("name").isNotNull()) &        (col("name").startswith(layer)) &        (~col("name").startswith("wkf"))    ).select(        col("name").alias("NAME_PIPELINE"),        col("properties.folder.name").alias("FOLDER"),        col("properties.parameters.bot.defaultValue").alias("BOT_SCRIPT_NAME"),        explode("properties.activities").alias("activity")    ).withColumn(        "MONITORAMENTO",        when(            (col("activity.name") == "alerta_falha") &            (col("activity.typeProperties.parameters.email_users.value").isNotNull()),            col("activity.typeProperties.parameters.email_users.value")        ).otherwise(lit("Não existe"))    ).select(        "NAME_PIPELINE",        "FOLDER",        "BOT_SCRIPT_NAME",        "MONITORAMENTO"    ).distinct()    
    return print(result_df)



# COMMAND ----------

process_json_pyspark(df, "lnd_org_raw")

# COMMAND ----------

# Filter the DataFrame
filtered_df = df.filter(     (col("name").startswith(layer)) &     (~col("name").startswith("wkf")) ) 
# Explode the activities array
exploded_df = filtered_df.withColumn("activity", explode(col("properties.activities"))) 
# Extract relevant fields and set default value for email_users
result_df = exploded_df.select(     col("name").alias("name_pipeline"),     col("properties.folder.name").alias("folder"),     col("properties.parameters.bot.defaultValue").alias("bot_script"),     when(col("activity.name") == "alerta_falha", col("activity.typeProperties.parameters.email_users.value")).otherwise("Não existe").alias("email_users") ) 
# Show the results 
result_df.show(truncate=False) 
# If you need to collect the results into a list 
# result_list = result_df.collect() for row in result_list: print(row

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, explode, lit
import json


df = spark.read.option("multiline", "true") \
    .json('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/uld/oni/observatorio_nacional/pipelines_adf/all_pipelines.json')

# Assuming 'properties' is a struct that contains an array field named 'activities' you want to explode
#print(df.count())
#display(df)
layer = "lnd_org_raw"

filtered_df = df.filter(
  (col("name").startswith(layer)) & (~col("name").startswith("wkf"))
)
exploded_df = filtered_df.withColumn("activity", explode(col("properties.activities")))
exploded_df = exploded_df.dropDuplicates(["id", "name"])

result_df = exploded_df.select(     col("name").alias("name_pipeline"),     col("properties.folder.name").alias("folder"),     col("properties.parameters.bot.defaultValue").alias("bot_script"),     when(col("activity.name") == "alerta_falha", col("activity.typeProperties.parameters.email_users.value")).otherwise("Não existe").alias("email_users") )

# COMMAND ----------

display(result_df)

