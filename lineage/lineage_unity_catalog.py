# Databricks notebook source
# sales_id,customer_name,amount,date
# 1,John Doe,100,2023-11-01
# 2,Jane Smith,200,2023-11-02



# dbutils.fs.mount(
#     source="s3a://your-bucket/sales-data/",
#     mount_point="/mnt/sales-data",
#     extra_configs={"fs.s3a.access.key": "your-access-key", "fs.s3a.secret.key": "your-secret-key"}
# )



# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

# Load the CSV file
# raw_data = spark.read.csv("/mnt/sales-data/sales_raw.csv", header=True, inferSchema=True)

# Save to Unity Catalog
# raw_data.write.format("delta").saveAsTable("sales_catalog.raw_sales_data")

# COMMAND ----------

# Example data in the form of a list of tuples
data = [("Alice", 29), ("Bob", 31), ("Cathy", 23)]

# Create a DataFrame using the data and column names
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
# df.show()

df.write.format("delta").saveAsTable("oni_lab.1_2_3.sdao")

# COMMAND ----------

transformed_data = spark.sql("""
    SELECT *
    FROM oni_lab.1_2_3.sdao
""")

# Save the transformed data
transformed_data.write.format("delta").saveAsTable("oni_lab.1_2_3.transformed_sales_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW oni_lab.1_2_3.sales_report AS
# MAGIC SELECT *
# MAGIC FROM oni_lab.1_2_3.transformed_sales_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CREATE SCHEMA IF NOT EXISTS 1_2_3;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS 1_2_3 CASCADE;

# COMMAND ----------

