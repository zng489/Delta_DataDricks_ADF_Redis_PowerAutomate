# Databricks notebook source
# Task 1

# Example data in the form of a list of tuples
data = [("Alice", 29), ("Bob", 31), ("Cathy", 23)]

# Create a DataFrame using the data and column names
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.display()

# COMMAND ----------

