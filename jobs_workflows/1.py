# Databricks notebook source
#Setting task values in the Parent_Job task
# dbutils.jobs.taskValues.set(key = 'child1-country', value = 'France')
# dbutils.jobs.taskValues.set(key = 'child2-country', value = 'Germany')

#Getting task value key in child1 task
# dbutils.jobs.taskValues.get(taskKey = "parent_job", key = "child1-country", default = 'Europe',debugValue = 0)

#Getting task value key in Child2 task
# dbutils.jobs.taskValues.get(taskKey = "parent_job", key = "child2-country", default = 'Europe',debugValue = 0)

# COMMAND ----------

# Task A: Store a value in the parent job
dbutils.jobs.taskValues.set(key='my_key', value='key_value')

# COMMAND ----------


# Make a Directory:
dbutils.fs.mkdirs("dbfs:/unity_catalog")

# COMMAND ----------

# Example data in the form of a list of tuples
data = [("Alice", 29), ("Bob", 31), ("Cathy", 23)]

# Create a DataFrame using the data and column names
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
# df.show()

# COMMAND ----------

# df.coalesce(1).write.format('csv').save('dbfs:/unity_catalog/', header = True, mode='overwrite')
# df.coalesce(1).write.format('csv').save('/Volumes/oni_lab/default/uds_oni_observatorio_nacional/unity_catalog', header = True, mode='overwrite')
df.write.csv('/Volumes/oni_lab/default/uds_oni_observatorio_nacional/unity_catalog', mode='overwrite')

# COMMAND ----------

# This is a simple Python script that prints a message to the console
print("Hello, World!")


# COMMAND ----------

# MAGIC %md
# MAGIC