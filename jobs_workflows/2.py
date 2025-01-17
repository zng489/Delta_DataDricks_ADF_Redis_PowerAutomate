# Databricks notebook source
# Task B: Retrieve the value from Task A
my_value = dbutils.jobs.taskValues.get(taskKey='1', key='my_key', default='default_value', debugValue=0)
print(my_value)  # This will print 'key_value' if Task A set it, or 'default_value' if it wasn't set.

# COMMAND ----------

# Task A: Store a value in the parent job
dbutils.jobs.taskValues.set(key='Q', value='W')

# COMMAND ----------

# This is a simple Python script that prints a message to the console
print("Testing")

# COMMAND ----------

