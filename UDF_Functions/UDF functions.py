# Databricks notebook source
In PySpark, UDFs (User Defined Functions) allow you to define custom functions that can be applied to DataFrame columns. Here are some types of functions you can create using UDFs, including the ones you've mentioned:

1. Scalar UDFs
These take a single value as input and return a single value. For example, a function to convert temperatures from Celsius to Fahrenheit.
2. Vectorized UDFs
These operate on a whole column of data at once (e.g., using NumPy), which can be more efficient than scalar UDFs. They process arrays or Series and return arrays.
3. Pandas UDFs
Introduced in PySpark 2.3, these are optimized for performance and allow you to use Pandas functions on PySpark DataFrames. They can operate on batches of data, making them faster for larger datasets.
4. Grouped Map UDFs
These take a DataFrame as input and return a DataFrame. They're useful for performing operations that need to consider groups of data, like aggregating or transforming based on a grouping column.
5. Grouped Aggregation UDFs
These can compute aggregate values over a group of rows, similar to SQL’s GROUP BY clause.
6. Windowed UDFs
UDFs can also be applied within window functions, allowing for computations that consider a set of rows defined by certain criteria.
7. Built-in Functions
While not UDFs, you can also use built-in functions available in the pyspark.sql.functions module, which are optimized for performance.

8. UDF SQL
In Apache Spark, a User Defined Function (UDF) allows you to define custom functions in Python (or other languages like Scala and Java) that can be used in Spark SQL queries. UDFs are particularly useful when you need to perform operations that are not available in Spark's built-in functions.
UDF SQL

# COMMAND ----------

def test():
  '''
  test function
  '''
  pass

# COMMAND ----------

test?

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import size
from pyspark.sql.types import DoubleType, IntegerType, StringType

# COMMAND ----------

def string_length(s):
    if s is not None:
        return len(s)
    return 0

length_udf = udf(string_length, IntegerType())

# COMMAND ----------

data = [("Alice",), ("Bob",), ("Catherine",)]
df = spark.createDataFrame(data, ["name"])

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

'''
def string_length(s):
    if s is not None:
        return len(s)
    return 0

length_udf = udf(string_length, IntegerType())
'''

df_with_length = df.withColumn("name_length", length_udf(df["name"]))
df_with_length.show()

# COMMAND ----------

spark.stop()

# COMMAND ----------



# COMMAND ----------

# Step 3: Define a Pandas UDF
@pandas_udf(StringType())
def to_uppercase(name_series):
    return name_series.str.upper()

# Step 4: Apply the Pandas UDF to the DataFrame
result_df = df.withColumn("uppercase_name", to_uppercase(df.name))

# Show the result
result_df.show()

# COMMAND ----------



# COMMAND ----------

def my_custom_function(value):
    return value.upper()  # Example: Convert string to uppercase

# COMMAND ----------

my_udf = udf(my_custom_function, StringType())

# COMMAND ----------

data = [("hello",), ("world",)]
df = spark.createDataFrame(data, ["words"])

# Using the UDF with DataFrame
df_with_udf = df.withColumn("upper_words", my_udf(df["words"]))
df_with_udf.show()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define your UDF function in Python
def my_udf_function(words):
    return words.upper()

# Register the UDF with Spark
spark.udf.register("my_udf", my_udf_function, StringType())

# Now you can use your UDF in Spark SQL
df.createOrReplaceTempView("words_table")
spark.sql("SELECT words, my_udf(words) AS upper_words FROM words_table").show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

# COMMAND ----------

@pandas_udf(DoubleType())
def my_pandas_udf(s: pd.Series) -> pd.Series:
    return s * 2

df.withColumn("new_column", my_pandas_udf(df["existing_column"]))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Partition Hints Example") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 23), ("David", 36),
        ("Eve", 29), ("Frank", 31), ("Grace", 25), ("Hannah", 41),
        ("Ivy", 38), ("Jack", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Initial repartitioning to 10 partitions
df = df.repartition(10)

# 1. Coalesce
# Reducing the number of partitions to 3
coalesced_df = df.coalesce(3)
print("Coalesced to 3 partitions:")
print(coalesced_df.rdd.getNumPartitions())

# 2. Repartition
# Increasing the number of partitions to 6
repartitioned_df = coalesced_df.repartition(6)
print("Repartitioned to 6 partitions:")
print(repartitioned_df.rdd.getNumPartitions())

# 3. Repartition by Range
# Repartitioning by age into 4 partitions
range_repartitioned_df = repartitioned_df.repartitionByRange(4, "Age")
print("Repartitioned by range into 4 partitions:")
print(range_repartitioned_df.rdd.getNumPartitions())

# 4. Rebalance (Note: Rebalance is more of a conceptual operation, in PySpark, it can be simulated by repartitioning)
# Using repartition to evenly distribute data across partitions
rebalance_df = range_repartitioned_df.repartition(6)
print("Rebalanced by repartitioning to 6 partitions:")
print(rebalance_df.rdd.getNumPartitions())

# Stop the Spark session
spark.stop()


# COMMAND ----------


