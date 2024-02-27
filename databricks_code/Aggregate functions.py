# Databricks notebook source
from pyspark.sql.functions import count, countDistinct, sum

races_df.filter("name = 'Chinese Grand Prix'").select(sum('round'), countDistinct("year")).show()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

races_df.filter("name = 'Chinese Grand Prix'").select(sum('round'), countDistinct("year")) \
.withColumnRenamed("sum(round)", "total_round") \
.withColumnRenamed("count(DISTINCT name)", "number_of_name").show()

# COMMAND ----------

races_df.display()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

#// Convert String to Integer Type
#df.withColumn("salary",col("salary").cast(IntegerType))
#df.withColumn("salary",col("salary").cast("int"))
#df.withColumn("salary",col("salary").cast("integer"))
#
#// Using select
#df.select(col("salary").cast("int").as("salary"))
#
#//Using selectExpr()
#  df.selectExpr("cast(salary as int) salary","isGraduated")
#  df.selectExpr("INT(salary)","isGraduated")
#
#//Using with spark.sql()
#spark.sql("SELECT INT(salary),BOOLEAN(isGraduated),gender from CastExample")
#spark.sql("SELECT cast(salary as int) salary, BOOLEAN(isGraduated),gender from CastExample")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

races_df = races_df.withColumn("round",col("round").cast("integer"))
# AnalysisException: "raceId" is not a numeric column. Aggregation function can only be applied on a numeric column.;

races_df.display()
races_df.printSchema()

# COMMAND ----------

races_df.groupBy("name").sum('round').show()

# COMMAND ----------

races_df\
.groupBy("name")\
.agg(sum("round")).show()

# COMMAND ----------

races_df\
.groupBy("name")\
.agg(sum("round"), countDistinct("name")).show()

# COMMAND ----------

# MAGIC %md Window Functions

# COMMAND ----------

demo_df = races_df.filter("year in (2018, 2019, 2020)")
demo_df.display()

# COMMAND ----------

demo_df = demo_df.filter("year in (2018, 2019, 2020)")

demo_grouped_df = demo_df.groupBy("year","name") \
.agg(sum("round").alias("round_column"), countDistinct("raceId").alias("raceId_column"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("year").orderBy(desc("raceId"))

demo_grouped_df.withColumn('Rank', rank().over(driverRankSpec)).show(100)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

races_df = spark.read.option('header', True).csv(f"{raw_folder_path}/races.csv")
races_df.display()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import IntegerType
races_df = races_df.withColumn("round", races_df["round"].cast(IntegerType()))
races_df = races_df.withColumn("year", races_df["year"].cast(IntegerType()))
races_df.printSchema()

# COMMAND ----------

The pivot() method returns a GroupedData object, just like groupBy(). You cannot use show() on a GroupedData object without using an aggregate function (such as sum() or even count()) on it before.

# COMMAND ----------

driver_standing = races_df \
.groupBy("name").pivot("year").count().display()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import sum

driver_standing = races_df \
.groupBy("name").agg(sum("round").alias("soma_dos_round"))

display(driver_standing)
#.groupBy("name","group_1","group_2")
#.agg => "(" sum("round").alias("soma_dos_round") ")" <= 

# COMMAND ----------

from pyspark.sql.functions import sum
display(driver_standing.filter("name = 'Luxembourg Grand Prix'"))

# COMMAND ----------

races_df.display()

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

driver_standing = races_df \
.groupBy("name").agg(sum("round").alias("soma_dos_round"), count(when(col("year") == "2009", True)).alias("ano_vencida"))

#.groupBy("name","group_1","group_2")
#.agg => "(" sum("round").alias("soma_dos_round") ")" <= 

# COMMAND ----------

display(driver_standing)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("ano_vencida").orderBy(desc("name") #, desc("column_name"))