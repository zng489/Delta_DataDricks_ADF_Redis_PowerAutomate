# Databricks notebook source
# MAGIC %run "./includes/configuration"
# MAGIC
# MAGIC raw_folder_path
# MAGIC Out[12]: 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/'
# MAGIC   
# MAGIC races_df = spark.read.option('header', True).csv(f"{raw_folder_path}/races.csv")
# MAGIC display(races_df.sample(0.08))

# COMMAND ----------

# MAGIC %run "./includes/configuration"
# MAGIC
# MAGIC raw_folder_path = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing files as Pandas

# COMMAND ----------

import pandas as pd

ds = spark.createDataFrame(pd.read_csv("https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2020-financial-year-provisional/Download-data/annual-enterprise-survey-2020-financial-year-provisional-csv.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing JSON file

# COMMAND ----------

# Data Ingestion JSON
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read \
.schema(constructors_schema)\
.json("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/inteligencia_ocupacional/teste_de_dados/constructors.json")

display(constructor_df.sample(0.005))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Multilines values in JSON file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                  StructField("surname", StringType(), True)
                                  ])


driver_schema = StructType(fields = [StructField('driverId', IntegerType(), False),
                                    StructField('driverRef', StringType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('code', StringType(), True),
                                    StructField('name', name_schema),            # -----> other schema
                                    StructField('dob', DateType(), True),
                                    StructField('nationality', StringType(), True),
                                    StructField('url', StringType(), True)])

driver_df = spark.read \
.schema(driver_schema)\
.json("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/inteligencia_ocupacional/teste_de_dados/drivers.json")

display(driver_df.sample(0.008))

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, lit

drivers_with_column_df = driver_df.withColumnRenamed('driverId', 'driver_id') \
                                   .withColumnRenamed('driverRef', 'driver_ref') \
                                   .withColumn('ingestion_date', current_timestamp()) \
                                   .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

display(drivers_with_column_df.sample(0.09))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Multiline with partition files

# COMMAND ----------

# Multiline in JSON file.
# C:\Users\Yuan\Desktop\raw\raw\qualifying ==> qualifying.split_1.json, qualifying.split_2.json

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option('multiline', True) \
.json('/Users/Yuan/Desktop/raw/qualifying/')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # DataBricks WorkFlows

# COMMAND ----------

# Calling the function in the script

# COMMAND ----------

# MAGIC %run "./includes/function"

# COMMAND ----------

import pandas as pd

ds = spark.createDataFrame(pd.read_csv("https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2020-financial-year-provisional/Download-data/annual-enterprise-survey-2020-financial-year-provisional-csv.csv"))

display(ds.sample(0.08))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = ds.withColumn("ingestion_date", current_timestamp())

#from pyspark.sql.functions import current_timestamp
#def add_ingestion_date(input_df):
#  output_df = input_df.withColumn("ingestion_date", current_timestamp())
#  return output_df


# OR FROM THE CREATE FUNCTION add_ingestion_date
#%run "./includes/function"
#circuits_final_df = add_ingestion_date(circuits_renamed_df)

display(circuits_final_df.sample(0.09))

# COMMAND ----------

###################################################################

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

###################################################################

# COMMAND ----------

from pyspark.sql.functions import lit

data_source = circuits_final_df.withColumn("data_source", lit(v_data_source))
data_source.display()

# COMMAND ----------

########################################################################

# COMMAND ----------

dbutils.notebook.exit("Sucess")
# dbutils.notebook.run("DataBricks WorkFlow", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

# MAGIC %md 
# MAGIC # %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits_parquet")

display(spark.read.parquet("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits_parquet"))


#...parquet(f"{raw_folder+path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits_parquet")

display(spark.read.parquet("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits_parquet"))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # File one

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, lit


##################################################################
# LOADING
##################################################################
#>> data lake address
data_lake_address = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#>> path_of_file
path_of_file = "/uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits.csv"
path = '{data_lake}{file}'.format(data_lake = data_lake_address, file = path_of_file)
# path = abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits.csv

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), False),
                                     StructField("location", StringType(), False),
                                     StructField("country", StringType(), False),
                                     StructField("lat", DoubleType(), False),
                                     StructField("lng", DoubleType(), False),
                                     StructField("alt", IntegerType(), False),
                                     StructField("url", StringType(), False)
                                    ])

df = spark.read\
.format("csv")\
.option("header",True)\
.option("delimiter", ",")\
.schema(circuits_schema)\
.load(path)
  
display(df.sample(0.05)) 

# COMMAND ----------

display(df.describe())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Changing Type values of columns

# COMMAND ----------

df_1 = df \
.withColumn("lat", df["lat"].cast(IntegerType())) \
.withColumn("lng", df["lng"].cast(IntegerType()))

display(df_1.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Renaming columns

# COMMAND ----------

df_1.columns

# COMMAND ----------

df_2 = df_1.withColumnRenamed("circuitId", 'circuit_id').withColumnRenamed("circuitRef", 'circuit_ref')
# df_2 = df_1.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref")
# circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
#                                          .withColumnRenamed("circuitRef", "circuit_ref") \
#                                          .withColumnRenamed("lat", "latitude") \
#                                          .withColumnRenamed("lng", "longitude") \
#                                          .withColumnRenamed("alt", "altitude") \


df_2.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating new columns and adding new values
# MAGIC ### Using select,lit,current_timestamp()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

df_2 = df_2.withColumn("ingestion", current_timestamp()) \
.withColumn("time_of_ingestion", lit("time"))

display(df_2.sample(0.05))

# COMMAND ----------

from pyspark.sql.functions import col

# Selecting only the columns required
df_3  = df_2 .select(col('circuit_id'), col('circuit_ref'), col('name'), col('location'), col('country'))
# df.select("*")
# df = df.select(df.Year) 
# df = df.select(col("Year"))


# df = df.select(df.Year, df.Industry_name_NZSIOC)
# df = df.select(df['Year'],df['Industry_name_NZSIOC'])
# df = df.select(col("Year"), col("Variable_name"))

display(df_3.sample(0.04))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting column and the same time shifting the values and the column_name
# MAGIC ### df.select(col("raceId"),lit("1").alias("lit_value1"))

# COMMAND ----------

# alias() and lit()

from pyspark.sql.functions import col,lit

df_4 = df_3.select(col("name"),lit(df_3['name']).alias("Full_name"))  # lit put the values, alias chance the column name!!
df_4.show(truncate=False)

# Show full contents of DataFrame (PySpark)
# df.show(truncate=False)

# Show top 5 rows and full column contents (PySpark)
# df.show(5,truncate=False) 

# Shows top 5 rows and only 10 characters of each column (PySpark)
# df.show(5,truncate=10) 

# Shows rows vertically (one line per column value) (PySpark)
# df.show(vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping

# COMMAND ----------

df_2.lat

# COMMAND ----------

df_drop = df_2.drop('lat', 'lng', 'alt')
#df.drop(df.age)
# df_drop = df_2.drop("lat","lng")
# df_drop = df_2.drop(df_2["lat"])

# df2 = df.drop('Category', 'ID')
# df2.show()

# columns_to_drop = ['Category', 'ID']
# df3 = df.drop(*columns_to_drop)
# df3.show()

display(df_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC ### count()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

df_count = df.select(count("*"))
display(df_count)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

df_count_specific = df.select(count("country"))
display(df_count_specific)

# COMMAND ----------

# MAGIC %md
# MAGIC ### countDistinct()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

df_countDistinct = df.select(countDistinct("country"))
display(df_countDistinct)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### sum()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

df_round = df.select(sum("country"))
display(df_round)

# COMMAND ----------

display(df.sample(0.08))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### filter("column = 'name'").select(sum('column_name'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, lit


##################################################################
# LOADING
##################################################################
#>> data lake address
data_lake_address = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#>> path_of_file
path_of_file = "/uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits.csv"
path = '{data_lake}{file}'.format(data_lake = data_lake_address, file = path_of_file)
# path = abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net//uds/uniepro/inteligencia_ocupacional/teste_de_dados/circuits.csv

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), False),
                                     StructField("location", StringType(), False),
                                     StructField("country", StringType(), False),
                                     StructField("lat", DoubleType(), False),
                                     StructField("lng", DoubleType(), False),
                                     StructField("alt", IntegerType(), False),
                                     StructField("url", StringType(), False)
                                    ])

df = spark.read\
.format("csv")\
.option("header",True)\
.option("delimiter", ",")\
.schema(circuits_schema)\
.load(path)
  
display(df.sample(0.05)) 

# COMMAND ----------

df.columns # ======================> Lists of columns

# COMMAND ----------

# I wanna sum lat, lng, alt for every dataframe column
columns = ["lat","lng", "alt"]

for dataframe in df.columns: # df.columns = [circuitId, circuitRef, name, location, country, lat, lng, alt, url] # df.columns = lists of columns
  if 'lat' in dataframe: # if <string> 'lat' in  <list of strings> dataframe: 
    print('the columns exists in dataframe')
  elif 'lng' in dataframe:
    print('the columns exists in dataframe')
  elif 'alt' in dataframe:
    print('the columns exists in dataframe')
  else:
    print('Error there is no columns')

# COMMAND ----------

df_concat_3_values = df.select(concat(df.lat, df.lng, df.alt).alias('sum_of_3_values'))#.collect()
display(df_concat_3_values.sample(0.08))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Select All columns from List
df.select(*columns).show()

# Select All columns
df.select([col for col in df.columns]).show()
df.select("*").show()

# COMMAND ----------

df["lat"]

# COMMAND ----------

# I wanna sum lat, lng, alt for every dataframe column
columns = ["lat","lng", "lat"]

for dataframe in df.columns:
  df.select(dataframe).show()
  if dataframe == 
  
  
#  if dataframe_columns == x:
#    print(df.x)
#  else:
#    print('No')

# COMMAND ----------

import pandas as pd

def check(col):
   if col in df:
      print "Column", col, "exists in the DataFrame."
   else:
      print "Column", col, "does not exist in the DataFrame."

df = pd.DataFrame(
   {
      "x": [5, 2, 1, 9],
      "y": [4, 1, 5, 10],
      "z": [4, 1, 5, 0]
   }
)

print "Input DataFrame is:\n", df
col = "x"
check(col)
col = "a"
check(col)

# COMMAND ----------

l = [(2, 1), (1,1)]
df = spark.createDataFrame(l)

def calc_dif(x,y):
    if (x>y) and (x==1):
        return x-y

dfNew = df.withColumn("calc", calc_dif(df["_1"], df["_2"]))
dfNew.show()


# COMMAND ----------

# I wanna sum lat, lng, alt for every dataframe column
columns = ["lat","lng", "alt"]

for dataframe in df.columns:   #df[columns]
  #df.select(dataframe).show()
  if (dataframe == df["lat"]) and (dataframe == df["lng"]):
    print('Yep')
  else:
    print('No values')
    
#  if dataframe_columns == x:
#    print(df.x)
#  else:
#    print('No')

# COMMAND ----------

for dataframe in df.columns:
  print(dataframe)

# COMMAND ----------

# I wanna sum lat, lng, alt for every dataframe column

columns = ["lat","lng", "alt"]

for dataframe in df.columns: # df.columns = [circuitId, circuitRef, name, location, country, lat, lng, alt, url] # df.columns = lists of columns
  if 'lat' in dataframe: # if <string> 'lat' in  <list of strings> dataframe: 
    print('the columns exists in dataframe')
  elif 'lng' in dataframe:
    print('the columns exists in dataframe')
  elif 'alt' in dataframe:
    print('the columns exists in dataframe')
    
    df.select(concat(df.lat, df.lng, df.alt).alias('sum_of_3_values'))#.collect()
    display(df)
  else:
    print('Error there is no columns')

    
#  if dataframe_columns == x:
#    print(df.x)
#  else:
#    print('No')

#    print("lat")
#  elif df["lng"] in df[columns]:
#    print("lng")
#  elif df["alt"] in df[columns]:
#    print("alt")
#  else:

# COMMAND ----------

# I wanna sum lat, lng, alt for every dataframe column

columns = ["lat","lng", "alt"]

for dataframe in df.columns:
  df.select(concat(df.lat, df.lng, df.alt).alias('sum_of_3_values'))#.collect()
  display(df)


# COMMAND ----------

df_with_filter = df.withColumn('')

# COMMAND ----------

df_filter = df.filter("country = 'USA'").select(sum('round')).show()
display(df_filter)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

races_df.filter("name = 'Chinese Grand Prix'").select(sum('round'), countDistinct("year")).show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC # File two

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, lit


##################################################################
# LOADING
##################################################################
#>> data lake address
data_lake_address = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#>> path_of_file
path_of_file = "/uds/uniepro/inteligencia_ocupacional/teste_de_dados/races.csv"
path = '{data_lake}{file}'.format(data_lake = data_lake_address, file = path_of_file)
df_v = spark.read\
.format("csv")\
.option("header",True)\
.option("delimiter", ",")\
.load(path)
               
#df.display()
display(df_v)

# COMMAND ----------

df_v1 = df_v.withColumn("ingestion", current_timestamp()) \
                                     .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
  
display(df_v1)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Write the output to processed container in parquet format

# PartitionBy = df2.write.mode('overwrite').partitionBy('nationality').parquet('/mnt/formula1ld/constructor/') #-----> Ira separar em partições de acordo com o ano.

PartitionBy = df2.write.mode('overwrite').parquet('/mnt/formula1ld/constructor/')
# display(spark.read.parquet('/.../.../'))




'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/inteligencia_ocupacional/teste_de_dados/constructors.json'
  
data_lake_address = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'




url = "https://raw.githubusercontent.com/thomaspernet/data_csv_r/master/data/adult.csv"
from pyspark import SparkFiles


  
# Write data to datalake as parquet

##################################################################
# SAVING
##################################################################

#>> data lake address
data_lake_address = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

#>> path_of_file
path_of_file = "/uds/uniepro/inteligencia_ocupacional/parquet.parquet"

#>> path to load the file
path = '{data_lake}{file}'.format(data_lake = data_lake_address, file = path_of_file)

ds.repartition(1).write.mode("overwrite").option('header', True).parquet(path)

#df.write.format("json").mode("overwrite).save(outputPath/file.json)



##################################################################
# LOADING
##################################################################

#>> data lake address
data_lake_address = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

#>> path_of_file
path_of_file = "/uds/uniepro/inteligencia_ocupacional/monitor_de_emprego.csv"

#>> path to load the file
path = '{data_lake}{file}'.format(data_lake = data_lake_address, file = path_of_file)

# df = spark.read.format("csv").option("header","true").load(filePath)

#>> 2 method to read files
df_data = spark.read.csv(path, header=True)
df = spark.read.format("csv").option("header","true").load(path)

df = spark.read.\
.format("csv")\
.option("header","true")\
.load(path)

# Parquet
#>> 1 df=spark.read.format("parquet).load(parquetDirectory
#>> 2 df=spark.read.parquet(parquetDirectory)

#https://ichi.pro/pt/spark-essentials-como-ler-e-gravar-dados-com-pyspark-101455223728077
#https://www.geeksforgeeks.org/read-text-file-into-pyspark-dataframe/

# There are three was to read text files into Pyspark DataFrame
#>> Using spark.read.text()
#>> Using spark.read.csv()
#>> Using spark.read.format().load()

# Read Delta
# Spark SQL
#>> SELECT * FROM delta. "path"

# Spark SQL Unmanaged Table
#>> spark.sql(" DROP TABLE IF EXISTS table_name")
#>> spark.sql(" CREATE TABLE table_name USING DELTA LOCATION '{}'".format(path))

# WRITTING Delta
#>> df.write.format('delta').partitionBy('someColumn').save("path")

#https://github.com/xavier211192/Databricks/blob/main/Spark%20Read_Write%20Cheat%20Sheet.pdf
##################################################################
##################################################################



# Write the output to processed container in parquet format

PartitionBy = df2.write.mode('overwrite').partitionBy('Year_(ANOS)').parquet('/..../.../') -----> Ira separar em partições de acordo com o ano.

display(spark.read.parquet('/.../.../'))