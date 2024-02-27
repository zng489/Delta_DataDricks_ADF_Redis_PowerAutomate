# Databricks notebook source
# Page 122

myRDD = sc.parallelize([
  ('Mike', 19),
  ('June', 18),
  ('Rachel', 16 ),
  ('Rob', 18),
  ('Scott', '17')
])

#=====X====X====X====X====X====X====X====X====X====#

myRDD.take(5)

#=====X====X====X====X====X====X====X====X====X====#


#=====X====X====X====X====X====X====X====X====X====#
#     Storage type Example
#     
#     Local files sc.textFile('/local folder/filename.csv')
#     Hadoop HDFS sc.textFile('hdfs://folder/filename.csv')
#     
#     AWS S3 (https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-sparkconfigure.html)
#     sc.textFile('s3://bucket/folder/filename.csv')
#     
#     Azure WASBs (https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-use-blob-storage)
#     sc.textFile('wasb://bucket/folder/filename.csv')
#     
#     Google Cloud Storage (https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#other_sparkhadoop_clusters)
#     sc.textFile('gs://bucket/folder/filename.csv')
#     
#     Databricks DBFS (https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html)
#     sc.textFile('dbfs://folder/filename.csv')

#=====X====X====X====X====X====X====X====X====X====#                      

# COMMAND ----------

# Page 122

models_df = sc.parallelize([
  ('MacBook Pro', 'Laptop'),
  ('MacBook', 'Laptop'),
  ('MacBook Air', 'Laptop' ),
  ('iMac', 'Desktop')
]).toDF(['Model', 'FormFactor'])


# The syntax for PYSPARK MAP FUNCTION is:
# a.map(lambda x: x+1)

# COMMAND ----------

models_df.createOrReplaceTempView('models')

# COMMAND ----------

import pandas as pd

df_myRDD =  pd.read_csv('https://raw.githubusercontent.com/drabastomek/learningPySpark/master/Chapter03/flight-data/departuredelays.csv')
df_myRDD

myRDD = spark.createDataFrame(df_myRDD)
display(myRDD)

# COMMAND ----------

dbutils.fs.mkdirs("/myRDD/")

# COMMAND ----------

dbutils.fs.mkdirs("/myRDD/")


#=====X====X====X====X====X====X====X
# Endereço do data lake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/data/airport-codes-na.txt'  # <----- O ARQUIVO ERA PARA SER 2019, NO ENTANTO ESQUECERAM DE COLOCAR NO DATA LAKE, LOGO FOI FEITO COM ARQUIVO 2020, E AS MUDANCAS FORAM CORRIGIDAS POSTERIORIOMENTE NO WITHCOLULMNRENAMED
path = var_adls_uri + de_para_path

#=====X====X====X====X====X====X====X
# importando o prm 
# prm em formato 'csv'
prm = spark.read\
.format("text")\
.option("header","true")\
.load(path)
###  #=====X====X====X====X====X====X====X
###  # Transformando para pandas e fazendo modificações
###  prm = prm.toPandas()
###  prm = prm.iloc[18:].reset_index(drop=True)
###  
###  prm = prm.rename(columns={"Cliente:": "Tabela_Origem",
###                                        "_c1": "Campo_Origem",
###                                        "_c2":"Transformação",
###                                        "_c3":"Campo_Destino",
###                                        "_c4":"Tipo_tamanho",
###                                        "_c5":"Descrição"})
###  
###  prm = prm[["Tabela_Origem", "Campo_Origem", "Transformação", "Campo_Destino", "Tipo_tamanho", "Descrição"]]
###  prm['ANO'] = 2019 # integer
###  prm_COM_NA = prm[prm['Campo_Origem'] == 'N/A']
###  prm_COM_NA.tail(3) #prm com values (valores N/A)
###  
###  #=====X====X====X====X====X====X====X




# Loading a table called fl_insurance_sample into the variable dfdf = spark.table('fl_insurance_sample')

# Storing the data in one CSV on the DBFSdf.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/fl_insurance_sample.csv")


#prm = prm.toPandas()

dbutils.fs.mkdirs("/ZY/")
prm.coalesce(1).write.format('text').save("dbfs:/ZY/airport_codes")

# prm.to_csv('dbfs:/myRDD')
# PermissionError: [Errno 13] Permission denied: 'dbfs:/myRDD'

#dbutils.fs.rm("dbfs:/myRDD/prm.csv", True)
#dbutils.fs.unmount("dbfs:/myRDD/prm.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC _____
# MAGIC _____

# COMMAND ----------

myRDD = (sc.textFile('dbfs:/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt',
                     minPartitions=4, use_unicode=True)).map(lambda element: element.split('\t'))

myRDD

# COMMAND ----------

myRDD.take(5)

# COMMAND ----------

myRDD.count()

# COMMAND ----------

myRDD.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ------
# MAGIC ------

# COMMAND ----------

#dbutils.fs.mkdirs("/myRDD/")

#=====X====X====X====X====X====X====X
# Endereço do data lake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/data/departuredelays.csv' 
path = var_adls_uri + de_para_path

prm = spark.read\
.format("csv")\
.option("header","true")\
.load(path)


#=====X====X====X====X====X====X====X

# The folder "/ZY/" was already created!
#dbutils.fs.mkdirs("/ZY/")
prm.coalesce(1).write.format('csv').save("dbfs:/ZY/departuredelays")
# dbfs:/ZY/departuredelays/part-00000-tid-3783072970161261666-72d34f26-415a-4d38-a687-09408bc08f7e-5333-1-c000.csv
# prm.to_csv('dbfs:/myRDD')
# PermissionError: [Errno 13] Permission denied: 'dbfs:/myRDD'
#dbutils.fs.rm("dbfs:/myRDD/prm.csv", True)
#dbutils.fs.unmount("dbfs:/myRDD/prm.csv")

#=====X====X====X====X====X====X====X

# COMMAND ----------

#=====X====X====X====X====X====X====X
myRDD = (sc.textFile('dbfs:/ZY/departuredelays/part-00000-tid-3783072970161261666-72d34f26-415a-4d38-a687-09408bc08f7e-5333-1-c000.csv').map(lambda element: element.split(",")))

myRDD.count()
# Out[18]: 1391578
# Command took 3.55 seconds -- by zhang.yuan@senaicni.com.br at 13/01/2022 15:46:59 on uniepro 2

#display(myRDD)
# 'RDD' object has no attribute 'limit'

# COMMAND ----------

myRDD.getNumPartitions()

# COMMAND ----------

#=====X====X====X====X====X====X====X
myRDD = (sc.textFile('dbfs:/ZY/departuredelays/part-00000-tid-3783072970161261666-72d34f26-415a-4d38-a687-09408bc08f7e-5333-1-c000.csv', minPartitions=8).map(lambda element: element.split(",")))

myRDD.count()
# Out[18]: 1391578
# Command took 3.55 seconds -- by zhang.yuan@senaicni.com.br at 13/01/2022 15:46:59 on uniepro 2

#Command took 1.14 seconds -- by zhang.yuan@senaicni.com.br at 13/01/2022 15:51:41 on uniepro 2

# COMMAND ----------

# MAGIC %md
# MAGIC _____
# MAGIC _____
# MAGIC _____

# COMMAND ----------

# Transformations
#  zipWithIndex()
# map()
# where(), filter()
# distinct()
# getNumPartitions()
# mapPartitionsWithIndex()



#dbutils.fs.mkdirs("/myRDD/")

#=====X====X====X====X====X====X====X
# Endereço do data lake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
de_para_path = '/uds/uniepro/data/departuredelays.csv' 
path = var_adls_uri + de_para_path

prm = spark.read\
.format("csv")\
.option("header","true")\
.load(path)

#=====X====X====X====X====X====X====X

# The folder "/ZY/" was already created!
#dbutils.fs.mkdirs("/ZY/")
prm.coalesce(1).write.mode("overwrite").format('csv').save("dbfs:/ZY/departuredelays")
# dbfs:/ZY/departuredelays/part-00000-tid-3783072970161261666-72d34f26-415a-4d38-a687-09408bc08f7e-5333-1-c000.csv
# prm.to_csv('dbfs:/myRDD')
# PermissionError: [Errno 13] Permission denied: 'dbfs:/myRDD'
#dbutils.fs.rm("dbfs:/myRDD/prm.csv", True)
#dbutils.fs.unmount("dbfs:/myRDD/prm.csv")

#=====X====X====X====X====X====X====X


# COMMAND ----------

# Transformations
# zipWithIndex()
# map()
# where(), filter()
# distinct()
# getNumPartitions()
# mapPartitionsWithIndex()

# COMMAND ----------

myRDD = (sc.textFile('dbfs:/ZY/departuredelays/part-00000-tid-1076534560867360930-cb777dfe-be3f-4ca0-9a4a-996e2081f469-11171-1-c000.csv', minPartitions=5).map(lambda element: element.split(",")))

myRDD.take(5)

# COMMAND ----------

# take() just like display, but in RDD

myRDD.map(lambda c: (c[0], c[1])).take(5)

# COMMAND ----------


# take() just like display, but in RDD

myRDD.filter(lambda c: c[3] == "DTW").take(5)

# COMMAND ----------

myRDD.take(10)

# COMMAND ----------

# .flatMap(...) Transformation

myRDD.filter(lambda c: c[3] == "DTW")\
.map(lambda c: (c[0],c[1]))\
.flatMap(lambda x: x)\
.take(10)

# COMMAND ----------

# .distinct(...) Transformation

myRDD.map(lambda c: (c[2]))\
.distinct()\
.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # sample
# MAGIC ![#1589F0](https://via.placeholder.com/1500x10/1589F0/?text=+"write_ur_test")
# MAGIC - .sample(...) Transformation

# COMMAND ----------

#=====X====X====X====X====X====X====X=====X====X====X====X====X====X====X

flights = (sc.textFile('dbfs:/ZY/departuredelays/part-00000-tid-1076534560867360930-cb777dfe-be3f-4ca0-9a4a-996e2081f469-11171-1-c000.csv', minPartitions=10).
           map(lambda element: element.split(',')))

flights.map(lambda c:c[3]).sample(True, 0.001).take(5)
# ['ABE', 'ABQ', 'AMA', 'AMA', 'ANC']

#=====X====X====X====X====X====X====X=====X====X====X====X====X====X====X

# sample(withReplacement, fraction, seed=None)
         # True with Duplicates
         # False No Duplicates
#=====X====X====X====X====X====X====X=====X====X====X====X====X====X====X

# COMMAND ----------

# MAGIC %md
# MAGIC # join
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .join(...) Transformation

# COMMAND ----------

#=====X====X====X====X====X====X====X=====X====X====X====X====X====X====X

flights = (sc.textFile('dbfs:/ZY/departuredelays/part-00000-tid-1076534560867360930-cb777dfe-be3f-4ca0-9a4a-996e2081f469-11171-1-c000.csv', minPartitions=10).
           map(lambda element: element.split(',')))

#flights.map(lambda c: (c[3], c[0])).take(5)
flt = flights.map(lambda c: (c[3], c[0]))
flt.collect()


# Out[87]: [('ABE', '01011245'),
# ('ABE', '01020600'),
# ('ABE', '01021245'),
# ('ABE', '01020605'),
# ('ABE', '01031245')]

#=====X====X====X====X====X====X====X=====X====X====X====X====X====X====X

# COMMAND ----------

#=====X====X====X====X====X====X====X=====X====X====X====X====X====X====X

airports = (sc.textFile('dbfs:/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt').
           map(lambda element: element.split('\t')))
# 143
# airports.map(lambda c: (c[3], c[1]) ).take(5)
# ut[88]: [('IATA', 'State'), ('YXX', 'BC'), ('ABR', 'SD'), ('ABI', 'TX'), ('CAK', 'OH')]


air = airports.map(lambda c: (c[3], c[1]) )
#air = air.zipWithIndex().filter(lambda x : x[1] > 0).map(lambda x : x[0])

air.collect()
# zipWithIndex()

#[(['City', 'State', 'Country', 'IATA'], 0),
# (['Abbotsford', 'BC', 'Canada', 'YXX'], 1),
# (['Aberdeen', 'SD', 'USA', 'ABR'], 2)]


#=====X====X====X====X====X====X====X=====X====X====X====X====X====X====X
# airports.map(lambda c: list(c)).take(5)

# air = airports.map(lambda c: [c[3], c[1]] )
# [['City', 'State', 'Country', 'IATA']


# air = airports.map(lambda c: (c[3], c[1]) )
# (['City', 'State', 'Country', 'IATA'])

# COMMAND ----------

flt.join(air).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # repartition
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .repartition(...) Transformation

# COMMAND ----------

# .repartition(...) Tranformation
# The myRDD originally generated has 2 partitions
# myRDD.getNumPartitions()
# Output 2

# Let's re-partition this to 8 we can have 8 partitions
myRDD2 = myRDD.repartition(8)

# Checking ht number of partitions for myRDD
myRDD2.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC # zipWithIndex
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .zipWithIndex(...) Transformation

# COMMAND ----------

airports = (sc.textFile('dbfs:/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt').
            map(lambda element: element.split('\t')))

airports.zipWithIndex().filter(lambda x : x[1] > 0).map(lambda x : x[0]).take(3)

# zipWithIndex()

#[(['City', 'State', 'Country', 'IATA'], 0),
# (['Abbotsford', 'BC', 'Canada', 'YXX'], 1),
# (['Aberdeen', 'SD', 'USA', 'ABR'], 2)]

# COMMAND ----------

# MAGIC %md
# MAGIC # reduceByKey
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .reduceByKey(...) Transformation

# COMMAND ----------

# Determine delays by originating city, - remove header row via zipWithIndex() and map()

flights.zipWithIndex().filter(lambda x : x[1] > 0).map(lambda x : x[0])\
.map(lambda c: (c[3], int(c[1]) )).reduceByKey(lambda x,y: x+ y).take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # sortByKey
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .reduceByKey(...) Transformation

# COMMAND ----------

flights\
.zipWithIndex()\
.filter(lambda x : x[1] > 0)\
.map(lambda x : x[0])\
.map(lambda c: (c[3], int(c[1]) ))\
.reduceByKey(lambda x,y: x+ y)\
.sortByKey()\
.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # union
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .union(...) Transformation

# COMMAND ----------

a.union(b).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # mapPartitionsWithIndex
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .mapPartitionsWithIndex(...) Transformation

# COMMAND ----------

def partitionElementCount(idx, iterator):
  count = 0
  for _ in iterator:
    count += 1
  return idx, count

fligths.mapPartitionsWithIndex(partitionElementCount(idx, iterator)).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Overview of RDD actions
# MAGIC
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .mapPartitionsWithIndex(...) Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC One way to add color to a README is by utilising a service that provides placeholder images.
# MAGIC
# MAGIC For example this Markdown can be used:
# MAGIC
# MAGIC - ![#f03c15](https://via.placeholder.com/15/f03c15/000000?text=+) `#f03c15`
# MAGIC - ![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) `#c5f015`
# MAGIC - ![#1589F0](https://via.placeholder.com/15/1589F0/000000?text=+) `#1589F0`
# MAGIC To create a list of any colors you like:
# MAGIC
# MAGIC #f03c15 #f03c15
# MAGIC #c5f015 #c5f015
# MAGIC #1589F0 #1589F0

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # take()
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .take(...) - Return values from the rows

# COMMAND ----------

airports = (sc.textFile('dbfs:/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt').
           map(lambda element: element.split('\t')))
airports.take(10)

# COMMAND ----------

flights = (sc.textFile('dbfs:/ZY/departuredelays/part-00000-tid-1076534560867360930-cb777dfe-be3f-4ca0-9a4a-996e2081f469-11171-1-c000.csv', minPartitions=10).
           map(lambda element: element.split('\t')))
flights.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # collect()
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .collect(...) - Return  all the values from the worker to the driver

# COMMAND ----------

airports = (sc.textFile('dbfs:/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt', minPartitions=10).
           map(lambda element: element.split('\t')))
airports.filter(lambda c:c[1] == 'WA').collect()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # count()
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .count(...)

# COMMAND ----------

airports = (sc.textFile('dbfs:/ZY/airport_codes/part-00000-tid-6966928513662212729-d3f37d5e-335b-4b85-837f-0b77c40570b3-115-1-c000.txt')
            .map(lambda element: element.split('\t')))

airports.zipWithIndex().filter(lambda x : x[1] > 0).map(lambda x : x[0]).count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # saveAsTextFile()
# MAGIC ![#c5f015](https://via.placeholder.com/1500x10/c5f015//?text=+"write_ur_test")
# MAGIC - .saveAsTextFile(...)

# COMMAND ----------

# Save airports as a text file
# Note, each partition has their own file

# saveAsTextFile
# airports.saveAsTextFile("/tmp/denny/airports")

# Review file structure
# Note that 'airports' is a folders with two files (part-zzzz) as the airports RDD is comprised of two partitions.
# "/tmp/denny/airports/_SUCESS"
# "/tmp/denny/airports/part-00000"
# "/tmp/denny/airports/part-00001"

airports.saveAsTextFile("dbfs:/ZY/teste")

# COMMAND ----------

