# Databricks notebook source
# Se precisar
# python
# Copiar código
from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder \
    .appName("DeltaLakeDetailedExample") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

########################################
### Importar bibliotecas necessárias ###
########################################

import os
import pyspark
from pyspark.sql import SparkSession

# Inicializar a SparkSession
spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .getOrCreate()

# Criar um DataFrame de exemplo
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Salvar como uma tabela Delta
#df.write.format("delta").mode("overwrite").save("/mnt/delta_table")


# COMMAND ----------

df.display()

# COMMAND ----------

dbutils.fs.ls("dbfs:/Folder")

# COMMAND ----------

# Make a Directory:
dbutils.fs.mkdirs("dbfs:/Folder")

# COMMAND ----------

####################################
### Salvar como uma tabela Delta ###
####################################

df.write.format("delta").mode("overwrite").save("dbfs:/Folder")

# COMMAND ----------

#################################
### Ler dados da tabela Delta ###
#################################

df_delta = spark.read.format("delta").load("dbfs:/Folder")
df_delta.show()

# COMMAND ----------

################################################
### Criar um novo DataFrame para atualização ###
################################################

new_data = [("Alice", 35), ("Bob", 45), ("David", 28)]
new_df = spark.createDataFrame(new_data, columns)

# COMMAND ----------

new_df.display()

# COMMAND ----------

################################################
### Criar um novo DataFrame para atualização ###
################################################

# new_data = [("Alice", 35), ("Bob", 45), ("David", 28)]
# new_df = spark.createDataFrame(new_data, columns)

# Atualizar os dados existentes
# new_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("/mnt/delta_table")
new_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/Folder")

# COMMAND ----------



# COMMAND ----------

spark.read.format("delta").load("dbfs:/Folder").display()

# COMMAND ----------

spark.read.format("delta").load("dbfs:/Folder").printSchema()

# COMMAND ----------



# COMMAND ----------

###############################################
### Consultar uma versão anterior da tabela ###
###############################################

# version_df = spark.read.format("delta").option("versionAsOf", 0).load("/mnt/delta_table")
version_df = spark.read.format("delta").option("versionAsOf", 0).load("dbfs:/Folder")
version_df.show()

# COMMAND ----------

version_df = spark.read.format("delta").option("versionAsOf", 1).load("dbfs:/Folder")
version_df.show()

# COMMAND ----------

version_df = spark.read.format("delta").option("versionAsOf", 2).load("dbfs:/Folder")
version_df.show()

# COMMAND ----------



# COMMAND ----------

#################################
### Atualizar Dados com Merge ###
#################################

# Suponha que queremos atualizar a idade de "Alice" e adicionar um novo registro:
  
# Novo DataFrame para atualização
updates = [("Alice", 40), ("David", 28)]
updates_df = spark.createDataFrame(updates, columns)

# Usar Merge para atualizar e inserir dados
from delta.tables import DeltaTable

# Criar uma tabela Delta a partir do caminho existente
# delta_table = DeltaTable.forPath(spark, "/mnt/delta_table")
delta_table = DeltaTable.forPath(spark, "dbfs:/Folder")


# Fazer o merge
delta_table.alias("old_data") \
    .merge(
        updates_df.alias("new_data"),
        "old_data.name = new_data.name"
    ) \
    .whenMatchedUpdate(set={"age": "new_data.age"}) \
    .whenNotMatchedInsert(values={"name": "new_data.name", "age": "new_data.age"}) \
    .execute()


# COMMAND ----------

delta_table.toDF().show()

# COMMAND ----------



# COMMAND ----------

###########################
### Ler dados filtrados ###
###########################

filtered_df = spark.read.format("delta").load("dbfs:/Folder").filter("age > 30")
filtered_df.show()
# Exibir estatísticas da tabela Delta
delta_table.toDF().describe().show()

# COMMAND ----------



# COMMAND ----------

#################################
### Otimização com Z-Ordering ###
################################

# Podemos melhorar o desempenho das consultas usando Z-Ordering:

# python
# Copiar código
# Otimizar a tabela com Z-Ordering
delta_table.optimize().where("age > 30").execute()

# Optimize the table with Z-Ordering
delta_table.optimize().zOrderBy("age").execute()

# COMMAND ----------



# COMMAND ----------

###################################
### Compactar arquivos pequenos ###
###################################

### spark.sql("OPTIMIZE delta.`/mnt/delta_table`")

# Remover arquivos antigos que não são mais necessários
### spark.sql("VACUUM delta.`/mnt/delta_table` RETAIN 168 HOURS")  # Retém arquivos por 7 dias
spark.sql("VACUUM delta.`dbfs:/Folder` RETAIN 168 HOURS")


# COMMAND ----------

spark.sql("VACUUM delta.`dbfs:/Folder` RETAIN 168 HOURS").display()

# COMMAND ----------



# COMMAND ----------

########################
### Schema Evolution ###
########################

# O Delta Lake permite que você altere o esquema da tabela, adicionando novas colunas conforme necessário.
# python
# Copiar código
# Adicionar uma nova coluna

new_data = [("Alice", 35, "F"), ("Bob", 45, "M"), ("Cathy", 29, "F")]
new_columns = ["name", "age", "gender"]
new_df = spark.createDataFrame(new_data, new_columns)

# Salvar com evolução de esquema

# new_df.write.format("delta").mode("append").option("mergeSchema", "true").save("/mnt/delta_table")
new_df.write.format("delta").mode("append").option("mergeSchema", "true").save("dbfs:/Folder")

# COMMAND ----------

spark.read.format("delta").load("dbfs:/Folder").display()

# COMMAND ----------



# COMMAND ----------

#######################################################
### Gerenciamento de Conflitos de Leitura e Escrita ###
#######################################################

# O Delta Lake permite que você trate conflitos de forma eficiente, usando a funcionalidade de transações ACID.
# python
# Copiar código

from delta.tables import DeltaTable

# Tentar atualizar a tabela enquanto lê
delta_table = DeltaTable.forPath(spark, "/mnt/delta_table")
try:
    # Exemplo de operação de leitura e escrita simultânea
    df_current = delta_table.toDF()
    df_current.show()

    # Atualizar a tabela
    delta_table.update("name = 'Alice'", {"age": "36"})
except Exception as e:
    print(f"Ocorreu um erro: {e}")

# COMMAND ----------



# COMMAND ----------

#######################
### Particionamento ###
#######################

# Particionar tabelas Delta pode melhorar o desempenho das consultas.
# python
# Copiar código
# Salvar como tabela Delta particionada

df.write.partitionBy("age").format("delta").mode("overwrite").save("/mnt/delta_table_partitioned")

# COMMAND ----------



# COMMAND ----------

#################################
### Monitoramento e Auditoria ###
#################################

# O Delta Lake permite que você monitore as operações e mantenha um histórico de alterações para auditoria.
# python
# Copiar código
# Consultar o histórico de operações

history_df = delta_table.history(10)  # últimos 10 comandos
history_df.show(truncate=False)

# COMMAND ----------

history_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming

# COMMAND ----------

# Se precisar
from pyspark.sql import SparkSession

# Inicializar a SparkSession com suporte a Delta
spark = SparkSession.builder \
    .appName("DeltaLakeStreamingExample") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# Se precisar
from pyspark.sql import SparkSession

# Initialize a SparkSession with support for Delta Lake
spark = SparkSession.builder \
    .appName("DeltaLakeStreamingExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

#######################
### Streaming_input ###
#######################

# Create a list of tuples with the data
data = [("Alice", 30), ("Bob", 25), ("Cathy", 27)]

# Define the schema
columns = ["name", "age"]

# Create DataFrame
df_test = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

df_test.display()

# COMMAND ----------

# Make a Directory:
dbutils.fs.mkdirs("dbfs:/Streaming")

# COMMAND ----------

# Delete Files:
dbutils.fs.rm("dbfs:/Streaming", True)  # True to delete recursively

# COMMAND ----------

# Suponha que df_test seja seu DataFrame
df_test.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("dbfs:/Streaming")

# COMMAND ----------



# COMMAND ----------

#############################################
### Criar uma fonte de dados de streaming ###
#############################################

# Para fins de exemplo, você pode usar um diretório onde os arquivos CSV serão colocados
# input_path = "/mnt/streaming_input"
input_path = "dbfs:/Streaming"

# Suponha que os arquivos CSV tenham as colunas "name" e "age"
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema of the CSV files
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Use the defined schema in the readStream operation
streaming_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

# COMMAND ----------

streaming_df.display()

# COMMAND ----------

########################################
### Processar e Gravar em Delta Lake ###
########################################

# Processar e Gravar em Delta Lake
# Agora, vamos processar os dados recebidos e gravá-los em uma tabela Delta.
# python
# Copiar código

# Especificar o caminho da tabela Delta
# delta_table_path = "/mnt/delta_table_streaming"
delta_table_path = "dbfs:/Delta_table_streaming"

# Escrever os dados em um formato Delta
query = streaming_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/Delta_table_streaming") \
    .start(delta_table_path)
# .option("checkpointLocation", "/mnt/checkpoints") \

query.awaitTermination()

# COMMAND ----------

##############################################
### Ler Dados da Tabela Delta em Streaming ###
##############################################

# Agora que temos os dados sendo gravados na tabela Delta, podemos criar uma consulta para ler esses dados em tempo real.
# python
# Copiar código
# Ler os dados da tabela Delta em streaming
delta_streaming_df = spark.readStream \
    .format("delta") \
    .load(delta_table_path)

# Escrever os dados lidos na saída do console
query_console = delta_streaming_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_console.awaitTermination()

# COMMAND ----------

##################################
### Simulando Dados de Entrada ###
##################################

# Para testar este exemplo, você pode usar o netcat (ou nc) para enviar dados ao socket. Abra um terminal e execute:
# bash
# Copiar código
# nc -lk 9999

# COMMAND ----------


