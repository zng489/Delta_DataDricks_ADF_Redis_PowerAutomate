# Databricks notebook source
!pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.fs.ls("dbfs:/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/")
dbutils.fs.cp("dbfs:/source", "dbfs:/dest", recurse=True)
dbutils.fs.mv("dbfs:/source", "dbfs:/dest")
dbutils.fs.rm("dbfs:/path", recurse=True)
dbutils.fs.put("dbfs:/myfile.txt", "Hello World", overwrite=True)

# COMMAND ----------

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializando o Faker e o Spark
fake = Faker()
spark = SparkSession.builder.getOrCreate()

# Função para gerar dados falsos
def generate_fake_data(n):
    data = []
    for _ in range(n):
        data.append((
            fake.name(),
            fake.address(),
            fake.email(),
            fake.phone_number(),
            fake.date_of_birth(minimum_age=18, maximum_age=99),
            fake.company()
        ))
    return data

# Gerando dados (exemplo de 100.000 registros)
fake_data = generate_fake_data(100000)

# Criando o DataFrame
columns = ["nome", "endereco", "email", "telefone", "data_nascimento", "empresa"]
df = spark.createDataFrame(fake_data, columns)

# Mostrar o DataFrame
df.show(5)

# COMMAND ----------

df.display()

# COMMAND ----------

df_100 = df.limit(100)

# COMMAND ----------

# Saving csv

df.write.format('csv').option('header', True).mode('overwrite').save(adl_uld)

# COMMAND ----------

# df_100.write.mode('overwrite').parquet('/Volumes/workspace/default/external_volume/datalake/folder_teste/parquet')

# Salvar o DataFrame como Delta
# FORMA ERRADO df_100.write.mode('overwrite').delta('/Volumes/workspace/default/external_volume/datalake/folder_teste/delta')
df_100.write.format('delta').mode('overwrite').save('/Volumes/workspace/default/external_volume/datalake/folder_teste/delta')

# COMMAND ----------

# MAGIC %md
# MAGIC Reading files through delta and parquet

# COMMAND ----------

delta_df = spark.read.format("delta").load('/Volumes/workspace/default/external_volume/datalake/folder_teste/delta', header=True)

# COMMAND ----------

# Ler os dados Delta gravados
# df = spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/bases_referencia/municipios/')

parquet_df = spark.read.format("parquet").load('/Volumes/workspace/default/external_volume/datalake/folder_teste/parquet', header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Reading through the SQL 1

# COMMAND ----------

parquet_df = spark.read.format("parquet").load('/Volumes/workspace/default/external_volume/datalake/folder_teste/parquet', header=True)

# COMMAND ----------

# Criar uma tabela temporária
parquet_df.createOrReplaceTempView("parquet_df_temp")

# Consultar com SQL
df_sql_temp = spark.sql("SELECT * FROM parquet_df_temp")

# COMMAND ----------

df_sql_temp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Reading through the SQL 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/external_volume/datalake/folder_teste/delta`
# MAGIC /*
# MAGIC select * from datalake__biz.oni.oni_bases_referencia_cnae_cnae_20__cnae_divisao;
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark Pandas

# COMMAND ----------

import pyspark.pandas as ps
pyspark_pandas = ps.read_parquet('/Volumes/workspace/default/external_volume/datalake/folder_teste/parquet')

# PandasAPIOnSparkAdviceWarning: If `index_col` is not specified for `read_parquet`, the default index is attached which can cause additional overhead.
#pyspark_pandas = ps.read_parquet(
#    '/Volumes/workspace/default/external_volume/datalake/folder_teste/parquet',
#    index_col="PassengerId"  # substitua com a coluna adequada
#)

# COMMAND ----------

pyspark_pandas.head(10)

# COMMAND ----------

pyspark_pandas.to_table("workspace.default.faker", overwriteSchema=True)
# workspace Catalog
# default Schema
# faker Table

# COMMAND ----------

# MAGIC %md
# MAGIC Creating table com spark sql

# COMMAND ----------

'''
spark.sql("create table if not exists `lab_teste_acesso`.`default`.`municipios` USING `parquet` LOCATION 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/bases_referencia/municipios/'")

spark.sql("create table if not exists `lab_oni`.`projecao_pacotes_r`.`projecao_setorial` USING `csv` OPTIONS (header 'true', inferSchema 'true') LOCATION 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/projecao_pacotes_r/projecao_setorial.csv'")

spark.sql("create table if not exists `lab_oni`.`projecao_pacotes_r`.`pnad_agg_projecao` USING `csv` OPTIONS (header 'true', inferSchema 'true') LOCATION 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/projecao_pacotes_r/pnad_agg_projecao.csv'")
'''


# COMMAND ----------

# MAGIC %md
# MAGIC Reading through Unity SQL

# COMMAND ----------

spark.sql("SELECT * FROM workspace.default.faker").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving as Table in Unity

# COMMAND ----------

# MAGIC %md
# MAGIC I could save with two ways, through inside the unity or at databricks

# COMMAND ----------

'''
SELECT * FROM workspace.default.faker

/*  ALTER TABLE modificar uma tabela existente. */

ALTER TABLE workspace.default.faker
CHANGE COLUMN nome COMMENT 'nome do funcionario';

CREATE TAG nome COMMENT 'strings';

/* 4. Alternativa: Usar TBLPROPERTIES para armazenar metadados (se Unity Catalog não estiver disponível)
Se o Unity Catalog não está habilitado ou não está disponível, você pode usar TBLPROPERTIES para armazenar metadados diretamente em uma tabela ou coluna.
Exemplo para adicionar uma propriedade à tabela: */

ALTER TABLE workspace.default.faker
SET TBLPROPERTIES ('nome_funcionarios' = 'string');


ALTER TABLE workspace.default.faker ALTER COLUMN nome SET TAGS ("nomes classificados");
'''

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Tagging and Classification

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.youtube.com/watch?v=CqZFbhfCbSk

# COMMAND ----------

# MAGIC %sql
# MAGIC --Apply tags to catalog
# MAGIC ALTER CATALOG demo SET TAGS ('project'='fairfield')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo;
# MAGIC
# MAGIC --Apply tags to Database
# MAGIC ALTER schema boat SET TAGS ('example');

# COMMAND ----------

# MAGIC %sql
# MAGIC use  catalog demo;
# MAGIC use schema boat;
# MAGIC
# MAGIC --Apply tags to table
# MAGIC ALTER TABLE demo SET TAGS ('owner'='youssef', 'usage'='demo');

# COMMAND ----------

# MAGIC %sql
# MAGIC --Set/unset tags for table column
# MAGIC ALTER TABLE demo ALTER COLUMN Age SET TAGS ('Classified')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Set/unset tags for table column
# MAGIC ALTER TABLE demo ALTER COLUMN Age UNSET TAGS ('Classified')

SELECT * FROM workspace.default.faker

/*  ALTER TABLE modificar uma tabela existente. */

ALTER TABLE workspace.default.faker
CHANGE COLUMN nome COMMENT 'nome do funcionario';

CREATE TAG nome COMMENT 'strings';

/* 4. Alternativa: Usar TBLPROPERTIES para armazenar metadados (se Unity Catalog não estiver disponível)
Se o Unity Catalog não está habilitado ou não está disponível, você pode usar TBLPROPERTIES para armazenar metadados diretamente em uma tabela ou coluna.
Exemplo para adicionar uma propriedade à tabela: */

ALTER TABLE workspace.default.faker
SET TBLPROPERTIES ('nome_funcionarios' = 'string');


ALTER TABLE workspace.default.faker ALTER COLUMN nome SET TAGS ("nomes classificados");