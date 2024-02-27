# Databricks notebook source
# MAGIC %run "./includes/configuration"

# COMMAND ----------

#processed_folder_path
raw_folder_path

# COMMAND ----------

races_df = spark.read.option('header', True).csv(f"{raw_folder_path}/races.csv")
races_df.display()

# COMMAND ----------

# races_filtered_df = races_df.filter("year = 2019")
# races_filtered_df = races_df.filter("year = 2019 & round <= 5")

races_filtered_df = races_df.filter(races_df["year"] == 2019)
# races_filtered_df = races_df.filter((races_df["year"] == 2019) & (races_df["round"] <= 5))

races_filtered_df.display()

# COMMAND ----------

races_df = spark.read.option('header', True).csv(f"{raw_folder_path}/races.csv").filter("year = 2019")   # .filter(races_df["year"] == 2019) NAO PORQUE races_df nao foi definido
races_df.display()

# COMMAND ----------

circuits_df = spark.read.option('header', True).csv(f"{raw_folder_path}/circuits.csv")
circuits_df.display()

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuitId == races_df.circuitId, "inner")
races_circuits_df.display()

# COMMAND ----------



# COMMAND ----------

circuits_df = spark.read.option('header', True).csv(f"{raw_folder_path}/circuits.csv").withColumnRenamed("name", "circuit_name")
#circuits_df.display()

races_df = spark.read.option('header', True).csv(f"{raw_folder_path}/races.csv").filter("year = 2019").withColumnRenamed("name", "race_name")
#races_df.display()

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuitId == races_df.circuitId, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round )

races_circuits_df.display()

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuitId == races_df.circuitId, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round )

races_circuits_df.display()

# COMMAND ----------

races_circuits_df.select("circuit_name").show()

# COMMAND ----------

# inner
race_circuits_df = circuits_df.join(races_df, circuits_df.circiut_id == races_df.circuit_id, 'inner') \
                               .select(circuits_df.circuits_name, circuits_df.location, races_df.race_name, races_df.round)

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_df, 
                                    circuits_df.circiut_id == races_df.circuit_id, 'left') \
                               .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# Right Outer Join

# Full Outer Join

# COMMAND ----------

# Join Transformation - Semi, Anti, Cross, Join

# Semi Joins


races_circuits_df.display()

# COMMAND ----------

# Semi Joins

circuits_df = spark.read.option('header', True).csv(f"{raw_folder_path}/circuits.csv").withColumnRenamed("circuitId", "circuits_id").withColumnRenamed("name", "circuits_name").withColumnRenamed("name", "circuits_name")
# circuits_df.display()
# circuitId circuitRef name location country lat lng alt url raceId year

races_df = spark.read.option('header', True).csv(f"{raw_folder_path}/races.csv").filter("year = 2019").withColumnRenamed("name", "race_name").withColumnRenamed("circuitId", "circuits_id")
#races_df.display()
# rraceId year round circuitId name date time url

races_circuits_df = circuits_df.join(races_df, circuits_df.circuits_id == races_df.circuits_id , "semi") \
                               .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country)

races_circuits_df.display()

# COMMAND ----------

from IPython.display import Image 
from IPython.core.display import HTML 
Image(url= "https://exploratory.io/note/kanaugust/Introduction-to-Join-Adding-Columns-from-Another-Data-Frame-iUm5YNI7RK/note_content/libs/exploratory/images/p3.png")

# COMMAND ----------

# Anti Joins

# Semi Joins

circuits_df = spark.read.option('header', True).csv(f"{raw_folder_path}/circuits.csv").withColumnRenamed("circuitId", "circuits_id").withColumnRenamed("name", "circuits_name").withColumnRenamed("name", "circuits_name")
# circuits_df.display()
# circuitId circuitRef name location country lat lng alt url raceId year

races_df = spark.read.option('header', True).csv(f"{raw_folder_path}/races.csv").filter("year = 2019").withColumnRenamed("name", "race_name").withColumnRenamed("circuitId", "circuits_id")
#races_df.display()
# rraceId year round circuitId name date time url

race_circuits_df = races_df.join(circuits_df,
                                circuits_df.circuits_id == races_df.circuits_id, "anti")
race_circuits_df.display()

# COMMAND ----------

# Cross Joins

race_circuits_df = races_df.crossJoin(circuits_df)
race_circuits_df.display()

# COMMAND ----------

from IPython.display import Image 
from IPython.core.display import HTML 
Image(url= "https://www.educative.io/api/edpresso/shot/5392077896548352/image/5672062263754752")

# COMMAND ----------

