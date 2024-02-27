# Databricks notebook source
list = ["trs/rfb_cnpj/cadastro_estbl_f","trs/rfb_cnpj/cadastro_empresa_f","trs/rfb_cnpj/cadastro_simples_f","trs/rfb_cnpj/cadastro_socio_f","trs/me/rais_estabelecimento"]
for x in list:
  var_adls_uri = f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/{x}'
  Base = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
  #Base.display()
  Base.coalesce(1).write.format('csv').save(f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/celso/{x}', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

  Base.coalesce(1).write.format('csv').save(f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/celso/{x}', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

#%run ./pyspark_read
#ONI.DADOS('/tmp/dev/uld/fiesc/rfb/rfb_fato/hot/','parquet',',').display()

# COMMAND ----------

#https://python-course.eu/oop/class-instance-attributes.php
#https://www.pythontutorial.net/python-oop/python-class-attributes/#class Robot:

# COMMAND ----------

#    class DATA_LAKE(object):
#      
#     ################################## 
#     ################################## 
#      
#      # class attribute
#      # self.AZURE_PATH
#      AZURE_PATH = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#      
#     ################################## 
#     ##################################  
#      
#     #  methods in python are used for string representation of a string
#      def __str__(self):
#        return 'okay'
#      
#      #  methods in python are used for string representation of a string
#      def __repr__(self):
#        return 'okay' 
#      
#      # method
#      # instance method to access instance variable          
#      def __init__(self):
#                   #, value_0):
#        #self.method_value = value_0
#        #print(self.method_value)
#      
#        #print(self.AZURE_PATH) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#        
#        self.value_1 = DATA_LAKE.AZURE_PATH
#        #print(self.value_1) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#       
#      ################################## 
#      ##################################
#                   
#      # method
#      # instance method to access instance variable              
#      def DADOS(self, datalake_path, file):
#        self.datalake_path = datalake_path
#        df =  spark.read.format("parquet").option("header","true").option('sep',',').load( self.value_1 + self.datalake_path)
#        #df = spark.read.format("parquet").option("encoding", "ISO-8859-1").option("header","true").option('sep',';').load(path)
#        #df = spark.read.format('json').load("dbfs:/mnt/diags/logs/resourceId=/SUBSCRIPTIONS/<subscription>/RESOURCEGROUPS/<resource group>/PROVIDERS/MICROSOFT.APIMANAGEMENT/SERVICE/<api service>/y=*/m=*/d=*/h=*/m=00/PT1H.json")
#        print(self.AZURE_PATH)
#        return df
#      
#     ################################## 
#     ################################## 
#      
#      # function
#      def Definir():
#        pass
#      
#      
#    if __name__ == '__main__':
#      ONI = DATA_LAKE()

# COMMAND ----------

#class DATA_LAKE(object):
#  
# ################################## 
# ################################## 
#  
#  # class attribute
#  # self.AZURE_PATH
#  AZURE_PATH = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#  
# ################################## 
# ##################################  
#  
# #  methods in python are used for string representation of a string
#  def __str__(self):
#    return 'okay'
#  
#  #  methods in python are used for string representation of a string
#  def __repr__(self):
#    return 'okay' 
#  
#  # method
#  # instance method to access instance variable          
#  def __init__(self, value_0):
#    self.method_value = value_0
#    print(self.method_value)
#  
#    print(self.AZURE_PATH) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#    
#    self.value_1 = DATA_LAKE.AZURE_PATH
#    print(self.value_1) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#   
#  ################################## 
#  ##################################
#               
#  # method
#  # instance method to access instance variable              
#  def DADOS(self, datalake_path ):
#    self.datalake_path = datalake_path
#    df =  spark.read.format("csv").option("header","true").option('sep',',').load( self.value_1 + self.datalake_path)
#    #df = spark.read.format("parquet").option("encoding", "ISO-8859-1").option("header","true").option('sep',';').load(path)
#    #df = spark.read.format('json').load("dbfs:/mnt/diags/logs/resourceId=/SUBSCRIPTIONS/<subscription>/RESOURCEGROUPS/<resource group>/PROVIDERS/MICROSOFT.APIMANAGEMENT/SERVICE/<api service>/y=*/m=*/d=*/h=*/m=00/PT1H.json")
#    
#    return print(self.AZURE_PATH)
#  
# ################################## 
# ################################## 
#  
#  # function
#  def Definir():
#    pass
#  
#  
#if __name__ == '__main__':
#  #OBSERVATORIO_DATA_LAKE = DATA_LAKE('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net')
#  DATA_LAKE('Teste').DADOS('/uds/uniepro/data/Rais_1000/')

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


class DATA_LAKE(object):
  
 # -------------------------------------------------------------------------------------------------------
  
  # class attribute
  # self.AZURE_PATH
  AZURE_PATH = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  

 # ------------------------------------------------------------------------------------------------------- 
  
 #  methods in python are used for string representation of a string
  def __str__(self):
    return 'okay'
  
  #  methods in python are used for string representation of a string
  def __repr__(self):
    return 'okay' 
  
  # method
  # instance method to access instance variable          
  def __init__(self):
               #, value_0):
    #self.method_value = value_0
    #print(self.method_value)
  
    #print(self.AZURE_PATH) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    
    self.value_1 = DATA_LAKE.AZURE_PATH
    #print(self.value_1) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    
  # -------------------------------------------------------------------------------------------------------
  
  # method
  # instance method to access instance variable              
  def DADOS(self, datalake_path, file_format, delimiter ):
    self.DADOS_datalake_path = datalake_path
    self.DADOS_file_format = file_format
    self.DADOS_delimiter = delimiter
    df =  spark.read.format(self.DADOS_file_format).option("header","true").option('sep',self.DADOS_delimiter).option('encoding',"ISO-8859-1").load( self.value_1 + self.DADOS_datalake_path)
    #df = spark.read.format("parquet").option("encoding", "ISO-8859-1").option("header","true").option('sep',';').load(path)
    #df = spark.read.format('json').load("dbfs:/mnt/diags/logs/resourceId=/SUBSCRIPTIONS/<subscription>/RESOURCEGROUPS/<resource group>/PROVIDERS/MICROSOFT.APIMANAGEMENT/SERVICE/<api service>/y=*/m=*/d=*/h=*/m=00/PT1H.json")
    print(self.AZURE_PATH)
    return df
  
 # -------------------------------------------------------------------------------------------------------
  
  # function
  def Definir():
    pass
  
  
if __name__ == '__main__':
  ONI = DATA_LAKE()
  ONI.DADOS('/uds/uniepro/data/Rais_1000/','csv',',').display()

# COMMAND ----------

#dbutils.library.installPyPI("regex")

# COMMAND ----------

#from unicodedata import normalize

# COMMAND ----------

#import regex

# COMMAND ----------

#def __normalize_str(_str):
#
#    return regex.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
#
#                  .encode('ASCII', 'ignore')
#
#                  .decode('ASCII')
#
#                  .replace(' ', '_')
#
#                  .replace('-', '_')
#
#                  .upper())

# COMMAND ----------

#def rename_columns(df):
#  from unicodedata import normalize  
#  import re
#  
#  regex = re.compile(r'[.,;{}()\n\t=]')
#  for col in df.columns:
#      col_renamed = regex.sub('', normalize('NFKD', col.strip())
#                             .encode('ASCII', 'ignore')        
#                             .decode('ASCII')       
#                              
#                             .replace(' ', '_')                    
#                             .replace('-', '_')
#                             .replace('/', '_')
#                             .replace('$', 'S')
#                             .upper())
#      df = df.withColumnRenamed(col, col_renamed)
#  return df

# COMMAND ----------

#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


class DATA_LAKE(object):
  
 # -------------------------------------------------------------------------------------------------------
  
  # class attribute
  # self.AZURE_PATH
  AZURE_PATH = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  

 # ------------------------------------------------------------------------------------------------------- 
  
 #  methods in python are used for string representation of a string
  def __str__(self):
    return 'okay'
  
  #  methods in python are used for string representation of a string
  def __repr__(self):
    return 'okay' 
  
  # method
  # instance method to access instance variable          
  def __init__(self):
               #, value_0):
    #self.method_value = value_0
    #print(self.method_value)
  
    #print(self.AZURE_PATH) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    
    self.value_1 = DATA_LAKE.AZURE_PATH
    #print(self.value_1) # get value AZURE_PATH 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
    
  # -------------------------------------------------------------------------------------------------------
  
  # method
  # instance method to access instance variable              
  def DADOS(self, datalake_path, file_format, delimiter,header,true ):
    self.DADOS_datalake_path = datalake_path
    self.DADOS_file_format = file_format
    self.DADOS_delimiter = delimiter
    self.header = header
    self.true = true
    #print(df)
    df =  spark.read.format(self.DADOS_file_format).option(self.header,self.true).option('sep',self.DADOS_delimiter).option('encoding',"ISO-8859-1").load( self.value_1 + self.DADOS_datalake_path)
    #df = spark.read.format("parquet").option("encoding", "ISO-8859-1").option("header","true").option('sep',';').load(path)
    #df = spark.read.format('json').load("dbfs:/mnt/diags/logs/resourceId=/SUBSCRIPTIONS/<subscription>/RESOURCEGROUPS/<resource group>/PROVIDERS/MICROSOFT.APIMANAGEMENT/SERVICE/<api service>/y=*/m=*/d=*/h=*/m=00/PT1H.json")
    print(self.AZURE_PATH)
    return df
  
 # -------------------------------------------------------------------------------------------------------
  
  # function
  def Definir():
    pass
  
  
if __name__ == '__main__':
  ONI = DATA_LAKE()
  #print(ONI)
  ONI.DADOS('/raw/usr/uniepro/rfb/siafi/','parquet',',','header','true').display()

# COMMAND ----------

