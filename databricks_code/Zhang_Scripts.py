# Databricks notebook source
# MAGIC %md
# MAGIC # Class Object HEADER

# COMMAND ----------

class Header(object):
  class_attributes = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  def __init__(self):
    self.value__abfss = Header.class_attributes
    
    
    
  def __str__(self):
    return self.value__abfss
  
  def path(self, format, header, true, sep_delim, character, path_suffix ):
    self.format = format
    
    self.header = header
    self.true = true
    
    self.sep_delim = sep_delim
    self.character = character 
    
    self.path_suffix = path_suffix 
  
    print(self.value__abfss)
    df =  spark.read.format(self.format).option(self.header,self.true).option(self.sep_delim ,self.character).option('encoding',"ISO-8859-1").load( self.value__abfss + self.path_suffix)
    #return self.format, self.header, df
    return df
    
    
  # function
  def Definir():
    pass

# COMMAND ----------



# COMMAND ----------

class Header(object):
  class_attributes = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  def __init__(self):
    self.value__abfss = Header.class_attributes
    
    
    
  def __str__(self):
    return self.value__abfss
  
  def path(self, format, header, true, sep_delim, character, path_suffix ):
    self.format = format
    
    self.header = header
    self.true = true
    
    self.sep_delim = sep_delim
    self.character = character 
    
    self.path_suffix = path_suffix 
  
    print(self.value__abfss)
    df =  spark.read.format(self.format).option(self.header,self.true).option(self.sep_delim ,self.character).option('encoding',"ISO-8859-1").load( self.value__abfss + self.path_suffix)
    #return self.format, self.header, df
    return df
    
    
  # function
  def Definir():
    pass
  
  
  
if __name__ == '__main__':
  #print(Class())
  HEADER = Header()
  #print(HEADER)
  
  #HEADER.path('teste')
  HEADER.path("parquet",
              "header",
              "true",
              "sep",
              ";",
              "/uds/oni/rede/observatorio_fiec/rais_regiao_nordeste/").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Rafael Inguru Investimento

# COMMAND ----------

# Esperando acesso

# COMMAND ----------

# MAGIC %md
# MAGIC # Danilo FIESC

# COMMAND ----------

import asyncio
import time
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from typing import List, Dict
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff,col,when,greatest


async def function():
  return 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

async def main(x):
  azure = await function()
  return azure + x

if __name__=='__main__':
  path = await main('/uds/oni/rede/observatorio_fiesc/rais_regiao_sul/')
  df = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(path)
  df.display()

# COMMAND ----------

import asyncio
import time
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from typing import List, Dict
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp, lit
from pyspark.sql.functions import datediff,col,when,greatest

 
 
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/rede/observatorio_fiec/rais_regiao_nordeste/'
#F = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
#F.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Windows Function

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC

# COMMAND ----------

# Import PySpark
import pyspark
from pyspark.sql import SparkSession

#Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# COMMAND ----------

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

# COMMAND ----------

import asyncio
import time
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from typing import List, Dict
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff,col,when,greatest

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp, lit


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/inss/cat/NR_ANO=2023/NM_ARQUIVO=NONE/'
F = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
F.display()

# COMMAND ----------

F.select(max(F.DT_ACIDENTE)).display()

# COMMAND ----------

F.select(min(F.DT_ACIDENTE)).display()

# COMMAND ----------

F.select(col('CD_COMP_ACIDENTE'))

# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp, lit


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_estbl_f'
CADASTRO_ESTBL_F = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
CADASTRO_ESTBL_F = Base_cadastro_estbl_f.withColumn("DT_SIT_CADASTRAL", to_date(Base_cadastro_estbl_f.DT_SIT_CADASTRAL))
CADASTRO_ESTBL_F = Base_cadastro_estbl_f.filter(Base_cadastro_estbl_f["DT_SIT_CADASTRAL"] >= lit('2022-01-01'))
CADASTRO_ESTBL_F.display()
print(CADASTRO_ESTBL_F.count())

# COMMAND ----------

#"trs/rfb_cnpj/cadastro_estbl_f"
#"trs/rfb_cnpj/cadastro_empresa_f",
#"trs/rfb_cnpj/cadastro_simples_f"
#"trs/rfb_cnpj/cadastro_socio_f"


#"trs/me/rais_estabelecimento"

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_empresa_f'
CADASTRO_EMPRESA_F = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
CADASTRO_EMPRESA_F.display()

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_simples_f'
CADASTRO_SIMPLES_F = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
CADASTRO_SIMPLES_F.display()

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_socio_f'
CADASTRO_SOCIO_F = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
CADASTRO_SOCIO_F.display()

# COMMAND ----------

df_0 = CADASTRO_ESTBL_F.join(CADASTRO_EMPRESA_F, on='CD_CNPJ_BASICO', how='left').drop()

# COMMAND ----------

df_1 = df_0.join(CADASTRO_SIMPLES_F, on='CD_CNPJ_BASICO', how='left')

# COMMAND ----------

df_2 = df_0.join(CADASTRO_SOCIO_F, on='CD_CNPJ_BASICO', how='left').drop(CADASTRO_SOCIO_F.CD_PAIS)

# COMMAND ----------

df_2.count()

# COMMAND ----------

df_2.display()

# COMMAND ----------

df_3 = df_2.drop('kv_process_control')

# COMMAND ----------

df_4 = df_3.drop('dh_insercao_trs')

# COMMAND ----------

df_4.printSchema()

# COMMAND ----------

df_4.repartition(10).write.format('csv').save(f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/celso/10_repartition', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

df_4.count()

# COMMAND ----------

#"trs/rfb_cnpj/cadastro_estbl_f", ESTOU PEGANDO a DT_SIT_CADASTRAL : pode ser essa data

from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp, lit


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_estbl_f'
Base_cadastro_estbl_f = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)
Base_cadastro_estbl_f = Base_cadastro_estbl_f.withColumn("DT_SIT_CADASTRAL", to_date(Base_cadastro_estbl_f.DT_SIT_CADASTRAL))
Base_cadastro_estbl_f = Base_cadastro_estbl_f.filter(Base_cadastro_estbl_f["DT_SIT_CADASTRAL"] >= lit('2022-01-01'))
#Base_cadastro_estbl_f.display()
print(Base_cadastro_estbl_f.count())

#Base_cadastro_estbl_f.select(min(col('DT_SIT_CADASTRAL'))).show()
#Base_cadastro_estbl_f.select(max(col('DT_SIT_CADASTRAL'))).show()

#Base_cadastro_estbl_f.coalesce(1).write.format('csv').save(f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/celso/Base_cadastro_estbl_f', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff,col,when,greatest


var_adls_uri = f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_empresa_f'
Base_cadastro_empresa_f = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)


#Base_cadastro_empresa_f = Base_cadastro_empresa_f.withColumn("dh_insercao_trs",to_date(col("dh_insercao_trs")))

t = Base_cadastro_empresa_f.sample(0.1)

t.coalesce(1).write.format('csv').save(f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/celso/cadastro_empresa_f_5MM', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

t.count()

# COMMAND ----------

#list = ["trs/rfb_cnpj/cadastro_estbl_f","trs/rfb_cnpj/cadastro_empresa_f","trs/rfb_cnpj/cadastro_simples_f","trs/rfb_cnpj/cadastro_socio_f","trs/me/rais_estabelecimento"]


var_adls_uri = f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_estbl_f'
Base = (spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri))
Base.printSchema()

#Base = Base.withColumn("dh_insercao_trs", to_date(Base.dh_insercao_trs))
#Base = Base.filter(Base["dh_insercao_trs"] >= lit('2022-01-01'))
Base.display()
Base.printSchema()
Base.count()

# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp, lit


#list = ["trs/rfb_cnpj/cadastro_estbl_f","trs/rfb_cnpj/cadastro_empresa_f","trs/rfb_cnpj/cadastro_simples_f","trs/rfb_cnpj/cadastro_socio_f"]



var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/rfb_cnpj/cadastro_simples_f"
Base_trs/rfb_cnpj/cadastro_simples_f = spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri)


Base_trs/rfb_cnpj/cadastro_simples_f.display()
Base_trs/rfb_cnpj/cadastro_simples_f.count()


# COMMAND ----------

Base = Base.withColumn("DT_SIT_CADASTRAL", to_date(Base.DT_SIT_CADASTRAL))
Base.display()

# COMMAND ----------

B = Base.filter(Base["DT_SIT_CADASTRAL"] >= lit('2022-01-01'))

# COMMAND ----------

B.count()

# COMMAND ----------

#list = ["trs/rfb_cnpj/cadastro_estbl_f","trs/rfb_cnpj/cadastro_empresa_f","trs/rfb_cnpj/cadastro_simples_f","trs/rfb_cnpj/cadastro_socio_f","trs/me/rais_estabelecimento"]

list = ["trs/rfb_cnpj/cadastro_estbl_f","trs/rfb_cnpj/cadastro_empresa_f","trs/rfb_cnpj/cadastro_simples_f","trs/rfb_cnpj/cadastro_socio_f"]
for x in list:
  var_adls_uri = f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/{x}'
  Base = (spark.read.format("parquet").option("header","true").option("encoding", "utf-8").option('sep',';').load(var_adls_uri))
  Base.display()

# COMMAND ----------

  #print(f'{x}')
  #rows = Base.count()
  #print(f"DataFrame Rows count : {rows}")
  Base.coalesce(1).write.format('csv').save(f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/celso_teste/{x}', sep=";", header = True, mode='overwrite', encoding='ISO-8859-1')

# COMMAND ----------

# MAGIC %md
# MAGIC # createOrReplaceTempView

# COMMAND ----------

data = [(1, 'maheer', 2000),(2, 'asi', '3000')]
schema = ['id', 'name', 'salary']

df = spark.createDataFrame(data, schema)

df

# COMMAND ----------

df.createOrReplaceTempView('employees')

# COMMAND ----------

help(df.createOrReplaceTempView)

# COMMAND ----------

df1 = spark.sql('SELECT * FROM employees')
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT id, UPPER(name) FROM employees

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # utils class

# COMMAND ----------


extra_words = ['area','conhecimento','ter','sao','paulo','profissional','work',\
               'trabalho','best','hierarquico','atuar','voce','areas','equipe',\
               'team','fazer','sera','ensino','medio','completo','completo','visando',\
               'experiencia','superior','empresa','documentos','realizar','desejavel',\
               'necessario','responsavel','novos','00','ser','possuir','sobre','atraves',\
               'boa','vaga','vagas','nao','bem','demais','atuacao','manter','dia','ira','acordo',\
               'todos','todo','bom','time','fim','alem','ate','100','6x1','pois','partir','local',\
               '50','frase','onde','fara','15','não','informado','world','america','make',\
               'cliente','clients','every','want','leading','vale','member','teams','looking',\
               '&nbsp','nbsp','100','1','end','dias','tipo','gostar','agora','faz','outras',\
               'outros','sp','segunda','terca','quarta','quinta','sexta','sabado','domingo','junto','afins','cada']
stopwords = get_stop_words('portuguese') + get_stop_words("english") + extra_words

class utils:
  '''
  Class that packages the project's global settings and utility functions.
  
  Attributes
  -----------
  No attributes.
  '''
  
  azure_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  data_path = azure_path + '/uds/uniepro/inteligencia_ocupacional/monitor_de_emprego.csv/'
  biz_path = azure_path + '/uds/uniepro/inteligencia_ocupacional/monitor_vagas_v2/biz'
  dict_path  = azure_path + '/uds/uniepro/inteligencia_ocupacional/monitor_vagas_v2/program_files/dicionarios/'
  pattern_path = azure_path + '/uds/uniepro/inteligencia_ocupacional/monitor_vagas_v2/program_files/padroes_regex/'
  tests_path = azure_path + '/uds/uniepro/inteligencia_ocupacional/monitor_vagas_v2/program_files/dados_teste/'
  uds_path = azure_path + '/uds/uniepro/inteligencia_ocupacional/monitor_vagas_v2/uds'
  trs_path = azure_path + '/uds/uniepro/inteligencia_ocupacional/monitor_vagas_v2/trs'
  
  training_area_path = azure_path + '/tmp/dev/raw/usr/uniepro/cbo_cadastro_mapa/'
  jobs_path = biz_path + '/vagas2'
  cbo4_path = biz_path + '/titulo_cbo4'
  
  
  def wordCount(df, col):
    '''
    wordCount(df, col)
    
    Generate word count Dataframe field col in dataframe df.
    
    Parameters
    ----------
    df: spark.Dataframe
      Base spark dataframe.
    col: str
      Dataframe column name to be counted.
      
    Raises
    ------
    No Raises.
    
    Returns
    -------
    count_df.
    '''
    
    count_df = df.withColumn('word', F.explode(F.split(F.col(col), ' ')))\
    .groupBy('word')\
    .count()
    
    
    
    return count_df
  @staticmethod
  def wordCountDisplay(df, col):
    '''
    wordCountDisplay(df, col)
    
    Displays word count of campo of dataframe df.
    
    Parameters
    ----------
    df: spark.Dataframe
      Base spark dataframe.
    col: str
      Dataframe column name to be counted.
      
    Raises
    ------
    No Raises.
    
    Returns
    -------
    No Returns.
    '''
    utils.wordCount(df, col).sort('count', ascending=False).display()
    return None
  
  @staticmethod
  def excelToParquet(file_path):
    '''
    excelToParquet(file_path)
    
    Converts a excel file with a single sheet to a parquet file.
    Saves the parquet file in the same path as the original excel file.
    All fields are converted to str type. 
    
    Parameters
    --------------
    file_path: str
      Path to excel file in the datalake.
      
    Raises
    -------------
    No Raises.
    
    Returns
    ---------------
    path_parquet : str
      Path string to the file saved in the datalake.
    '''
    
    #copy from datalake to dbfs
    path_dir, file_name = os.path.split(file_path)
    path_dbfs = '/tmp/' + file_name

    dbutils.fs.cp(file_path, path_dbfs)  
    pd_df = pd.read_excel('/dbfs' + path_dbfs, engine='openpyxl').astype(str)
    sparkDF= spark.createDataFrame(pd_df)

    path_parquet = path_dir + '/parquet/' + file_name
    sparkDF.write.parquet(path_parquet, 'overwrite')
    
    return path_parquet
  
  
  @staticmethod
  def saveExcelToLake(pd, path_lake):
    '''
    saveExcelToLake(pd, path_lake)
    
    Saves pandas dataframe as excel to path lake.
    
    Parameters
    ----------
    pd: pandas.DataFrame
      Dataframe object to be saved.
    path_lake: str
      Path File in the datalake. 
      
    Raises
    ------
    No Raises.
    
    Returns
    -------
    result : str
      Path string to the saved file in the datalake.
    '''
  
    result = ""
    try:
      #set variables
      name = str(time.time()).replace('.','-') + '.xlsx'
      file_name_local_tmp = '/local_disk0/tmp/' + name
      file_name_dbfs_tmp = '/dbfs/tmp/' + name
      
      #save object to local drive
      pd.to_excel(file_name_local_tmp, engine='xlsxwriter')   

      #copy excel file from local disk to dbfs
      shutil.copyfile(file_name_local_tmp, file_name_dbfs_tmp)  
      #copy file from dbfs to datalake
      dbutils.fs.cp(file_name_dbfs_tmp.replace('/dbfs',''), path_lake)
      
      #delete file from dbfs
      dbutils.fs.rm(file_name_dbfs_tmp)
      #delete file from local disk
      os.remove(file_name_local_tmp)
      
      #success exit message
      result = "Arquivo salvo: " + path_lake    
      

    except Exception as e:
      print(e)

    return result

  def getResultsFromPD(pd_true, pd_pred):
    '''
    getResultsFromPD(pd_true, pd_pred)
    
    Computes recall, precision, accuracy and confusion from dataframes pd_true and pd_pred.
    Generates a row of results for each column in pd_true.

    Parameters
    ----------
    pd_true: pandas.DataFrame
      Dataframe with truth values.
    pd_pred: pandas.DataFrame
      Dataframe with predicted values.
      
    Raises
    ------
    No Raises.

    Returns
    -------
    pd.Dataframe
      Pandas dataframe with computed results.
    '''
    
    results_dict = {}
    for target in pd_true.columns[1:]:
      y_true = pd_true[target]
      y_pred = pd_pred[target]
      recall = recall_score(y_true, y_pred)
      precision = precision_score(y_true, y_pred)
      accuracy = accuracy_score(y_true, y_pred)
      confusion_array = confusion_matrix(y_true, y_pred)
      results_dict[target] = {
        'total_true': y_true.sum(),
        'total_pred': y_pred.sum(),
        'recall': recall,
        'precision': precision,
        'accuracy' : accuracy,
        'confusion' : confusion_array
      }
    results_pd = pd.DataFrame(results_dict).T
    
    return results_pd
  
  def generateSample(source_path, sample_size, seed, col_select=['*'], col_label=[]):
    '''
    generateSample(source_path, sample_size, seed, col_select=['*'], col_label=[])
    
    Generates a random sample without replacement from a DataFrame.
    
    Parameters
    ----------
    source_path: str
      path to the source DataFrame.
    sample_size: int
      Size of the sample in number of rows, will return an approximate number.
    seed: int
      Seed to generate sample.
    col_select: list[str]
      List of column names that will make the sample.
    col_label: list['str']
      List of column names that will generate new empty collumnns in the sample.
    
    Raises
    ------
    No Raises.
    
    Returns
    -------
    spark.DataFrame
      The sample.
    '''
    
    df = spark.read.parquet(source_path)
    df_size = df.count()
    ratio = sample_size/df_size
    sample = df.sample(ratio, seed).select(*col_select)
    for col in col_label:
      sample = sample.withColumn(col, F.lit(''))

    return sample
  
  @staticmethod
  def makeTrans():
    '''
    makeTrans()
    
    Checks and removes accented characters.
    
    Parameters
    ----------
    No Parameters.
     
    Raises
    ------
    No Raises.
     
    Returns
    -------
    str, str
      The result of the processed string, including the string without accent.
    '''
    
    matching_string = ""
    replace_string = ""
    
    for i in range(ord(" "), sys.maxunicode):
      name = unicodedata.name(chr(i), "")
      if "WITH" in name:
        try:
          base = unicodedata.lookup(name.split(" WITH")[0])
          matching_string += chr(i)
          replace_string += base
        except KeyError:
          pass

    return matching_string, replace_string
  
  @staticmethod
  def cleanText(c):
    '''
    cleanText(c)
    
    Performs text cleaning, excluding accents.
    
    Parameters
    ----------
    c: str
      Column name of the dataframe that contains the text field.
    
    Raises
    ------
    No Raises.
     
    Returns
    -------
    spark.Series
      The text column treated.
    '''
    
    matching_string, replace_string = utils.makeTrans()
    return translate(
        regexp_replace(c, "\p{M}", ""), 
        matching_string, replace_string
    ).alias(c)
    
  @staticmethod
  def preProcessingText(df, col_name, new_col_name):
    '''
    preProcessingText(df, col_name, new_col_name)
    
    Performs pre-processing of the text field, including accent, punctuation, numbers and special characters removal,
    lowercase text and whitespace removal at the beginning and end of the text. 
    
    Parameters
    ----------
    df: spark.Dataframe
      Base spark dataframe with the text field.
    col_name: str
      Column name with text field to be processed.
    new_col_name: str
      Name of the new column that will be pre-processed.
     
    Raises
    ------
    No Raises.
     
    Returns
    -------
    pandas.DataFrame
      Base dataframe with the pre-processed text field.
    '''
    
    df = df.withColumn(new_col_name, lower(utils.cleanText(col_name)))
    
    df_pd = df.toPandas()
    
    df_pd[new_col_name] = df_pd[new_col_name].str.replace("[^a-zA-Z#]", " ")
    
    df_pd[new_col_name] = df_pd[new_col_name].str.strip()
    
    return df_pd
    
  def removeStopWords(df_pd, col_name, new_col_name, extra_words=[]):
    '''
    removeStopWords(df_pd, col_name, new_col_name)
    
    Removes stop-words from a text field.
    
    Parameters
    ----------
    df_pd: pandas.DataFrame
      Base spark dataframe with the text field.
    col_name: str
      Name of the column with text field to be processed.
    new_col_name: str
      Name of the new column that will be pre-processed.
    extra_words: list
      List of extra words to be removed.
     
    Raises
    ------
    No Raises.
     
    Returns
    -------
    pandas.DataFrame
      Base dataframe with the text field without stop-words.
    '''
    
    stop_words = get_stop_words('portuguese') + extra_words

    doc_tokenized = df_pd[col_name].apply(lambda doc: doc.split())
    doc_tokenized = doc_tokenized.apply(lambda x: [item for item in x if item not in stop_words])
    
    doc_detokenized = []
    for i in range(len(df_pd)):
      doc = ' '.join(doc_tokenized[i])
      doc_detokenized.append(doc)
      
    df_pd[new_col_name] = doc_detokenized
    
    return df_pd
  
  def stemming(df_pd, col_name, new_col_name):
    '''
    stemming(df_pd, col_name, new_col_name)
    
    Apply the stemming technique, reducing words by focusing on their meaning,
    may generate grammatically incorrect words.
    
    Parameters
    ----------
    df_pd: pandas.DataFrame
      Base pandas dataframe with the text field.
    col_name: str
      Name of the column with the text field to apply stemming.
    new_col_name: str
      Name of the new column that will have the stemmed text.
     
    Raises
    ------
    No Raises.
     
    Returns
    -------
    pandas.DataFrame
      Base dataframe with the text stemmed in a new column.
    '''
    
    nltk.download('rslp')
    
    stemmer = nltk.stem.RSLPStemmer()
    
    text_stem = []
    for text in df_pd[col_name]:
      text_stem.append(' '.join([stemmer.stem(word) for word in text.split()]))

    df_pd[new_col_name] = text_stem
    
    return df_pd
  
  def lemmatization(df_pd, col_name, new_col_name):
    '''
    lemmatization(df_pd, col_name, new_col_name)
    
    Apply the lemmatization technique, reducing words by focusing on their meaning, 
    pulling out all the inflections and getting to the lemma.
    
    Parameters
    ----------
    df_pd: pandas.DataFrame
      Base pandas dataframe with the text field.
    col_name: str
      Name of the column with the text field to apply lemmatization.
    new_col_name: str
      Name of the new column that will have the lemmatized text.
     
    Raises
    ------
    No Raises.
     
    Returns
    -------
    pandas.DataFrame
      Base dataframe with the text lemmatized in a new column.
    '''
    
    stanza.download('pt')
    nlp = stanza.Pipeline('pt')
    
    text_lemma = []

    for text in df_pd[col_name]:
      lemma = ''
      for sent in nlp(text).sentences:
        for word in sent.words:
          lemma += word.lemma + ' '  
      text_lemma.append(lemma.strip())
      
    df_pd[new_col_name] = text_lemma
    
    return df_pd
  
  def acronym_in_full(df_pd, col_name, acronym_dict):
    '''
    acronym_in_full(df_pd, col_name, acronym_dict)
    
    Replaces acronyms with their full name in a text field, 
    according to the definitions given in the dictionary.
    
    Parameters
    ----------
    df_pd: pandas.DataFrame
      Base pandas dataframe with the text field.
    col_name: str
      Name of the column with text field to be processed.
    acronym_dict: dict
      The acronyms and their respective names in full.
     
    Raises
    ------
    No Raises.
     
    Returns
    -------
    pandas.DataFrame
      Base dataframe with the text field processed.
    '''

    doc_tokenized = df_pd[col_name].apply(lambda doc: doc.split())
    doc_tokenized = doc_tokenized.apply(lambda x: [acronym_dict[item] if item in acronym_dict else item for item in x])
    
    doc_detokenized = []
    for i in range(len(df_pd)):
      doc = ' '.join(doc_tokenized[i])
      doc_detokenized.append(doc)
      
    df_pd[col_name] = doc_detokenized
    
    return df_pd
  
  def createCategoricalColumn(df, new_col_name, col_list, content_list, content_default):
    '''
    createCategoricalColumn(df, new_col_name, col_list, content_list)
    
    Create categorical column from binary columns and duplicate records according to classification.
    
    Parameters
    ----------
    df: spark.Dataframe
      Base spark dataframe with the binary columns.
    new_col_name: str
      Name of the categorical column that will be created.
    col_list: list
      Name of the binary columns.
    content_list: list
      Content to be filled in the categorical column referring to each binary classification.
    content_default: str
      Content of the categorical column if there is no classification for the job.

    Raises
    ------
    No Raises.
     
    Returns
    -------
    spark.Dataframe
      Base dataframe with the categorical column and the duplicated records.
    '''
    
    new_df = df
    
    new_df = new_df.withColumn(new_col_name, F.array())
    for i, col in enumerate(col_list):
      new_df = new_df.withColumn(new_col_name, F.when(F.col(col)==1, array_union(F.col(new_col_name),F.array(F.lit(content_list[i]))))
                                                .otherwise(F.col(new_col_name)))
    
    new_df = new_df.withColumn(new_col_name, F.when(F.size(F.col(new_col_name))==0, array_union(F.col(new_col_name),F.array(F.lit(content_default))))
                                              .otherwise(F.col(new_col_name)))
    
    new_df = new_df.withColumn(new_col_name, explode(F.col(new_col_name)))
    
    return new_df
  
  def wordCloud(tup, max_words=50, width=400,
                      height=200, random_state=42,
                      max_font_size=40):
    '''
    displayWordCloud(tup)
    Displays word cloud using WordCout Library
    
    
    Parameters
    ----------
    tup: dict
      key values pairs to generate word cloud. 
      
    Raises
    ------
    No Raises
    
    Return
    ------
    wordcloud object
    '''
    wordcloud = WordCloud(background_color='white',
                          max_words=max_words,
                          width = width,
                          height = height,
                          random_state=random_state,
                          max_font_size = max_font_size
                         ).generate(str(tup))
    
    return wordcloud

  def removePunctuation(df,col,new_col):
    '''
    removePunctuation(df, col, new_col)

    Removes Punctuation from a text field.

    Parameters
    ----------
    df: spark.DataFrame
      Base spark dataframe with the text field.
    col: str
      Name of the column with text field to be processed.
    new_col: str
      Name of the new column that will be pre-processed.

    Raises
    ------
    No Raises.

    Returns
    -------
    pandas.DataFrame
      Base dataframe with the text field without Punctuation.
    '''
    column = F.split(F.trim(F.lower(regexp_replace(F.concat_ws("SEPARATORSTRING", F.col(col)),'[.,;:!?\-*/+=%$@#&]',' '))), "SEPARATORSTRING").alias(col)
    df = df.withColumn(new_col,column)
    return df.withColumn(new_col,F.concat_ws(",",F.col(new_col)))
  

  def removeStopWords(df,col,new_col):
    '''
    removeStopWords(df, col, new_col,stopwords)

    Removes stop-words from a text field.

    Parameters
    ----------
    df: spark.DataFrame
      Base spark dataframe with the text field.
    col: str
      Name of the column with text field to be processed.
    new_col: str
      Name of the new column that will be pre-processed.
    stopwords: list
      List of stop-words to be removed.

    Raises
    ------
    No Raises.

    Returns
    -------
    pandas.DataFrame
      Base dataframe with the text field without stop-words.
    '''
    doc_tokenized =  df.withColumn('t_decricao_clear_token',F.split(F.col(col), ' '))
    remover = StopWordsRemover(stopWords=stopwords)
    remover.setInputCols(["t_decricao_clear_token"]).setOutputCols(["t_decricao_clear"])
    df2 = remover.transform(doc_tokenized)
    df2 = df2.withColumn(new_col, F.concat_ws(" ", "t_decricao_clear"))
    df2 = df2.drop("t_decricao_clear_token",'t_decricao_clear')
    return df2
