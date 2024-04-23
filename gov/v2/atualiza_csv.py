# Databricks notebook source
# MAGIC %run ./__libs_xls

# COMMAND ----------

dbutils.widgets.text("env","dev") #tmp ou prod
env = dbutils.widgets.get("env")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

from datetime import datetime, timezone, timedelta

# COMMAND ----------

path_gov = path
path_arquivo = path + '/'.join(['upload'])
env, path_gov, path_arquivo

# COMMAND ----------

def get_path_tables (tipo): # esquema, tabela ou campo
  return path_gov + '/'.join(['tables',tipo])

# COMMAND ----------

def atualiza(dataframe_csv):
  chave_merge = []
  merge = dataframe_csv.withColumn("dt_atualizacao", current_timestamp())
  colunas = merge.columns
  chave = colunas[0]
  tipo = ''
  erro = 1
  
  if chave == 'path_esquema':
    tipo = 'esquema'
    erro = 0
    colunas_validas = ['dt_atualizacao', 'esquema_ds','ativo', 'esquema_tx', 
                       'cLivre1', 'cLivre2', 'cLivre3', 'cLivre4', 'cLivre5']
    colunas_chave = ['path_esquema']

  if chave == 'path_tabela':
    tipo = 'tabela'
    erro = 0
    colunas_validas = ['dt_atualizacao', 'chave', 'col_version', 'tipo_carga', 'ativo', 'login_datasteward', 'login_dataowner', 
                       'tabela_ds', 'tabela_tx', 'acesso_default', 'dados_pessoais', 'eixo', 'base', 'base_tx', 
                       'cLivre1', 'cLivre2', 'cLivre3', 'cLivre4', 'cLivre5']
    colunas_chave = ['path']
    merge = merge.withColumn("path", col('path_tabela'))
    
  if chave == 'path_campo':
    tipo = 'campo'
    erro = 0
    colunas_validas = ['dt_atualizacao', 'ativo', 'campo_ds', 'ind_relevancia', 'dado_pessoal', 
                       'cLivre1', 'cLivre2', 'cLivre3', 'cLivre4', 'cLivre5', 'Tema']    
    colunas_chave = ['path', 'campo']
    
    
  if erro == 0:
    upset = {}
    key_join = []
    for chave in colunas_chave:
      key_join.append("delta." + chave + " = " + "up." + chave)
    for coluna in colunas:
      if coluna in colunas_validas:
        if coluna not in colunas_chave: 
          upset[coluna] = 'up.' + coluna    
    
    key = ' and '.join(key_join)
    if tipo == 'campo': key = key.replace("up.path", "up.path_campo")

    #print(upset)
    #print(key)
    
    delta = DeltaTable.forPath(spark, get_path_tables(tipo)) #delta.toDF().display()
    delta.alias("delta") \
          .merge(merge.alias("up"), key)\
                      .whenMatchedUpdate(set = upset)\
                      .execute()        
    
    resultado = 'dataframe atualizado'
  else:
    resultado = 'arquivo csv inválido'

  return [resultado,tipo]

# COMMAND ----------

# varre o diretório à procura de arquivos para importar e atualizar os registros do catálogo
file_exists = 0
lista = []
try:
  lista = dbutils.fs.ls(path_arquivo)
  if lista != []:
    for item in lista:
      if item.size > 0:
        arquivo = item.path
        arquivo_valido = 0
        
        if arquivo[-4:] == '.csv':
          arquivo_valido = 1
          df = spark.read.format("csv").option("sep", ";").option("header", "true").option("encoding", "ISO-8859-1").load(arquivo)
          
        if arquivo[-5:] == '.xlsx':
          arquivo_valido = 1
          wb = OpenExcelFromLake(arquivo)
          ws = wb[wb.sheetnames[0]]
          dados = ws.values
          colunas = next(dados)[0:]
          df_pandas = pd.DataFrame(dados, columns=colunas)          
          df = spark.createDataFrame(df_pandas) 
        
        if arquivo_valido == 0:
          print(arquivo, 'arquivo inválido - não é xlsx ou csv')
        else:
          resultado = atualiza(df)
          if resultado[0] == 'dataframe atualizado':
            now = datetime.now().astimezone(timezone(timedelta(hours=-3))) # current date and time
            date_time = now.strftime("%Y-%m-%d__%H-%M-%S")
            df.write.csv(path_arquivo + '/'.join(['','feito',resultado[1] + '_' + date_time]))
            dbutils.fs.rm(arquivo,True)
          print(arquivo, resultado)            

except(AnalysisException) as e:
  file_exists = 0
  print("Arquivo não encontrado")
  dbutils.notebook.exit("File not found")
