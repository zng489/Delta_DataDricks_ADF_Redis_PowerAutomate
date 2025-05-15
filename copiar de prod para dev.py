# Databricks notebook source
# Lista os arquivos e diretórios no caminho especificado no Data Lake Storage Gen2
dbutils.fs.ls('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/')

# COMMAND ----------

# Lista os arquivos e diretórios no caminho especificado no Data Lake Storage Gen2 em DESENVOLVIMENTO
dbutils.fs.ls('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/')

# COMMAND ----------

#BACKUP DE DEV - CASO NÃO QUEIRA UTILIZAR O SOFDELETE COMO SEGURANÇA
# Copia os arquivos e diretórios do caminho especificado para a pasta de backup com a data atual
from datetime import datetime

# Define o caminho de origem e destino
source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/'
current_date = datetime.now().strftime('%Y%m%d')
destination_path = f'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic_BKP_{current_date}/'

# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

# Deleta os arquivos e diretórios no caminho especificado no Data Lake Storage Gen2 em DESENVOLVIMENTO
dbutils.fs.rm('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/', recurse=True)

# COMMAND ----------

# Copia os arquivos e diretórios do caminho especificado para o destino
source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/'

# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)