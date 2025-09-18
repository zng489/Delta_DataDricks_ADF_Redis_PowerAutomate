# Databricks notebook source
# Copia os arquivos e diretórios do caminho especificado para o destino
source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/trs/oni/un/comtrade/comer_wld/commodity_annual/'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/un/comtrade/comer_wld/commodity_annual/'

# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

trs_empresas_auxiliar = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/empresas_auxiliar/' 
trs_dim_cnae = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/dim_cnae/'
trs_estrutura_setorial = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/estrutura_setorial/' 
trs_path_monitor_completo = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/mongodb_monitor__investimento/' 
trs_path_dolar = '/tmp/dev/trs/oni/bacen/cotacao/dolar_americano/' 
trs_path_euro = '/tmp/dev/trs/oni/bacen/cotacao/euro/' 
biz_path_municipios = '/tmp/dev/biz/oni/bases_referencia/municipios/' 
biz_base_unica_rais = '/tmp/dev/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais/'

# COMMAND ----------

          "trusted_path_2":"/me/comex/mun_exp_p/", 
          "trusted_path_3":"/me/comex/mun_imp_p/", 

          trs/oni/rfb_cnpj/tabelas_auxiliares_f/nat_juridica

# COMMAND ----------

source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/rfb_cnpj/tabelas_auxiliares_f/motivo/'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/rfb_cnpj/tabelas_auxiliares_f/motivo/'
# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais/'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais/'

# COMMAND ----------

# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

# Deleta os arquivos e diretórios no caminho especificado no Data Lake Storage Gen2 em DESENVOLVIMENTO
dbutils.fs.rm('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/', recurse=True)
# Copia os arquivos e diretórios do caminho especificado para o destino
source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/ativ_econ/class_internacionais/cnae_isic/'

# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)


para fazer processo para todos os camingos a baixo

trs_empresas_auxiliar = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/empresas_auxiliar/' 
trs_dim_cnae = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/dim_cnae/'
trs_estrutura_setorial = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/estrutura_setorial/' 
trs_path_monitor_completo = '/tmp/dev/trs/oni/observatorio_nacional/monitor_investimentos/painel_monitor/mongodb_monitor__investimento/' 
trs_path_dolar = '/tmp/dev/trs/oni/bacen/cotacao/dolar_americano/' 
trs_path_euro = '/tmp/dev/trs/oni/bacen/cotacao/euro/' 
biz_path_municipios = '/tmp/dev/biz/oni/bases_referencia/municipios/' 
biz_base_unica_rais = '/tmp/dev/biz/oni/base_unica_cnpjs/cnpjs_rfb_rais/'

# COMMAND ----------

paths = [
    "trs/oni/un/comtrade/comer_wld/commodity_annual/"
]

base = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
tmp_prefix = "tmp/dev/"

for path in paths:
    from_prod_path = f"{base}{path}"  
    print(from_prod_path)

    from_dev_path = f"{base}{tmp_prefix}{path}"
    print(from_dev_path)

    # Check if source path exists before copying
    if dbutils.fs.ls(from_dev_path):
        dbutils.fs.cp(
            from_dev_path,
            from_prod_path,
            recurse=True
        )
    else:
        print(f"Source path does not exist: {from_dev_path}")

# COMMAND ----------

source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/un/comtrade/comer_wld/commodity_annual/'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/un/comtrade/comer_wld/commodity_annual/'
# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

# Lista de caminhos relativos
paths = [
    "trs/oni/un/comtrade/comer_wld/commodity_annual/"
]

# Prefixos
base = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
tmp_prefix = "tmp/dev/"

# Processa cada caminho
for path in paths:
  #print(path)
  from_prod_path = f"{base}{path}"  
  print(from_prod_path)

  from_dev_path = f"{base}{tmp_prefix}{path}"
  print(from_dev_path)


  # Copiando da producao para dev
  dbutils.fs.cp(from_dev_path, from_prod_path, recurse=True)

# COMMAND ----------

De PROD -> DEV
trs/oni/oni/rfb_cnpj/
 
De DEV -> PROD
"uld/oni/quick_wins/CAT_GRADUACAO_STEM_D/",
"uld/oni/quick_wins/CAT_POS_STEM_D/",
"uld/oni/quick_wins/EDUC_GRADUACAO_CURSOS_NOVO_F/",
"uld/oni/quick_wins/EDUC_GRADUACAO_IGC_F/",
"uld/oni/quick_wins/EDUC_POS_CURSO_F/",
"uld/oni/quick_wins/EDUC_POS_DISCENTE_F/",
"uld/oni/quick_wins/tesouro_siconfi_investimentos_uniao/",
"uld/oni/quick_wins/PROPRIEDADE_INTELECTUAL_CONTRATOS_F/",
"uld/oni/quick_wins/PROPRIEDADE_INTELECTUAL_DESENHO_INDUSTRIAL_F/",
"uld/oni/quick_wins/PROPRIEDADE_INTELECTUAL_MARCAS_F/",
"uld/oni/quick_wins/PROPRIEDADE_INTELECTUAL_PATENTES_F/"

# COMMAND ----------

cadastro_empresa_f
 
cadastro_estbl_f
 
cadastro_simples_f
 
cadastro_socio_f
 
regime_tributario_f
 
tabelas_auxiliares_f
 

# COMMAND ----------

# Lista de caminhos relativos
paths = [
    "trs/oni/rfb_cnpj/cadastro_estbl_f/"
]

# Prefixos
base = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
tmp_prefix = "tmp/dev/"

# Processa cada caminho
for path in paths:
    source_path = f"{base}{path}"
    destination_path = f"{base}{tmp_prefix}{path}"

    print(f"Deletando destino: {destination_path}")
    dbutils.fs.rm(destination_path, recurse=True)

    print(f"Copiando de {source_path} para {destination_path}")
    dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

/trs/oni/rfb_cnpj/cadastro_estbl_f/ANO_ARQUIVO=2024/MES_ARQUIVO=12'
/uds/oni/observatorio_nacional/BACKUP/cota_empresa_pcd'
/biz/oni/bases_referencia/municipios'
/biz/oni/bases_referencia/cnae/cnae_20/cnae_divisao'

    #"biz/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_empresa"
    'biz/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_estab'

# COMMAND ----------

tmp/dev/biz/oni/painel_pcd/cota_empresa_pcd/

# COMMAND ----------

# Lista de caminhos relativos
paths = [
    #"biz/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_empresa"
    'biz/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_estab'
]

# Prefixos
base = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
tmp_prefix = "tmp/dev/"

# Processa cada caminho
for path in paths:
    source_path = f"{base}{path}"
    destination_path = f"{base}{tmp_prefix}{path}"

    print(f"Deletando destino: {destination_path}")
    dbutils.fs.rm(destination_path, recurse=True)

    print(f"Copiando de {source_path} para {destination_path}")
    dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/midr/fco/carteira/')

# COMMAND ----------

from pyspark.sql.functions import input_file_name
import os

# Caminho da pasta com os arquivos Parquet
path = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/midr/fco/carteira/"

# Listar arquivos
files = dbutils.fs.ls(path)  # Check permissions or ensure the correct credentials are used.
print(files)

# COMMAND ----------

from pyspark.sql.functions import input_file_name
import os

# Caminho da pasta com os arquivos Parquet
path = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/midr/fco/carteira/

# Listar arquivos
files = dbutils.fs.ls(path)

# Filtrar apenas arquivos parquet e mostrar nome e tamanho
parquet_files = [(f.name, f.size) for f in files if f.name.endswith(".parquet")]

for name, size in parquet_files:
    print(f"Nome: {name}, Tamanho: {size} bytes")


# COMMAND ----------

# Caminho da pasta ou arquivo
import crawler.functions as cf#
uld_path = "trs/oni/midr/fco/carteira/"
for f in cf.list_subdirectory(dbutils, uld_path):
  print(f)
  if f.name.endswith(".parquet"):
      print(f"Arquivo: {f.path}")
      print(f"Tamanho (bytes): {f.size}")
      print(f"Data modificação: {f.modificationTime}")  # timestamp em ms
      from datetime import datetime
      print(f"Data legível: {datetime.fromtimestamp(f.modificationTime/1000)}\n")


# COMMAND ----------

from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# SparkSession
spark = SparkSession.builder.getOrCreate()

# Caminho da pasta
base_path = "trs/oni/midr/fco/carteira/"

# Acesso ao Hadoop FileSystem
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

# Função para pegar data de modificação de cada arquivo
def get_file_modification_time(file_path):
    status = fs.getFileStatus(spark._jvm.Path(file_path))
    return status.getModificationTime()  # retorna timestamp em ms

# Listar somente arquivos parquet
files = [f for f in cf.list_subdirectory(dbutils, base_path) if f.endswith(".parquet")]

# Criar lista com arquivo + timestamp
file_dates = [(f, get_file_modification_time(f)) for f in files]

# Transformar em DataFrame Spark
df_files = spark.createDataFrame(file_dates, ["file_path", "modification_time_ms"])

# Converter timestamp para formato legível
from pyspark.sql.functions import from_unixtime, col
df_files = df_files.withColumn("modification_time", from_unixtime(col("modification_time_ms") / 1000))

df_files.show(truncate=False)


# COMMAND ----------

import crawler.functions as cf#
uld_path = "trs/oni/midr/fco/carteira/"
for path in cf.list_subdirectory(dbutils, uld_path):
  print(path)

# COMMAND ----------

uld_path = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/midr/fco/carteira/"
files = dbutils.fs.ls(uld_path)

for file in files:
    print(f"Arquivo: {file.name}")
    print(f"Última modificação: {file.modificationTime}")

# COMMAND ----------

# Lista de caminhos relativos
paths = [
    #"biz/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_empresa"
    'trs/oni/midr/fco/carteira/','trs/oni/midr/fco/contratacoes/','trs/oni/midr/fco/desembolsos/',
    'trs/oni/midr/fne/carteira/','trs/oni/midr/fne/contratacoes/','trs/oni/midr/fne/desembolsos/',
    'trs/oni/midr/fno/carteira/','trs/oni/midr/fno/contratacoes/','trs/oni/midr/fno/desembolsos/',
]

# Prefixos
base = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
tmp_prefix = "uds/oni/observatorio_nacional/oni_raw/dev/"

# Processa cada caminho
for path in paths:
    source_path = f"{base}{path}"
    destination_path = f"{base}{tmp_prefix}{path}"
    print(source_path)
    print(destination_path)


    print(f"Deletando destino: {destination_path}")
    dbutils.fs.rm(destination_path, recurse=True)

    print(f"Copiando de {source_path} para {destination_path}")
    dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

