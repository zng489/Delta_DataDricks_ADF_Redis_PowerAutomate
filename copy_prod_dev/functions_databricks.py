# Databricks notebook source
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

column_list = df.columns
print(columns)

# COMMAND ----------

KEYS = column_list
VALUES = ['New_Column_1', 'New_Column_2' ]

dict_items_SEM_N_A = dict(zip(KEYS, VALUES))
print(dict_items_SEM_N_A)
dict_items_SEM_N_A.items()

# COMMAND ----------

lista_de_valores_1 = []
# df_prm_2019_SEM_N_A['_c4']

list_of_type = ['string', 'string']
for cast in list_of_type:
  lista = cast.capitalize()
  lista_de_valores_1.append(lista)
lista_de_valores_1

# COMMAND ----------

lista_de_valores_2 = []

for x in lista_de_valores_1:
  t = x +'Type()'
  lista_de_valores_2.append(t)

lista_de_valores_2

# COMMAND ----------

lista = [cast.capitalize() for cast in list_of_type]
lista 

# COMMAND ----------

lista_add_Type = [we +'Type()' for we in lista] 
print(lista_add_Type)

# COMMAND ----------

#lista = [cast.replace('double', 'float') for cast in list_of_type]
#print(lista) 

# COMMAND ----------

dict_items_SEM_N_A

# COMMAND ----------

VALUES

# COMMAND ----------

KEYS_cast = VALUES
VALUES_cast = lista
dict_items_CAST = dict(zip(KEYS_cast, lista_add_Type))

# COMMAND ----------

dict_items_CAST

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col

# COMMAND ----------

df_matched = df.select([col(c).alias(dict_items_SEM_N_A.get(c,c)) for c in df.columns])
df_matched

# COMMAND ----------

df_matched.display()

# COMMAND ----------

df_matched.display()

# COMMAND ----------

df_matched

# COMMAND ----------

KEYS_cast = VALUES
VALUES_cast = lista
dict_items_CAST = dict(zip(KEYS_cast, lista_add_Type))
dict_items_CAST

# COMMAND ----------

df_matched.display()

# COMMAND ----------

dict_items_CAST

# COMMAND ----------

df_matched.columns

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Define your data types
dict_items_CAST = {'New_Column_1': StringType(), 'New_Column_2': StringType()}

# Select columns and apply the specified data types
df_matched_cast = df_matched.select(
    *[col(c).cast(dict_items_CAST.get(c, StringType())).alias(c) for c in df_matched.columns]
)


# COMMAND ----------

df_matched_cast.display()

# COMMAND ----------

df_matched_cast = df_matched.select([col(c).cast(dict_items_CAST.get(c, c)).alias(c) for c in df_matched.columns])

# COMMAND ----------



lista = [cast.replace('double', 'float') for cast in df_prm_2019_SEM_N_A['_c4']] 
print(lista)
['string', 'string', 'integer', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'integer', 'string', 'string', 'integer', 'string', 'integer', 'float', 'float', 'float', 'float', 'float', 'integer', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'integer', 'integer', 'string', 'integer', 'string', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'integer', 'integer', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string']
KEYS_cast = df_prm_2019_SEM_N_A['_c3']
VALUES_cast = lista
dict_items_CAST = dict(zip(KEYS_cast, VALUES_cast))
# dict_items_CAST
#{'CD_MUNICIPIO': 'string','CD_CNAE10_CLASSE': 'string','FL_VINCULO_ATIVO_3112': 'integer','CD_TIPO_VINCULO': 'string','CD_MOTIVO_DESLIGAMENTO': 'string','CD_MES_DESLIGAMENTO': 'string','CD_TIPO_ADMISSAO': 'string','CD_GRAU_INSTRUCAO': 'string','CD_SEXO': 'string'}

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

from unicodedata import normalize

import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re
import os

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

table = {"schema":"ibge","table":"pintec_n_grau_obst_cnae","prm_path":"usr/ibge_pintec/FIEC_IBGE_pintec_n_grau_obst_cnae_mapeamento_unificado_para_correcao_raw_.xlsx","source":"crw/oni/ibge/pintec/n_grau_obst/n_grau_obst_cnae/","destination":"oni/ibge/pintec/n_grau_obst/n_grau_obst_cnae/"}

#{"notebook":"/ibge/pintec/org_raw_pintec_n_grau_obst_cnae"}

adf = {"adf_factory_name":"cnibigdatafactory","adf_pipeline_name":"raw_trs_pnadc_a_visita5_f","adf_pipeline_run_id":"04a40e47-07bd-4415-a3a9-2b77158f490b","adf_trigger_id":"7adb91d09feb444d9c383c002feea0d0","adf_trigger_name":"Sandbox","adf_trigger_time":"2023-06-28T13:41:09.760834Z","adf_trigger_type":"Manual"}

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']
lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])

# COMMAND ----------

raw_path = "{raw}/{dst}".format(raw=raw, dst=table["destination"])
adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

# COMMAND ----------

def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .upper())

prm = dls['folders']['prm']
prm_path = os.path.join(prm, table["prm_path"])

# COMMAND ----------

prm_path

# COMMAND ----------

# Parse ba doc
def parse_ba_doc(dbutils, var_adl_path: str, headers: dict, metadata: dict = None, sheet_names: list = None,
                 file_system='datalake', scope: str = 'adls_gen2'):
  """
  This functions reads a Excel file and returns a json in format {"name_of_tab": ["origin_column", "destination_column", "type_of_column"]} where name_of_tab is the name of the specified tab in excel file corresponding to a year, origin_column is the "from" column, destination_columns is the "to" column and type_of_column is the type we want to cast the column.
  Params:
    - dbutils: databricks's dbutils object, so we can access secrets
    - var_adl_path: Is the full path to read the documentation in adls;
    - headers = {
          name_header: Name of the header which position is specified in "pos_header" parameter. It is needed for parsing the file. and should be written exactly as it is in excel file.
          pos_header: Letter or Position of the Excel column which has the name of the header specified in "name_header" parameter.
          pos_org: Letter or Position of the Excel column from which the origin table's column names come from.
          pos_dst: Letter or Position of the Excel column from which the origin table's column names will become in raw table.
          pos_type: Letter or Position of the Excel column from which the origin table's column type will become in raw table.
    }
    - metadata: Parse metadata values of process, like where the table is from = {
          name_header: Name of the header which position is specified in "pos_header" parameter. It is needed for parsing the file. and should be written exactly as it is in excel file.
          pos_header: Letter or Position of the Excel column which has the name of the header specified in "name_header" parameter.
    }
    - sheet_names (Optional): Return only specified sheets
    - scope: Adls scope. Only works for Gen1. Default is "adls_gen1"
  """
  """if scope == 'adls_gen1':
      logging.warning("parse_ba_doc: scope adls_gen1 isn't available, changing automatically to adls_gen2")
      scope = 'adls_gen2'

  var_adl = auth_adl(dbutils, file_system=file_system, scope=scope)
  file_client = var_adl.get_file_client(var_adl_path)
  download = file_client.download_file()"""

  for header in headers:
      if header.startswith('pos'):
          headers[header] = col_to_num(headers[header])

  if metadata is not None:
      for k in metadata:
          if k.startswith('pos'):
              metadata[k] = col_to_num(metadata[k])


  var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
  file_info = dbutils.fs.ls(var_adls_uri + var_adl_path)
  file_name = file_info[0].name
  file_path = file_info[0].path
  dbfs_tmp_spark = "dbfs:/tmp/"
  dbfs_tmp_driver = "/dbfs/tmp/"

  dbutils.fs.cp(file_path, dbfs_tmp_spark + file_name)

  excel = open_workbook(dbfs_tmp_driver + file_name)

  dbutils.fs.rm(dbfs_tmp_spark + file_name)

  parse, parse_metadata, invalid_lines = {}, {}, {}
  sheets = excel.sheet_names()
  if sheet_names is not None:
    sheets = filter(lambda sheet: sheet in sheet_names, sheets)

  for sheet_name in sheets:
    current_sheet = excel.sheet_by_name(sheet_name)
    if current_sheet.visibility == 0:
      table_row_index = find_table_row_index(current_sheet, headers['name_header'], headers['pos_header'])
      if table_row_index > 0:
        parse[sheet_name] = []

        for row_index in range(table_row_index + 1, current_sheet.nrows):
          line = [current_sheet.row(row_index)[headers['pos_org']].value.strip("\n").strip(),
                  current_sheet.row(row_index)[headers['pos_dst']].value.strip("\n").strip(),
                  current_sheet.row(row_index)[headers['pos_type']].value.strip("\n").strip()]

          if all(len(l) == 0 for l in line):
            break

          if any(len(l) == 0 for l in line):
            print(line)
            if sheet_name not in invalid_lines:
              invalid_lines[sheet_name] = []
            invalid_lines[sheet_name].append(row_index + 1)
          else:
            parse[sheet_name].append(line)

      if metadata is not None:
        metadata_row_index = find_table_row_index(current_sheet, metadata['name_header'],
                                                  metadata['pos_header'])
        for row_index in range(metadata_row_index + 1, current_sheet.nrows):
          key = normalize_str(current_sheet.row(row_index)[metadata['pos_header']].value)
          if len(key) == 0:
            break

          if sheet_name not in parse_metadata:
            parse_metadata[sheet_name] = {}

          value = current_sheet.row(row_index)[metadata['pos_header'] + 1].value
          value = value.replace('\\', '/')

          parse_metadata[sheet_name][key] = value.strip()

  if len(invalid_lines.keys()) > 0:
    raise Exception('Documentation with invalid lines', invalid_lines)

  if len(parse_metadata.keys()) > 0:
    return parse, parse_metadata
  else:
    return parse

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
parse_ba_doc(dbutils, prm_path, headers=headers)

# COMMAND ----------


headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

for sub_plan in var_prm_dict.values():
  for row in sub_plan:
    row[0] = __normalize_str(row[0])

def __transform_columns(sheet: str):
  for org, dst, _type in var_prm_dict[sheet]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)



_df = None
for lnd_year_path in cf.list_subdirectory(dbutils, lnd_path):
  df_adl_files = cf.list_adl_files(spark, dbutils, lnd_year_path)

  files_folder = cf.list_subdirectory(dbutils, lnd_year_path)

  for i, file in enumerate(files_folder):
    path_origin = '{adl_path}/{file_path}'.format(adl_path=var_adls_uri, file_path=file)
    year = lnd_year_path.split('/')[-1].replace('.parquet', '').split('_')[-1]
    df = spark.read.parquet(path_origin)

    cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=year)

    df = df.select(*__transform_columns(year))
    df = df.withColumn("ANO", f.lit(year))
    
    _df_n_columns = len(_df.columns) if _df else len(df.columns)

    if len(df.columns) != _df_n_columns:
      biggest_df = df if len(df.columns) > len(_df.columns) else _df
      smallest_df = df if len(df.columns) > len(_df.columns) else _df      
    else:
      biggest_df = _df
      smallest_df = df
      
    if _df:
      _df = biggest_df.unionByName(smallest_df)      
    else:
      _df = df
      
  if _df:
    df = _df

df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

# COMMAND ----------

df.write.parquet(path=adl_sink, mode='overwrite')

# COMMAND ----------

import collections
import re
import string
from unicodedata import normalize

from xlrd import open_workbook

# função que move arquivo do /dbfs/tmp/ para o datalake (bom para imagens)
def move_file_to_lake(spark, dbutils, caminho_origem, caminho_datalake):
  resultado = "ok"
  if caminho_origem[0:10] != '/dbfs/tmp/':
    resultado = 'erro: o caminho de origem deve começar com "/dbfs/tmp/" !'
  if resultado == "ok":
    var_final = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/" + caminho_datalake
    caminho_origem_spark = caminho_origem.replace('/dbfs/tmp/','dbfs:/tmp/')
    if ~dbutils.fs.mv(caminho_origem_spark, var_final):
      resultado == "erro: não foi possível mover o arquivo para o data lake"
  return resultado


def check_ba_doc(df, parse_ba: dict, sheet: str, exclude_cols: list = None, return_dst_cols=False):
    """
    Perform some checks of explicit invalid records on the specification and returns the list of the destination columns.
    :param df: dataframe of data extracted from landing
    :param parse_ba: Dictionary extracted from parse_ba_doc function. Should contain year as keys and lists of 3 string values each as content, that corresponds to the origin column, destination column and type of the column.
    :param sheet: The current sheet that must be validated with the dataframe
    :param exclude_cols: List of columns to not include on validation. By default uses ['nm_arq_in', 'nr_reg', 'dh_arq_in']
    :param return_dst_cols: Case everything is ok, returns the list of the destination columns
    """
    if exclude_cols is None:
      exclude_cols = ['nm_arq_in', 'nr_reg', 'dh_arq_in']

    # List all columns in the dataframe except for the control fields
    list_of_columns = list(set(df.columns) - set(exclude_cols))

    # List of all origin columns that exist in the specification
    list_of_existent_columns = [row[0] for row in parse_ba[sheet] if row[0].lower() != 'n/a']

    # List of origin columns that exist in the specification but don't exist on the dataframe.
    # This corresponding to a documentation error.
    list_not_present = list(set(list_of_existent_columns) - set(list_of_columns))

    # List of all destination columns that exist in the specification
    list_of_destination_columns = [row[1] for row in parse_ba[sheet]]

    # List of destination columns that repeat more than once in specification.
    # This corresponding to a documentation error.
    var_repetitions = collections.Counter(list_of_destination_columns)
    list_of_repetitions = [i for i in var_repetitions if var_repetitions[str(i)] > 1]

    # Returns the documentation errors that were found, specifying the corresponding lines in the file.
    if len(list_not_present) > 0 or len(list_of_repetitions) > 0:
      error_msg = """
Exception:
      Sheet: {sheet}
      All columns: {list_of_columns}
      Specification columns not existent in origin: {list_not_present}
      Repeated columns: {list_of_repetitions}
    """.format(sheet=sheet, list_of_columns=list_of_columns, list_not_present=list_not_present,
               list_of_repetitions=list_of_repetitions)
      raise Exception(error_msg)

    return list_of_destination_columns if return_dst_cols else None


def col_to_num(col):
  if isinstance(col, int):
    return col

  if isinstance(col, str):
    num = 0
    for c in col:
      if c in string.ascii_letters:
        num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num - 1


def find_table_row_index(current_sheet, name_header, pos_header):
  table_row_index = -1
  for row_index in range(0, current_sheet.nrows):
    value = current_sheet.row(row_index)[pos_header].value
    if name_header == value:
      table_row_index = row_index
      break

  return table_row_index


def normalize_str(_str):
  return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                .encode('ASCII', 'ignore')
                .decode('ASCII')
                .replace(' ', '_')
                .replace('-', '_')
                .replace('/', '_')
                .upper())


def parse_ba_doc(dbutils, var_adl_path: str, headers: dict, metadata: dict = None, sheet_names: list = None,
                 file_system='datalake', scope: str = 'adls_gen2'):
  """
  This functions reads a Excel file and returns a json in format {"name_of_tab": ["origin_column", "destination_column", "type_of_column"]} where name_of_tab is the name of the specified tab in excel file corresponding to a year, origin_column is the "from" column, destination_columns is the "to" column and type_of_column is the type we want to cast the column.
  Params:
    - dbutils: databricks's dbutils object, so we can access secrets
    - var_adl_path: Is the full path to read the documentation in adls;
    - headers = {
          name_header: Name of the header which position is specified in "pos_header" parameter. It is needed for parsing the file. and should be written exactly as it is in excel file.
          pos_header: Letter or Position of the Excel column which has the name of the header specified in "name_header" parameter.
          pos_org: Letter or Position of the Excel column from which the origin table's column names come from.
          pos_dst: Letter or Position of the Excel column from which the origin table's column names will become in raw table.
          pos_type: Letter or Position of the Excel column from which the origin table's column type will become in raw table.
    }
    - metadata: Parse metadata values of process, like where the table is from = {
          name_header: Name of the header which position is specified in "pos_header" parameter. It is needed for parsing the file. and should be written exactly as it is in excel file.
          pos_header: Letter or Position of the Excel column which has the name of the header specified in "name_header" parameter.
    }
    - sheet_names (Optional): Return only specified sheets
    - scope: Adls scope. Only works for Gen1. Default is "adls_gen1"
  """
  """if scope == 'adls_gen1':
      logging.warning("parse_ba_doc: scope adls_gen1 isn't available, changing automatically to adls_gen2")
      scope = 'adls_gen2'

  var_adl = auth_adl(dbutils, file_system=file_system, scope=scope)
  file_client = var_adl.get_file_client(var_adl_path)
  download = file_client.download_file()"""

  for header in headers:
      if header.startswith('pos'):
          headers[header] = col_to_num(headers[header])

  if metadata is not None:
      for k in metadata:
          if k.startswith('pos'):
              metadata[k] = col_to_num(metadata[k])


  var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
  file_info = dbutils.fs.ls(var_adls_uri + var_adl_path)
  file_name = file_info[0].name
  file_path = file_info[0].path
  dbfs_tmp_spark = "dbfs:/tmp/"
  dbfs_tmp_driver = "/dbfs/tmp/"

  dbutils.fs.cp(file_path, dbfs_tmp_spark + file_name)

  excel = open_workbook(dbfs_tmp_driver + file_name)

  dbutils.fs.rm(dbfs_tmp_spark + file_name)

  parse, parse_metadata, invalid_lines = {}, {}, {}
  sheets = excel.sheet_names()
  if sheet_names is not None:
    sheets = filter(lambda sheet: sheet in sheet_names, sheets)

  for sheet_name in sheets:
    current_sheet = excel.sheet_by_name(sheet_name)
    if current_sheet.visibility == 0:
      table_row_index = find_table_row_index(current_sheet, headers['name_header'], headers['pos_header'])
      if table_row_index > 0:
        parse[sheet_name] = []

        for row_index in range(table_row_index + 1, current_sheet.nrows):
          line = [current_sheet.row(row_index)[headers['pos_org']].value.strip("\n").strip(),
                  current_sheet.row(row_index)[headers['pos_dst']].value.strip("\n").strip(),
                  current_sheet.row(row_index)[headers['pos_type']].value.strip("\n").strip()]

          if all(len(l) == 0 for l in line):
            break

          if any(len(l) == 0 for l in line):
            print(line)
            if sheet_name not in invalid_lines:
              invalid_lines[sheet_name] = []
            invalid_lines[sheet_name].append(row_index + 1)
          else:
            parse[sheet_name].append(line)

      if metadata is not None:
        metadata_row_index = find_table_row_index(current_sheet, metadata['name_header'],
                                                  metadata['pos_header'])
        for row_index in range(metadata_row_index + 1, current_sheet.nrows):
          key = normalize_str(current_sheet.row(row_index)[metadata['pos_header']].value)
          if len(key) == 0:
            break

          if sheet_name not in parse_metadata:
            parse_metadata[sheet_name] = {}

          value = current_sheet.row(row_index)[metadata['pos_header'] + 1].value
          value = value.replace('\\', '/')

          parse_metadata[sheet_name][key] = value.strip()

  if len(invalid_lines.keys()) > 0:
    raise Exception('Documentation with invalid lines', invalid_lines)

  if len(parse_metadata.keys()) > 0:
    return parse, parse_metadata
  else:
    return parse

# COMMAND ----------

