# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


# COMMAND ----------

tables = {'schema':'', 'table':'', 'raw_path':'/usr/oni/ibge/pintec_semestral/',
          'trusted_path':'/oni/ibge/pintec_semestral/', 'prm_path':''}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn
var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from trs_control_field import trs_control_field as tcf
import pyspark.sql.functions as f
import crawler.functions as cf
from pyspark.sql import SparkSession
import time
import pandas as pd
from pyspark.sql.functions import col, when, explode, lit
import json
from unicodedata import normalize 
import datetime
import re
import os
from core.string_utils import normalize_replace

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
trusted = dls['folders']['trusted']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])

raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])

adl_raw= f'{var_adls_uri}{raw_path}'
print(adl_raw)


trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path'])

adl_trusted = f'{var_adls_uri}{trusted_path}'
print(adl_trusted)

# COMMAND ----------

def read_parquet(file_path: str):
    return spark.read.format("parquet").load(file_path)


# COMMAND ----------

'''
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, IntegerType

col_cast_map = {
    "UF": StringType(),
    "CODIGO_DO_MUNICIPIO": StringType(),
    "NOME_DO_MUNICIPIO": StringType(),
    "PESSOA_FISICA_OU_JURIDICA": StringType(),
    "DATA_DA_CONTRATACAO": StringType(),
    "VENCIMENTO_FINAL": StringType(),
    "TIPOLOGIA_DO_MUNICIPIO": StringType(),
    "FAIXA_DE_FRONTEIRA": StringType(),
    "SEMIARIDO": StringType(),
    "SETOR": StringType(),
    "PROGRAMA": StringType(),
    "LINHA_DE_FINANCIAMENTO": StringType(),
    "ATIVIDADE": StringType(),
    "PORTE": StringType(),
    "FINALIDADE_DA_OPERACAO": StringType(),
    "RISCO_DA_OPERACAO": StringType(),
    "TAXA_DE_JUROS": DoubleType(),
    "FORMA_DA_TAXAS_DE_JUROS": StringType(),
    "SITUACAO_DA_OPERACAO": StringType(),
    "RATING": StringType(),
    "QUANTIDADE_DE_CONTRATOS": IntegerType(),
    "SALDO_DA_CARTEIRA": DoubleType(),
    "SALDO_EM_ATRASO": DoubleType(),
    "INTITUICAO_OPERADORA": StringType()
}

for col_name, col_type in col_cast_map.items():
    if col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast(col_type))
'''

# COMMAND ----------

from pyspark.sql.types import StringType, DoubleType, IntegerType

from pyspark.sql.types import StringType, DoubleType, IntegerType

col_cast_map = {
    "NR_ANO": IntegerType(),
    "NM_ATIVIDADES_DA_INDUSTRIA": StringType(),
    "NR_EMPRESAS_TOTAL": DoubleType(),
    "NR_REL_SUSTENT_TOTAL": DoubleType(),
    "NR_IMPLEMENT_INOV_PROD_PROC_TOTAL": DoubleType(),
    "NR_NOME_BASE": StringType(),
    "NR_ATIVAS_IMPLEMENT_INOV": DoubleType(),
    "NR_INOV_PROD_PROC_NEG_TOTAL": DoubleType(),
    "NR_INOV_PROD": DoubleType(),
    "NR_INOV_PROC_NEG": DoubleType(),
    "NR_APENAS_PJT_INCOMP_ABAND_TOTAL": DoubleType(),
    "NR_APENAS_PJT_INCOMP": DoubleType(),
    "NR_APENAS_PJT_ABAND": DoubleType(),
    "NR_IMPLEMENT_INOV_PROD_TOTAL": DoubleType(),
    "NR_GRAU_NOV_NOVO_EMP": DoubleType(),
    "NR_GRAU_NOV_NOVO_MERC_NAC": DoubleType(),
    "NR_GRAU_NOV_NOVO_MERC_MUND": DoubleType(),
    "NR_RESULT_INOV_MELHOR_ESP": DoubleType(),
    "NR_RESULT_INOV_ACORDO_ESP": DoubleType(),
    "NR_RESULT_INOV_ABAIXO_ESP": DoubleType(),
    "NR_RESULT_INOV_CEDO_AVAL": DoubleType(),
    "NR_IMPLEMENT_INOV_PROC_NEG_TOTAL": DoubleType(),
    "NR_FUNC_EMP_PROD_BENS_SERV": DoubleType(),
    "NR_FUNC_EMP_LOGIST": DoubleType(),
    "NR_FUNC_EMP_PROCES_INFO": DoubleType(),
    "NR_FUNC_EMP_CONTAB": DoubleType(),
    "NR_FUNC_EMP_PRAT_GEST": DoubleType(),
    "NR_FUNC_EMP_GEST_RH": DoubleType(),
    "NR_FUNC_EMP_MKT": DoubleType(),
    "NR_IMPLEMENT_INOV_PROD_PROC_NEG_TOTAL": DoubleType(),
    "NR_DIFICUL_OBST_TOTAL": DoubleType(),
    "NR_GRAU_INST_ECO_IMP": DoubleType(),
    "NR_GRAU_INST_ECO_POU_IMP": DoubleType(),
    "NR_GRAU_INST_ECO_N_IMP": DoubleType(),
    "NR_GRAU_ACIR_CONC_IMP": DoubleType(),
    "NR_GRAU_ACIR_CONC_POU_IMP": DoubleType(),
    "NR_GRAU_ACIR_CONC_N_IMP": DoubleType(),
    "NR_GRAU_BAIX_DEM_IMP": DoubleType(),
    "NR_GRAU_BAIX_DEM_POU_IMP": DoubleType(),
    "NR_GRAU_BAIX_DEM_N_IMP": DoubleType(),
    "NR_GRAU_DIF_PARC_IMP": DoubleType(),
    "NR_GRAU_DIF_PARC_POU_IMP": DoubleType(),
    "NR_GRAU_DIF_PARC_N_IMP": DoubleType(),
    "NR_GRAU_LIM_REC_INT_IMP": DoubleType(),
    "NR_GRAU_LIM_REC_INT_POU_IMP": DoubleType(),
    "NR_GRAU_LIM_REC_INT_N_IMP": DoubleType(),
    "NR_GRAU_LIM_TEC_EXT_IMP": DoubleType(),
    "NR_GRAU_LIM_TEC_EXT_POUC_IMP": DoubleType(),
    "NR_GRAU_LIM_TEC_EXT_N_IMP": DoubleType(),
    "NR_GRAU_ESC_REC_PUB_IMP": DoubleType(),
    "NR_GRAU_ESC_REC_PUB_POU_IMP": DoubleType(),
    "NR_GRAU_ESC_REC_PUB_N_IMP": DoubleType(),
    "NR_GRAU_MUD_PRIO_EST_IMP": DoubleType(),
    "NR_GRAU_MUD_PRIO_EST_POU_MP": DoubleType(),
    "NR_GRAU_MUD_PRIO_EST_N_IMP": DoubleType(),
    "NR_N_IMPLEMENT_INOV_PROD_PROC_NEG_TOTAL": DoubleType(),
    "NR_GRAU_BAIX_DEM_POU_IMP.1": DoubleType(),
    "NR_GRAU_OBT_REC_PUB_IMP": DoubleType(),
    "NR_GRAU_OBT_REC_PUB_POUC_IMP": DoubleType(),
    "NR_GRAU_OBT_REC_PUB_N_IMP": DoubleType(),
    "NR_GRAU_MUD_PRIO_EST_POU_IMP": DoubleType(),
    "NR_REL_COOP_OUT_ORG_TOTAL": DoubleType(),
    "NR_REL_COOP_CL_CONS": DoubleType(),
    "NR_REL_COOP_FORNC": DoubleType(),
    "NR_REL_COOP_CONC": DoubleType(),
    "NR_REL_COOP_ICT": DoubleType(),
    "NR_REL_COOP_STARTUP": DoubleType(),
    "NR_REL_COOP_CN_EMP_CN": DoubleType(),
    "NR_REL_COOP_OUT_EMP_GP": DoubleType(),
    "NR_DISP_ATIV_PED": DoubleType(),
    "NR_VALOR_PED (1000)": DoubleType(),
    "NR_PROD_PROC_DISP_PED_ANO+1_ANO_AUMENT": DoubleType(),
    "NR_PROD_PROC_DISP_PED_ANO+1_ANO_MANUT": DoubleType(),
    "NR_PROD_PROC_DISP_PED_ANO+1_ANO_DIMIN": DoubleType(),
    "NR_PROD_PROC_DISP_PED_ANO+2_ANO+1_AUMENT": DoubleType(),
    "NR_PROD_PROC_DISP_PED_ANO+2_ANO+1_MANUT": DoubleType(),
    "NR_PROD_PROC_DISP_PED_ANO+2_ANO+1_DIMIN": DoubleType(),
    "NR_N_IMPLEMENT_INOV_PROD_PROC_2023_TOTAL": DoubleType(),
    "NR_REAL_ATIV_PED_ANO+1": DoubleType(),
    "NR_EXPEC_DISP_PED_ANO+2_ANO+1_AUMENT ": DoubleType(),
    "NR_EXPEC_DISP_PED_ANO+2_ANO+1_MANUT": DoubleType(),
    "NR_EXPEC_DISP_PED_ANO+2_ANO+1_DIMIN": DoubleType(),
    "NR_IMPLEMENT_INOV_PAND_ATIV_MANT": DoubleType(),
    "NR_IMPLEMENT_INOV_PAND_ATIV_DESACEL": DoubleType(),
    "NR_IMPLEMENT_INOV_PAND_ATIV_DESCONT": DoubleType(),
    "NR_IMPLEMENT_INOV_PAND_NOVA_ATIV_REAL": DoubleType(),
    "NR_N_IMPLEMENT_INOV_PROD_PROC_2021_TOTAL": DoubleType(),
    "NR_N_INOV_COVID": DoubleType(),
    "NM_FAIXA DE PESSOAL": StringType(),
    "NR_APOIO_PUB_ATIV_INOV_INSTR_TOTAL": DoubleType(),
    "NR_INCENT_FISC_P&D_INOV_TEC": DoubleType(),
    "NR_INCENT_FISC_LEI_INFO": DoubleType(),
    "NR_APOIO_N_REMB_P&D_I_PESQ": DoubleType(),
    "NR_APOIO_FINANC_PJT_PD&I_PARC_UNIV_INST": DoubleType(),
    "NR_APOIO_FINANC_COMP_MAQ_EQUIP": DoubleType(),
    "NR_APOIO_COMP_PUB": DoubleType(),
    "NR_APOIO_OUT_PROG_AP": DoubleType(),
    "NR_INST_UTILIZ": DoubleType(),
    "NR_APOIO_PUB_TIPO_ATIV_INOV_TOTAL": DoubleType(),
    "NR_ATIV_INOV_DISP_INFO_MT_ADEQ": DoubleType(),
    "NR_ATIV_INOV_DISP_INFO_PARC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_DISP_INFO_POUC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_DISP_INFO_N_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PROC_ADM_MT_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PROC_ADM_PARC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PROC_ADM_POUC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PROC_ADM_N_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PRAZO_CONSEC_MT_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PRAZO_CONSEC_PARC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PRAZO_CONSEC_POUC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_PRAZO_CONSEC_N_ADEQ": DoubleType(),
    "NR_ATIV_INOV_COND_CONTRAP_MT_ADEQ": DoubleType(),
    "NR_ATIV_INOV_COND_CONTRAP_PARC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_COND_CONTRAP_POUC_ADEQ": DoubleType(),
    "NR_ATIV_INOV_COND_CONTRAP_N_ADEQ": DoubleType(),
    " NR_N_APOIO_PUB_TIPO_ATIV_INOV_TOTAL": DoubleType(),
    "NR_N_APOIO_PUB_ATIV_INOV_TIPO_INSTR_TOTAL": DoubleType(), # ---> espaço branco na fonte, corrigido pelo código depois.
    "NR_N_APOIO_PUB_ATIV_INOV_INT_UTI": DoubleType(),
    "NR_N_APOIO_PUB_ATIV_INOV_SEM_INT_UTI": DoubleType(),
}

# Aplicar cast no DataFrame para colunas existentes
#from pyspark.sql.functions import col

#for col_name, dtype in col_cast_map.items():
#    if col_name in df.columns:
#        df = df.withColumn(col_name, col(col_name).cast(dtype))


#for col_name, col_type in col_cast_map.items():
#    if col_name in df.columns:
#        df = df.withColumn(col_name, col(col_name).cast(col_type))
#for col_name, col_type in col_cast_map.items():
#    df = df.withColumn(col_name, col(col_name).cast(col_type))


#for col_name, col_type in col_cast_map.items():
#  if col_name in df.columns:
#    df = df.withColumn(col_name, col(col_name).cast(col_type))

#for origem, destino in col_rename_map.items():
#  df = df.withColumnRenamed(origem, destino)


# COMMAND ----------

#for f in dbutils.fs.ls(adl_raw):
#  print(f.name)
#  for a in dbutils.fs.ls(f'{adl_raw}/{f.name}'):

#    print(a.path)

# COMMAND ----------

# Regex para pegar os dois últimos segmentos
regex = r"([^/]+)/([^/]+)/?$"

for f in dbutils.fs.ls(adl_raw):
  print(f.name)
  for a in dbutils.fs.ls(f'{adl_raw}/{f.name}'):

    print(a.path)
    df = read_parquet(a.path)

    for col_name in df.columns:
        new_col_name = col_name.strip().upper()
        if new_col_name != col_name:
            df = df.withColumnRenamed(col_name, new_col_name)



    df = df.drop("dh_insercao_raw")
    df = df.filter(df['NR_ANO'].isNotNull())
    # Cast automático para as colunas existentes no DataFrame
    for col_name, col_type in col_cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, df[col_name].cast(col_type))
    
    fechamento = "camada trs - nova serie historica pintec 2021 2022 2023"

    df = tcf.add_control_fields(df, adf)

    match = re.search(regex, a.path)
    if match:
        # Extrai os dois grupos capturados
        part1, part2 = match.groups()
        print(f"{part1}/{part2}")# Regex para pegar os dois últimos segmentos
    path = f'{adl_trusted}{part1}/{part2}'
    print(path)
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(path)
    

# COMMAND ----------



# COMMAND ----------

for file_info in dbutils.fs.ls('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/_pintec/TIPO=po/'):
  print(file_info.path)
  # Extract using split
  base_name = file_info.path.split("NR_NOME_BASE=")[-1]
  print(base_name)  # Output: ef_pand_cnae/

  df = spark.read.format('parquet').load(file_info.path)


  import io
  import zipfile
  import requests

  # Converte Spark DataFrame para Pandas
  df_para_envio = df.toPandas()

  # Gera HTML em buffer de memória
  html_buffer = io.StringIO()
  df_para_envio.to_html(buf=html_buffer, index=False)
  html_content = html_buffer.getvalue().encode("utf-8")

  MAX_SIZE_DISCORD = 8 * 1024 * 1024  # 8MB

  webhook_url = "https://discord.com/api/webhooks/1388940448357417052/s1aWkSTVae-D3pRfldnJJYEKsGWTJx3UAmDFbaC3RB7IkCzfMlV3GUbGPiXHcMXdDOL2"

  if len(html_content) <= MAX_SIZE_DISCORD:
      files = {
          "file": (f"{base_name}.html", io.BytesIO(html_content), "text/html")
      }
      #payload = {
      #    "content": f"📋 Logs das execuções entre **{str_date_ini}** e **{str_dia_selecionado}**"
      #}
  else:
      zip_buffer = io.BytesIO()
      with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
          zip_file.writestr(f"{base_name}.html", html_content)
      zip_buffer.seek(0)

      files = {
          "file": (f"{base_name}.zip", zip_buffer, "application/zip")
      }
      #payload = {
      #    "content": f"📦 HTML compactado (ZIP) das execuções entre **{str_date_ini}** e **{str_dia_selecionado}**"
      #}
      # data=payload,
  response = requests.post(webhook_url, files=files)

  if response.status_code == 200:
      json_response = response.json()
      attachments = json_response.get("attachments", [])
      if attachments:
          file_url = attachments[0].get("url")
          print(f"✅ Arquivo enviado: {file_url}")

          link_payload = {
              "content": f"🔗 Link direto para download: {file_url}"
          }
          r = requests.post(webhook_url, json=link_payload)
          if r.status_code in (200, 204):
              print("✅ Link enviado com sucesso para o Discord.")
          else:
              print(f"⚠️ Falha ao enviar o link: {r.status_code} - {r.text}")
      else:
          print("⚠️ Mensagem enviada, mas sem anexo.")
  else:
      print(f"❌ Erro ao enviar para o Discord: {response.status_code} - {response.text}")

# COMMAND ----------

Validação

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/pintec_semestral/po/prod_proc_evolu_disp/')
df.display()


# COMMAND ----------

import io
import re
import requests
import time

# Webhook do Discord

webhook_url = "https://discord.com/api/webhooks/1388157056715985037/YXSo7VyPxwpH8d_Edqel13psoChz085R76uD6u9tQXubVT1RLLZJNGlO5X6Tz_lKOQu2"

# Regex para extrair os dois últimos segmentos do caminho
regex = r"([^/]+)/([^/]+)/?$"

for f in dbutils.fs.ls(adl_trusted):
    print(f.name)
    for a in dbutils.fs.ls(f'{adl_trusted}/{f.name}'):

        print(a.path)
        df = spark.read.format("parquet").load(a.path)

        match = re.search(regex, a.path)
        if match:
            part1, part2 = match.groups()
            print(f"{part1}/{part2}")

        path = f'{adl_trusted}/{part1}/{part2}'
        base_name = f'{part1}_{part2}'

        # Converte Spark DataFrame para Pandas
        df_para_envio = df.toPandas()

        # Gera HTML em buffer de memória
        html_buffer = io.StringIO()
        df_para_envio.to_html(buf=html_buffer, index=False)
        html_content = html_buffer.getvalue().encode("utf-8")

        # Prepara o envio direto do arquivo .html
        files = {
            "file": (f"{base_name}.html", html_content, "text/html")
        }

        # Envia ao Discord
        response = requests.post(webhook_url, files=files)

        if response.status_code == 200:
            json_response = response.json()
            attachments = json_response.get("attachments", [])
            if attachments:
                file_url = attachments[0].get("url")
                print(f"✅ Arquivo enviado: {file_url}")

                # Envia link direto como mensagem
                link_payload = {
                    "content": f"📄 Relatório HTML disponível: [Baixar {base_name}.html]({file_url})"
                }
                time.sleep(2)
                r = requests.post(webhook_url, json=link_payload)
                time.sleep(2)
                if r.status_code in (200, 204):
                    print("✅ Link enviado com sucesso para o Discord.")
                else:
                    print(f"⚠️ Falha ao enviar o link: {r.status_code} - {r.text}")
            else:
                print("⚠️ Mensagem enviada, mas sem anexo.")
        else:
            print(f"❌ Erro ao enviar para o Discord: {response.status_code} - {response.text}")

# COMMAND ----------

import io
import zipfile
import requests

regex = r"([^/]+)/([^/]+)/?$"

for f in dbutils.fs.ls(adl_trusted):
  print(f.name)
  for a in dbutils.fs.ls(f'{adl_trusted}/{f.name}'):

    print(a.path)
    df = spark.read.format("parquet").load(a.path)


    match = re.search(regex, a.path)
    if match:
        # Extrai os dois grupos capturados
        part1, part2 = match.groups()
        print(f"{part1}/{part2}")# Regex para pegar os dois últimos segmentos
    path = f'{adl_trusted}/{part1}/{part2}'

    base_name = path



    # Converte Spark DataFrame para Pandas
    df_para_envio = df.toPandas()

    # Gera HTML em buffer de memória
    html_buffer = io.StringIO()
    df_para_envio.to_html(buf=html_buffer, index=False)
    html_content = html_buffer.getvalue().encode("utf-8")

    MAX_SIZE_DISCORD = 8 * 1024 * 1024  # 8MB

    webhook_url = "https://discord.com/api/webhooks/1388157056715985037/YXSo7VyPxwpH8d_Edqel13psoChz085R76uD6u9tQXubVT1RLLZJNGlO5X6Tz_lKOQu2"

    if len(html_content) <= MAX_SIZE_DISCORD:
        files = {
            "file": (f"{path}.html", io.BytesIO(html_content), "text/html")
        }
        #payload = {
        #    "content": f"📋 Logs das execuções entre **{str_date_ini}** e **{str_dia_selecionado}**"
        #}
    else:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr(f"{base_name}.html", html_content)
        zip_buffer.seek(0)

        files = {
            "file": (f"{base_name}.zip", zip_buffer, "application/zip")
        }
        #payload = {
        #    "content": f"📦 HTML compactado (ZIP) das execuções entre **{str_date_ini}** e **{str_dia_selecionado}**"
        #}
        # data=payload,
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200:
        json_response = response.json()
        attachments = json_response.get("attachments", [])
        if attachments:
            file_url = attachments[0].get("url")
            print(f"✅ Arquivo enviado: {file_url}")

            link_payload = {
                "content": f"🔗 Link direto para download: {file_url}"
            }
            r = requests.post(webhook_url, json=link_payload)
            if r.status_code in (200, 204):
                print("✅ Link enviado com sucesso para o Discord.")
            else:
                print(f"⚠️ Falha ao enviar o link: {r.status_code} - {r.text}")
        else:
            print("⚠️ Mensagem enviada, mas sem anexo.")
    else:
        print(f"❌ Erro ao enviar para o Discord: {response.status_code} - {response.text}")

# COMMAND ----------

import io
import zipfile
import requests

regex = r"([^/]+)/([^/]+)/?$"

for f in dbutils.fs.ls(adl_trusted):
  print(f.name)
  for a in dbutils.fs.ls(f'{adl_trusted}/{f.name}'):

    print(a.path)
    df = spark.read.format("parquet").load(a.path)
    # Converte para Pandas
    df_para_envio = df.toPandas()

    match = re.search(regex, a.path)
    if match:
        # Extrai os dois grupos capturados
        part1, part2 = match.groups()
        print(f"{part1}/{part2}")# Regex para pegar os dois últimos segmentos
    path = f'{adl_trusted}/{part1}/{part2}'

    base_name = path

    # Gera o HTML
    html_str = df_para_envio.to_html(index=False, border=1, justify="center")
    html_content = html_str.encode("utf-8")

    # Compacta em ZIP
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(f"{base_name}.html", html_content)
    zip_buffer.seek(0)

    # Dados para envio
    files = {
        "file": (f"{base_name}.zip", zip_buffer, "application/zip")
    }
    webhook_url = "https://discord.com/api/webhooks/1388157056715985037/YXSo7VyPxwpH8d_Edqel13psoChz085R76uD6u9tQXubVT1RLLZJNGlO5X6Tz_lKOQu2"

    # Envia o ZIP para o Discord
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200:
        print("✅ Arquivo enviado com sucesso.")
        try:
            file_url = response.json().get("attachments", [])[0].get("url", "")
            if file_url:
                print(f"🔗 Link: {file_url}")
        except Exception:
            print("⚠️ Arquivo enviado, mas sem link de retorno.")
    else:
        print(f"❌ Erro ao enviar: {response.status_code} - {response.text}")

# COMMAND ----------



fechamento="camada trs - nova serie historica pintec 2021 2022 2023"
df.write.option("userMetadata",  fechamento) \
        .option("mergeSchema", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "auto") \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("TIPO","NR_NOME_BASE") \
        .save(path=adl_trusted)


#.option("delta.logRetentionDuration", "365 days") \        
#df.write.partitionBy('NR_ANO').mode('overwrite', ).parquet(path=adl_raw, mode='overwrite')

# COMMAND ----------

