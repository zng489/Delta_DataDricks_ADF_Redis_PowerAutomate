# Databricks notebook source

trs_rais_vinculo2008a2018_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/ibge/cnae_subclasses/'
df_rais_vinculo2008a2018 = spark.read.parquet(trs_rais_vinculo2008a2018_path)

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/ibge/cnae_subclasses/'
df = spark.read.format("parquet").option("header","true").load(var_adls_uri)
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# COMMAND ----------

LIST_TRS = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw")
LIST_TRS

data = []
for x in LIST_TRS:
  q = x[0]

  
  try:
    c = dbutils.fs.ls(x[0])

    # Not
    for t in c:
      
      try:
        t[0]
        print(t[0])
      
        for e in dbutils.fs.ls(t[0]):
          e[0]
          data.append(e[0])
          
      except:
        h = f'{t[0]}'
       
      data.append(t[0])
      data.append(h)
        
  except:
    v = f'{x[0]}'
    #print(v)
    #print(z.split('.')[-1].split('net')[-1]
    
  #data.append(q)

  data.append(v)
  
data  




# COMMAND ----------

dbutils.library.list()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

import pandas as pd

LIST_RAW = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/")

RAW = []
NAME = []
for each_one in LIST_RAW:
  path_raw = each_one[0].split('.')[-1].split('net')[-1]
  name = each_one[1].split('/')[0]
  
  RAW.append(path_raw)
  NAME.append(name)

data = {'RAW':RAW,'NAME':NAME}
data

df = pd.DataFrame(data)
df
#
#
#import pandas as pd
#
#data = {'Path':list_RAW[0][0],'Name':list_RAW[0][1]}
#
#df = pd.DataFrame([data])
#df

# COMMAND ----------

import pandas as pd

LIST_TRS = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/")

TRS = []
NAME = []
for each_one in LIST_TRS:
  path_trs = each_one[0].split('.')[-1].split('net')[-1]
  name = each_one[1].split('/')[0]
  
  TRS.append(path_trs)
  NAME.append(name)

data = {'TRS':TRS,'NAME':NAME}
data

df = pd.DataFrame(data)
df


# COMMAND ----------

import pandas as pd

LIST_BIZ = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/")

BIZ = []
NAME = []
for each_one in LIST_BIZ:
  path_biz = each_one[0].split('.')[-1].split('net')[-1]
  name = each_one[1].split('/')[0]
  
  BIZ.append(path_biz)
  NAME.append(name)

data = {'BIZ':BIZ,'NAME':NAME}
data

df = pd.DataFrame(data)
df

# COMMAND ----------

import pandas as pd

LIST_BIZ = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/uniepro/")

BIZ = []
NAME = []
for each_one in LIST_BIZ:
  path_biz = each_one[0].split('.')[-1].split('net')[-1]
  name = each_one[1].split('/')[0]
  
  BIZ.append(path_biz)
  NAME.append(name)

data = {'BIZ':BIZ,'NAME':NAME}
data

df = pd.DataFrame(data)
df

# COMMAND ----------

LIST_TRS = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/")
LIST_TRS

# COMMAND ----------

import pandas as pd

LIST_RAW = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uld")

RAW = []
NAME = []
for each_one in LIST_RAW:
  path_raw = each_one[0].split('.')[-1].split('net')[-1]
  name = each_one[1].split('/')[0]
  print(path_raw)

# COMMAND ----------

LIST_TRS = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw")
LIST_TRS

data = []
for x in LIST_TRS:
  q = x[0]

  
  try:
    c = dbutils.fs.ls(x[0])

    # Not
    for t in c:
      t[0]
      print(t[0])
      data.append(t[0])
      
      for e in dbutils.fs.ls(t[0]):
        e[0]
        data.append(e[0])
      
  except:
    v = f'{x[0]}'
    #print(v)
    #print(z.split('.')[-1].split('net')[-1]
    
  #data.append(q)

  data.append(v)
  
data  


data_set = []
for x in data:
  k = x.split('.')[-1].split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('ID')[0].split('nr')[0]
  data_set.append(k)
  
data_set  

mylist = list(dict.fromkeys(data_set))
#mylist.remove('parquet')


len(mylist)

dados = {'RAW':mylist}

df = pd.DataFrame(dados)



# COMMAND ----------

mylist

# COMMAND ----------

data_set = []
for x in data:
  k = x.split('.')[-1].split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('ID')[0].split('nr')[0]
  data_set.append(k)
  
data_set  


# COMMAND ----------

mylist = list(dict.fromkeys(data_set))
mylist.remove('parquet')

# COMMAND ----------

len(mylist)

# COMMAND ----------

LIST_TRS = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw")
LIST_TRS

data = []
for x in LIST_TRS:
  q = x[0]
  
  try:
    c = dbutils.fs.ls(x[0])
    # Not
    for t in c:
      t[0]
      data.append(t[0])
      
      for e in dbutils.fs.ls(t[0]):
        e[0]
        data.append(e[0])
      
  except:
    v = f'{x[0]}'
    #print(v)
    #print(z.split('.')[-1].split('net')[-1]
  data.append(q)
  #data.append(t[0])
  #data.append(e)
  data.append(v)
  
data  


data_set = []
for x in data:
  k = x.split('.')[-1].split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('ID')[0].split('nr')[0]
  data_set.append(k)
  
data_set  

mylist = list(dict.fromkeys(data_set))
mylist.remove('parquet')


len(mylist)

# COMMAND ----------

mylist

# COMMAND ----------

LIST_TRS = dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/")
LIST_TRS

data = []
for x in LIST_TRS:
  q = x[0]
  
  try:
    c = dbutils.fs.ls(x[0])
    # Not
    for t in c:
      t[0]
      data.append(t[0])
      
      for e in dbutils.fs.ls(t[0]):
        e[0]
        data.append(e[0])
      
  except:
    v = f'{x[0]}'
    #print(v)
    #print(z.split('.')[-1].split('net')[-1]
  data.append(q)
  #data.append(t[0])
  #data.append(e)
  data.append(v)
  
data  


data_set = []
for x in data:
  k = x.split('.')[-1].split('_committed')[0].split('_started')[0].split('NR')[0].split('_SUCCESS')[0].split('DT')[0].split('ANO')[0].split('ID')[0].split('nr')[0]
  data_set.append(k)
  
data_set  

mylist = list(dict.fromkeys(data_set))
mylist.remove('parquet')


len(mylist)

# COMMAND ----------

mylist

# COMMAND ----------

map_columns = {'UF': 'ufNome', 'Vacina': 'vacinaRedDetalhada', 'Data da Pauta': 'dtPNI', 'Nº Pauta': "numeroPauta", "Nome Pauta": "nomePauta",
               'Grupo Prioritário': 'grupo_atendimento_pauta',
               'População Alvo': 'populacaoAlvo', 'Tipo de Dose': 'tipoDose', 'Nº Dose 1 Pop. Alvo': 'qtDose1PopAlvo',
               'Nº Dose 2 Pop. Alvo': 'qtDose2PopAlvo', 'Nº Dose Única Pop. Alvo': 'qtDoseUnicaPopAlvo',
               'Nº Doses (Geral)': 'qtPNITodas', '% Ajuste': 'percentualAjuste', 'Nº Dose 1 (Geral)': 'qtPNI1',
               'Nº Dose 2 (Geral)': 'qtPNI2', 'Nº Dose Unica (Geral)': 'qtPNIUnica',
               'Nº Dose Adicional Pop. Alvo': 'qtDoseAdicionalPopAlvo',
               'Nº Dose Reforco Pop. Alvo': 'qtDoseReforcoPopAlvo', 'Caixas enviadas': 'qtCaixasEnviadas',
               'Doses por caixa': 'qtDosesPorCaixa', 'Doses encaminhadas': 'qtDosesEncaminhadas',
               '(D1 + D2 + DU + DA) * Ajuste': 'qtD1D2DUDAAjuste', 'D1 + DU': 'qtD1Du',
               'Nº Dose Adicional (Geral)': 'qtPNIAdicional', 'Nº Dose Reforco (Geral)': 'qtPNIReforco'
               }


# COMMAND ----------

sparkDF2.display()

# COMMAND ----------

import pandas as pd

data = {'Path':Path,'Name':Name}

df = pd.DataFrame([data])
df

# COMMAND ----------

Path

# COMMAND ----------

import pandas as pd

data = {'Path':list_RAW[0][0],'Name':list_RAW[0][1]}

df = pd.DataFrame([data])
df

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import pandas as pd

data = {'Path':list_RAW[0][0],'Name':list_RAW[0][1]}

df = pd.DataFrame([data])
df

# COMMAND ----------

list_RAW[0]

# COMMAND ----------

list_RAW[0][0]

# COMMAND ----------

list_RAW[0][1]

# COMMAND ----------

def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths
    

paths = get_dir_content('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net')
[print(p) for p in paths]

# COMMAND ----------

def get_csv_files(directory_path):
  """recursively list path of all csv files in path directory """
  csv_files = []
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.csv'):
      csv_files.append(path)
  return csv_files

# COMMAND ----------

get_csv_files('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net')

# COMMAND ----------

import os
os.listdir('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/')

# COMMAND ----------

dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro")

# COMMAND ----------

dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/crw")

# COMMAND ----------

['/tmp/dev/raw/usr/ibge/pintec/cnae/bio_nano_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/grau_nov_imp_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/org_mkt_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/prod_vend_int_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_loc_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/resp_imp_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_inoc_proj_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_programa_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/po/bio_nano_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/grau_nov_imp_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/org_mkt_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
 '/tmp/dev/raw/crw/rfb_cno/',
 '/tmp/dev/raw/crw/pintec/faixa_ocup/',
 '/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_motivo_situacao/hot/
 '/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_municipios/hot/']

# COMMAND ----------

frames = [d, df_pandas]
result = pd.concat(frames)

from pyspark.sql.types import *
df_schema = StructType([StructField("path", StringType(), True)\
                       ,StructField("rows", StringType(), True)])

s = spark.createDataFrame(result,df_schema)  
s.display()
#
#var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
#ccs.coalesce(1).write.format('csv').save(var_adls_uri + '/uds/uniepro/data/provisoria/', sep=";", header = True, mode='overwrite')

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
s.coalesce(1).write.format('csv').save(var_adls_uri + '/uds/uniepro/data/provisoria/', sep=";", header = True, mode='overwrite')

# COMMAND ----------


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
x = /uds/uniepro/data/provisoria/
df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')

uds/uniepro/data/provisoria/

# COMMAND ----------

path_tmp = ['/tmp/dev/raw/crw/pintec/faixa_ocup/2017/tab_1.2.1/',
 '/tmp/dev/raw/crw/vagascom/vagas/',
 '/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_atributos_cnae/hot/',
 '/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_natu_jur/hot/']

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data = []
for x in path_tmp:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  #list_schema_names = df.schema.names
  #print(df.count())
  path = x
  rows = df.count()
  
  data.append({'path':path, 'rows':rows})
 
df_pandas = pd.DataFrame(data)
df_pandas


# COMMAND ----------

path_tmp = ['/tmp/dev/raw/crw/banco_mundial/indicadores_selecionados/',
'/tmp/dev/raw/crw/banco_mundial/documentacao_paises/',
'/tmp/dev/raw/crw/banco_mundial/documentacao_indica/',
'/tmp/dev/raw/crw/aneel/gera_energia_distrib/',
'/tmp/dev/raw/crw/bne/vagas/',
'/tmp/dev/raw/crw/catho/vagas/',
'/tmp/dev/raw/crw/fmi_proj/pais/',
'/tmp/dev/raw/crw/fmi_proj/grupo_paises/',
'/raw/crw/ibge/cnae_subclasses/',
'/raw/crw/ibge/deflatores/',
'/raw/crw/ibge/ipca/',
'/raw/crw/ibge/pintec_bio_nano/',
'/raw/crw/ibge/pintec_grau_nov_imp/',
'/raw/crw/ibge/pintec_org_mkt/',
'/raw/crw/ibge/pintec_prob_obst/',
'/raw/crw/ibge/pintec_prod_vend_int/',
'/raw/crw/ibge/pintec_rel_coop/',
'/raw/crw/ibge/pintec_rel_coop_loc/',
'/raw/crw/ibge/pintec_resp_imp/',
'/raw/crw/ibge/pintec_tipo_inov_proj/',
'/raw/crw/ibge/pintec_tipo_programa/']
#'/tmp/dev/raw/usr/ibge/pintec/cnae/bio_nano_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/grau_nov_imp_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/org_mkt_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/']
#'/tmp/dev/raw/usr/ibge/pintec/cnae/prod_vend_int_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_loc_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/resp_imp_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_inoc_proj_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_programa_cnae/',
#'/tmp/dev/raw/usr/ibge/pintec/po/bio_nano_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/grau_nov_imp_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/org_mkt_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
#'/raw/crw/ibge/pnadc/',
#'/raw/crw/ibge/pop_estimada/',
#'/raw/crw/ibge/pop_projetada_expectativa_txmortal/',
#'/raw/crw/ibge/pop_projetada_indicadores_implicitos/',
#'/raw/crw/ibge/pop_projetada_pessoas_prop/',
#'/raw/crw/ibge/pop_projetada_pop_sex_idade/',
#'/raw/crw/ibge/pop_projetada_txfecund/',
#'/raw/crw/ibge/relatorio_dtb_brasil_municipio/',
#'/raw/crw/ibge/scnt_conta_financeira_unificado/',
#'/raw/crw/ibge/scnt_contas_economicas_unificado/',
#'/raw/crw/ibge/scnt_valor_preco1995_saz_unificado/',
#'/raw/crw/ibge/scnt_valor_preco1995_unificado/',
#'/raw/crw/ibge/scnt_valor_preco_correntes_unificado/',
#'/raw/crw/ibge/scnt_volume_saz_se_unificado/',
#'/raw/crw/ibge/scnt_volume_se_unificado/',
#'/raw/crw/ibge/scnt_volume_tx_unificado/',
#'/raw/crw/inep_afd/brasil_regioes_e_ufs/',
#'/raw/crw/inep_afd/escolas/',
#'/raw/crw/inep_afd/municipios/',
#'/raw/crw/inep_atu/brasil_regioes_e_ufs/',
#'/raw/crw/inep_atu/escolas/',
#'/raw/crw/inep_atu/municipios/',
#'/raw/crw/inep_censo_escolar/curso_educacao_profissional/',
#'/raw/crw/inep_censo_escolar/docente/',
#'/raw/crw/inep_censo_escolar/escola/',
#'/raw/crw/inep_censo_escolar/etapa_ensino/',
#'/raw/crw/inep_censo_escolar/gestor/',
#'/raw/crw/inep_censo_escolar/matriculas/',
#'/raw/crw/inep_censo_escolar/turmas/',
#'/raw/crw/inep_dsu/brasil_regioes_e_ufs/',
#'/raw/crw/inep_dsu/escolas/',
#'/raw/crw/inep_dsu/municipios/',
#'/raw/crw/inep_enem/enem_escola/',
#'/raw/crw/inep_enem/enem_escola_dicionario/',
#'/raw/crw/inep_enem/microdados_enem/',
#'/raw/crw/inep_had/brasil_regioes_e_ufs/',
#'/raw/crw/inep_had/escolas/',
#'/raw/crw/inep_had/municipios/',
#'/raw/crw/inep_icg/escolas/',
#'/raw/crw/inep_ideb/ideb/',
#'/raw/crw/inep_inse/socioeconomico/',
#'/tmp/dev/raw/crw/inep_iqes/cpc/',
#'/tmp/dev/raw/crw/inep_iqes/icg/',
#'/raw/crw/inep_ird/escolas/',
#'/raw/crw/inep_prova_brasil/ts_quest_aluno/',
#'/raw/crw/inep_prova_brasil/ts_resposta_aluno/',
#'/raw/crw/inep_prova_brasil/ts_resultado_aluno/',
#'/raw/crw/inep_rmd/brasil_regioes_e_ufs/',
#'/raw/crw/inep_rmd/municipios/',
#'/raw/crw/inep_saeb/prova_brasil_2013/',
#'/raw/crw/inep_saeb/prova_brasil_2015/',
#'/raw/crw/inep_saeb/prova_brasil_2017/',
#'/raw/crw/inep_saeb/prova_brasil_2019/',
#'/raw/crw/inep_saeb/saeb_aluno_unificado/',
#'/raw/crw/inep_saeb/saeb_diretor_unificada/',
#'/raw/crw/inep_saeb/saeb_escola_unificada/',
#'/raw/crw/inep_saeb/saeb_professor_unificada/',
#'/raw/crw/inep_saeb/saeb_resultado_brasil_unificado/',
#'/raw/crw/inep_saeb/saeb_resultado_municipio_unificado/',
#'/raw/crw/inep_saeb/saeb_resultado_regiao_unificado/',
#'/raw/crw/inep_saeb/saeb_resultado_uf_unificado/',
#'/raw/crw/inep_saeb/ts_quest_aluno/',
#'/raw/crw/inep_saeb/ts_resposta_aluno/',
#'/raw/crw/inep_saeb/ts_resultado_aluno/',
#'/raw/crw/inep_taxa_rendimento/brasil_regioes_e_ufs/',
#'/raw/crw/inep_taxa_rendimento/escolas/',
#'/raw/crw/inep_taxa_rendimento/municipios/',
#'/raw/crw/inep_tdi/brasil_regioes_e_ufs/',
#'/raw/crw/inep_tdi/escolas/',
#'/raw/crw/inep_tdi/municipios/',
#'/raw/crw/infojobs/vagas/',
#'/tmp/dev/raw/crw/inss/cat_copy/',
#'/raw/crw/me/caged/',
#'/raw/crw/me/caged_ajustes/',
#'/raw/crw/me/novo_caged_exc/',
#'/raw/crw/me/novo_caged_for/',
#'/raw/crw/me/novo_caged_mov/',
#'/tmp/dev/raw/crw/mec/pnp_efi/',
#'/tmp/dev/raw/crw/mec/pnp_fin/',
#'/tmp/dev/raw/crw/mec/pnp_mat/',
#'/tmp/dev/raw/crw/mec/pnp_ser/']
##'/tmp/dev/raw/crw/ms_sinan/acbi/'
##'/tmp/dev/raw/crw/ms_sinan/acgr/',
##'/tmp/dev/raw/crw/ms_sinan/canc/',
##'/tmp/dev/raw/crw/ms_sinan/derm/',
##'/tmp/dev/raw/crw/ms_sinan/lerd/',
##'/tmp/dev/raw/crw/ms_sinan/ment/',
##'/tmp/dev/raw/crw/ms_sinan/pair/',
##'/tmp/dev/raw/crw/ocde/projecoes_economicas/',
##'/tmp/dev/raw/crw/vagascertas/vagas/',
##'/raw/usr/me/cadastro_cbo/',
##'/raw/usr/me/rais_estabelecimento/',
##'/raw/usr/me/rais_vinculo/',
##'/tmp/dev/raw/crw/rfb_cno/',
##'/tmp/dev/raw/crw/pintec/faixa_ocup/',
##'/tmp/dev/raw/crw/vagascom/vagas/',
##'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_atributos_cnae/hot/',
##'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_motivo_situacao/hot/',
##'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_municipios/hot/',
##'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_natu_jur/hot/',
##'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_pais/hot/',
##'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_quali_socios/hot/',
##'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_atributos_cnae/hot/']

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data = []
for x in path_tmp:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  #list_schema_names = df.schema.names
  #print(df.count())
  path = x
  rows = df.count()
  
  data.append({'path':path, 'rows':rows})
 
df_pandas = pd.DataFrame(data)
df_pandas


#sparkDF=spark.createDataFrame(pd.Dataframe(data))  
#sparkDF.display()


# COMMAND ----------

path_tmp = ['/raw/crw/inep_afd/municipios/',
'/raw/crw/inep_atu/brasil_regioes_e_ufs/',
'/raw/crw/inep_atu/escolas/',
'/raw/crw/inep_atu/municipios/',
'/raw/crw/inep_censo_escolar/curso_educacao_profissional/',
'/raw/crw/inep_censo_escolar/docente/',
'/raw/crw/inep_censo_escolar/escola/',
'/raw/crw/inep_censo_escolar/etapa_ensino/',
'/raw/crw/inep_censo_escolar/gestor/',
'/raw/crw/inep_censo_escolar/matriculas/',
'/raw/crw/inep_censo_escolar/turmas/',
'/raw/crw/inep_dsu/brasil_regioes_e_ufs/',
'/raw/crw/inep_dsu/escolas/',
'/raw/crw/inep_dsu/municipios/',
'/raw/crw/inep_enem/enem_escola/',
'/raw/crw/inep_enem/enem_escola_dicionario/',
'/raw/crw/inep_enem/microdados_enem/',
'/raw/crw/inep_had/brasil_regioes_e_ufs/',
'/raw/crw/inep_had/escolas/',
'/raw/crw/inep_had/municipios/',
'/raw/crw/inep_icg/escolas/',
'/raw/crw/inep_ideb/ideb/',
'/raw/crw/inep_inse/socioeconomico/',
'/tmp/dev/raw/crw/inep_iqes/cpc/',
'/tmp/dev/raw/crw/inep_iqes/icg/',
'/raw/crw/inep_ird/escolas/',
'/raw/crw/inep_prova_brasil/ts_quest_aluno/',
'/raw/crw/inep_prova_brasil/ts_resposta_aluno/',
'/raw/crw/inep_prova_brasil/ts_resultado_aluno/',
'/raw/crw/inep_rmd/brasil_regioes_e_ufs/',
'/raw/crw/inep_rmd/municipios/',
'/raw/crw/inep_saeb/prova_brasil_2013/',
'/raw/crw/inep_saeb/prova_brasil_2015/',
'/raw/crw/inep_saeb/prova_brasil_2017/',
'/raw/crw/inep_saeb/prova_brasil_2019/',
'/raw/crw/inep_saeb/saeb_aluno_unificado/',
'/raw/crw/inep_saeb/saeb_diretor_unificada/',
'/raw/crw/inep_saeb/saeb_escola_unificada/',
'/raw/crw/inep_saeb/saeb_professor_unificada/',
'/raw/crw/inep_saeb/saeb_resultado_brasil_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_municipio_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_regiao_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_uf_unificado/',
'/raw/crw/inep_saeb/ts_quest_aluno/',
'/raw/crw/inep_saeb/ts_resposta_aluno/',
'/raw/crw/inep_saeb/ts_resultado_aluno/',
'/raw/crw/inep_taxa_rendimento/brasil_regioes_e_ufs/',
'/raw/crw/inep_taxa_rendimento/escolas/',
'/raw/crw/inep_taxa_rendimento/municipios/',
'/raw/crw/inep_tdi/brasil_regioes_e_ufs/',
'/raw/crw/inep_tdi/escolas/',
'/raw/crw/inep_tdi/municipios/',
'/raw/crw/infojobs/vagas/',
'/tmp/dev/raw/crw/inss/cat_copy/',
'/raw/crw/me/caged/',
'/raw/crw/me/caged_ajustes/',
'/raw/crw/me/novo_caged_exc/',
'/raw/crw/me/novo_caged_for/',
'/raw/crw/me/novo_caged_mov/',
'/tmp/dev/raw/crw/mec/pnp_efi/',
'/tmp/dev/raw/crw/mec/pnp_fin/',
'/tmp/dev/raw/crw/mec/pnp_mat/',
'/tmp/dev/raw/crw/mec/pnp_ser/']



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data = []
for x in path_tmp:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  #list_schema_names = df.schema.names
  #print(df.count())
  path = x
  rows = df.count()
  
  data.append({'path':path, 'rows':rows})
 
df_pandas_0 = pd.DataFrame(data)
df_pandas_0



# COMMAND ----------

path_tmp = ['/tmp/dev/raw/crw/ms_sinan/acbi/',
'/tmp/dev/raw/crw/ms_sinan/acgr/',
'/tmp/dev/raw/crw/ms_sinan/canc/',
'/tmp/dev/raw/crw/ms_sinan/derm/',
'/tmp/dev/raw/crw/ms_sinan/lerd/',
'/tmp/dev/raw/crw/ms_sinan/ment/',
'/tmp/dev/raw/crw/ms_sinan/pair/',
'/tmp/dev/raw/crw/ocde/projecoes_economicas/',
'/tmp/dev/raw/crw/vagascertas/vagas/',
'/raw/usr/me/cadastro_cbo/',
'/raw/usr/me/rais_estabelecimento/']


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data = []
for x in path_tmp:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  #list_schema_names = df.schema.names
  #print(df.count())
  path = x
  rows = df.count()
  
  data.append({'path':path, 'rows':rows})
 
df_pandas_1 = pd.DataFrame(data)
df_pandas_1

# COMMAND ----------

path_tmp = ['/raw/crw/ibge/pnadc/',
'/raw/crw/ibge/pop_estimada/',
'/raw/crw/ibge/pop_projetada_expectativa_txmortal/',
'/raw/crw/ibge/pop_projetada_indicadores_implicitos/',
'/raw/crw/ibge/pop_projetada_pessoas_prop/',
'/raw/crw/ibge/pop_projetada_pop_sex_idade/',
'/raw/crw/ibge/pop_projetada_txfecund/',
'/raw/crw/ibge/relatorio_dtb_brasil_municipio/',
'/raw/crw/ibge/scnt_conta_financeira_unificado/',
'/raw/crw/ibge/scnt_contas_economicas_unificado/',
'/raw/crw/ibge/scnt_valor_preco1995_saz_unificado/',
'/raw/crw/ibge/scnt_valor_preco1995_unificado/',
'/raw/crw/ibge/scnt_valor_preco_correntes_unificado/',
'/raw/crw/ibge/scnt_volume_saz_se_unificado/',
'/raw/crw/ibge/scnt_volume_se_unificado/',
'/raw/crw/ibge/scnt_volume_tx_unificado/',
'/raw/crw/inep_afd/brasil_regioes_e_ufs/',
'/raw/crw/inep_afd/escolas/',
'/raw/usr/me/rais_vinculo/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_pais/hot/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_quali_socios/hot/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_atributos_cnae/hot/',
'/tmp/dev/raw/crw/vagascom/vagas/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_atributos_cnae/hot/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_motivo_situacao/hot/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_municipios/hot/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_natu_jur/hot/']

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data = []
for x in path_tmp:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  #list_schema_names = df.schema.names
  #print(df.count())
  path = x
  rows = df.count()
  
  data.append({'path':path, 'rows':rows})
 
df_pandas_2 = pd.DataFrame(data)
df_pandas_2

# COMMAND ----------

frames = [df_pandas, df_pandas_1, df_pandas_2]
result = pd.concat(frames)

# COMMAND ----------

result

# COMMAND ----------


frames = [df_pandas, df_pandas_1, df_pandas_2]
result = pd.concat(frames)
s = spark.createDataFrame(pd.DataFrame(result))  
s.display()

# COMMAND ----------


frames = [df_pandas, df_pandas_1, df_pandas_2]
result = pd.concat(frames)
s = spark.createDataFrame(pd.DataFrame(result))  
s.display()

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
s.coalesce(1).write.format('csv').save(var_adls_uri + '/uds/uniepro/data/provisoria/', sep=";", header = True, mode='overwrite')

# COMMAND ----------


spark.read.format("parquet").option("header","true").option('sep',';').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_pais/hot/').display()

# COMMAND ----------

#'/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',



#'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
#'/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
#'/tmp/dev/raw/crw/rfb_cno/',
#'/tmp/dev/raw/crw/pintec/faixa_ocup/',
#'/tmp/dev/raw/crw/vagascom/vagas/',
#'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_atributos_cnae/hot/',
#'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_motivo_situacao/hot/'
#'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_municipios/hot/',

# COMMAND ----------

spark.read.format("parquet").option("header","true").option('sep',';').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/')

# COMMAND ----------

path_tmp = ['/tmp/dev/raw/usr/ibge/pintec/cnae/bio_nano_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/grau_nov_imp_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/org_mkt_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/prod_vend_int_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_loc_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/resp_imp_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_inoc_proj_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_programa_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/po/bio_nano_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/grau_nov_imp_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/org_mkt_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
'/tmp/dev/raw/crw/rfb_cno/',
'/tmp/dev/raw/crw/pintec/faixa_ocup/',
'/tmp/dev/raw/crw/vagascom/vagas/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_atributos_cnae/hot/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_motivo_situacao/hot/'
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_municipios/hot/',
'/tmp/dev/uld/fiesc/rfb/dimensoes/dim_rfb_natu_jur/hot/',
'/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',           
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
'/tmp/dev/raw/crw/pintec/faixa_ocup/2017/tab_1.2.1/']



var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
data = []

for x in path_tmp:
  try:
  #print(f'{var_adls_uri}{x}')
    df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
    right = x
    #right.append({'right':right})
    
  except:
    error = x
    #error.append({'error':error})
    
  #list_schema_names = df.schema.names
  #print(df.count())
  
#    path = x
#    rows = df.count()
#  
  data.append({'right':right, 'error':error})
#
df_p = pd.DataFrame(data)
df_p

# COMMAND ----------

list(df_p['error'].drop_duplicates())

# COMMAND ----------

df = spark.read.format("parquet").option("header","true").option('sep',';').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/')


# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
path = f'{var_adls_uri}/tmp/dev/raw/crw/ms_sinan/pair/'

df = spark.read.format("parquet").option("header","true").option('sep',';').load(path)
df.coalesce(1).write.mode("overwrite").option('header', True).csv('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/data/Juca')

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls("abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/")

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/raw/crw/ibge/pintec_bio_nano/'

df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

/raw/crw/ibge/pintec_bio_nano/
# lista_de_endereco = ['/tmp/dev/raw/crw/banco_mundial/documentacao_paises/', '/tmp/dev/raw/crw/banco_mundial/documentacao_indica/']

lista_de_endereco = ['/tmp/dev/raw/crw/banco_mundial/indicadores_selecionados/',
'/tmp/dev/raw/crw/banco_mundial/documentacao_paises/',
'/tmp/dev/raw/crw/banco_mundial/documentacao_indica/',
'/tmp/dev/raw/crw/aneel/gera_energia_distrib/',
'/tmp/dev/raw/crw/bne/vagas/',
'/tmp/dev/raw/crw/catho/vagas/',
'/tmp/dev/raw/crw/fmi_proj/pais/',
'/tmp/dev/raw/crw/fmi_proj/grupo_paises/',
'/raw/crw/ibge/cnae_subclasses/',
'/raw/crw/ibge/deflatores/',
'/raw/crw/ibge/ipca/',
'/raw/crw/ibge/pintec_bio_nano/',
'/raw/crw/ibge/pintec_grau_nov_imp/',
'/raw/crw/ibge/pintec_org_mkt/',
'/raw/crw/ibge/pintec_prob_obst/',
'/raw/crw/ibge/pintec_prod_vend_int/',
'/raw/crw/ibge/pintec_rel_coop/',
'/raw/crw/ibge/pintec_rel_coop_loc/',
'/raw/crw/ibge/pintec_resp_imp/',
'/raw/crw/ibge/pintec_tipo_inov_proj/',
'/raw/crw/ibge/pintec_tipo_programa/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/bio_nano_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/grau_nov_imp_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/org_mkt_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/prod_vend_int_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_loc_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/resp_imp_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_inoc_proj_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_programa_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/po/bio_nano_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/grau_nov_imp_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/org_mkt_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
'/raw/crw/ibge/pnadc/',
'/raw/crw/ibge/pop_estimada/',
'/raw/crw/ibge/pop_projetada_expectativa_txmortal/',
'/raw/crw/ibge/pop_projetada_indicadores_implicitos/',
'/raw/crw/ibge/pop_projetada_pessoas_prop/',
'/raw/crw/ibge/pop_projetada_pop_sex_idade/',
'/raw/crw/ibge/pop_projetada_txfecund/',
'/raw/crw/ibge/relatorio_dtb_brasil_municipio/',
'/raw/crw/ibge/scnt_conta_financeira_unificado/',
'/raw/crw/ibge/scnt_contas_economicas_unificado/',
'/raw/crw/ibge/scnt_valor_preco1995_saz_unificado/',
'/raw/crw/ibge/scnt_valor_preco1995_unificado/',
'/raw/crw/ibge/scnt_valor_preco_correntes_unificado/',
'/raw/crw/ibge/scnt_volume_saz_se_unificado/',
'/raw/crw/ibge/scnt_volume_se_unificado/',
'/raw/crw/ibge/scnt_volume_tx_unificado/',
'/raw/crw/inep_afd/brasil_regioes_e_ufs/',
'/raw/crw/inep_afd/escolas/',
'/raw/crw/inep_afd/municipios/',
'/raw/crw/inep_atu/brasil_regioes_e_ufs/',
'/raw/crw/inep_atu/escolas/',
'/raw/crw/inep_atu/municipios/',
'/raw/crw/inep_censo_escolar/curso_educacao_profissional/',
'/raw/crw/inep_censo_escolar/docente/',
'/raw/crw/inep_censo_escolar/escola/',
'/raw/crw/inep_censo_escolar/etapa_ensino/',
'/raw/crw/inep_censo_escolar/gestor/',
'/raw/crw/inep_censo_escolar/matriculas/',
'/raw/crw/inep_censo_escolar/turmas/',
'/raw/crw/inep_dsu/brasil_regioes_e_ufs/',
'/raw/crw/inep_dsu/escolas/',
'/raw/crw/inep_dsu/municipios/',
'/raw/crw/inep_enem/enem_escola/',
'/raw/crw/inep_enem/enem_escola_dicionario/',
'/raw/crw/inep_enem/microdados_enem/',
'/raw/crw/inep_had/brasil_regioes_e_ufs/',
'/raw/crw/inep_had/escolas/',
'/raw/crw/inep_had/municipios/',
'/raw/crw/inep_icg/escolas/',
'/raw/crw/inep_ideb/ideb/',
'/raw/crw/inep_inse/socioeconomico/',
'/tmp/dev/raw/crw/inep_iqes/cpc/',
'/tmp/dev/raw/crw/inep_iqes/icg/',
'/raw/crw/inep_ird/escolas/',
'/raw/crw/inep_prova_brasil/ts_quest_aluno/',
'/raw/crw/inep_prova_brasil/ts_resposta_aluno/',
'/raw/crw/inep_prova_brasil/ts_resultado_aluno/',
'/raw/crw/inep_rmd/brasil_regioes_e_ufs/',
'/raw/crw/inep_rmd/municipios/',
'/raw/crw/inep_saeb/prova_brasil_2013/',
'/raw/crw/inep_saeb/prova_brasil_2015/',
'/raw/crw/inep_saeb/prova_brasil_2017/',
'/raw/crw/inep_saeb/prova_brasil_2019/',
'/raw/crw/inep_saeb/saeb_aluno_unificado/',
'/raw/crw/inep_saeb/saeb_diretor_unificada/',
'/raw/crw/inep_saeb/saeb_escola_unificada/',
'/raw/crw/inep_saeb/saeb_professor_unificada/',
'/raw/crw/inep_saeb/saeb_resultado_brasil_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_municipio_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_regiao_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_uf_unificado/',
'/raw/crw/inep_saeb/ts_quest_aluno/',
'/raw/crw/inep_saeb/ts_resposta_aluno/',
'/raw/crw/inep_saeb/ts_resultado_aluno/',
'/raw/crw/inep_taxa_rendimento/brasil_regioes_e_ufs/',
'/raw/crw/inep_taxa_rendimento/escolas/',
'/raw/crw/inep_taxa_rendimento/municipios/',
'/raw/crw/inep_tdi/brasil_regioes_e_ufs/',
'/raw/crw/inep_tdi/escolas/',
'/raw/crw/inep_tdi/municipios/',
'/raw/crw/infojobs/vagas/',
'/tmp/dev/raw/crw/inss/cat_copy/',
'/raw/crw/me/caged/',
'/raw/crw/me/caged_ajustes/',
'/raw/crw/me/novo_caged_exc/',
'/raw/crw/me/novo_caged_for/',
'/raw/crw/me/novo_caged_mov/',
'/tmp/dev/raw/crw/mec/pnp_efi/',
'/tmp/dev/raw/crw/mec/pnp_fin/',
'/tmp/dev/raw/crw/mec/pnp_mat/',
'/tmp/dev/raw/crw/mec/pnp_ser/',
'/tmp/dev/raw/crw/ms_sinan/acbi/',
'/tmp/dev/raw/crw/ms_sinan/acgr/',
'/tmp/dev/raw/crw/ms_sinan/canc/',
'/tmp/dev/raw/crw/ms_sinan/derm/',
'/tmp/dev/raw/crw/ms_sinan/lerd/',
'/tmp/dev/raw/crw/ms_sinan/ment/',
'/tmp/dev/raw/crw/ms_sinan/pair/',
'/tmp/dev/raw/crw/ocde/projecoes_economicas/',
'/tmp/dev/raw/crw/vagascertas/vagas/',
'/raw/usr/me/cadastro_cbo/',
'/raw/usr/me/rais_estabelecimento/',
'/raw/usr/me/rais_vinculo/']


dataset = []
for x in lista_de_endereco:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  list_schema_names = df.schema.names
  print(list_schema_names)
  
  Column = {'Path_Temp':f'{x}',
            'Nomes_das_colunas':list_schema_names}
  
  Tabela = pd.DataFrame(Column)
Tabela
  

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'


lista_de_endereco = ['/tmp/dev/raw/crw/banco_mundial/documentacao_paises/', '/tmp/dev/raw/crw/banco_mundial/documentacao_indica/']



dataset = []
for x in lista_de_endereco:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  list_schema_names = df.schema.names
  #print(list_schema_names)
  
  
  
  Column = {'Path':f'{x}',
            'Nomes_das_colunas':list_schema_names}
  
  dataset.append(Column)
dataset
pd.DataFrame(dataset)
  
#Tabela

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd


var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'


# lista_de_endereco = ['/tmp/dev/raw/crw/banco_mundial/documentacao_paises/', '/tmp/dev/raw/crw/banco_mundial/documentacao_indica/']

lista_de_endereco = ['/tmp/dev/raw/crw/banco_mundial/indicadores_selecionados/',
'/tmp/dev/raw/crw/banco_mundial/documentacao_paises/',
'/tmp/dev/raw/crw/banco_mundial/documentacao_indica/',
'/tmp/dev/raw/crw/aneel/gera_energia_distrib/',
'/tmp/dev/raw/crw/bne/vagas/',
'/tmp/dev/raw/crw/catho/vagas/',
'/tmp/dev/raw/crw/fmi_proj/pais/',
'/tmp/dev/raw/crw/fmi_proj/grupo_paises/',
'/raw/crw/ibge/cnae_subclasses/',
'/raw/crw/ibge/deflatores/',
'/raw/crw/ibge/ipca/',
'/raw/crw/ibge/pintec_bio_nano/',
'/raw/crw/ibge/pintec_grau_nov_imp/',
'/raw/crw/ibge/pintec_org_mkt/',
'/raw/crw/ibge/pintec_prob_obst/',
'/raw/crw/ibge/pintec_prod_vend_int/',
'/raw/crw/ibge/pintec_rel_coop/',
'/raw/crw/ibge/pintec_rel_coop_loc/',
'/raw/crw/ibge/pintec_resp_imp/',
'/raw/crw/ibge/pintec_tipo_inov_proj/',
'/raw/crw/ibge/pintec_tipo_programa/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/bio_nano_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/grau_nov_imp_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/org_mkt_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/prod_vend_int_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_loc_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/resp_imp_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_inoc_proj_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_programa_cnae/',
'/tmp/dev/raw/usr/ibge/pintec/po/bio_nano_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/grau_nov_imp_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/org_mkt_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
'/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
'/raw/crw/ibge/pnadc/',
'/raw/crw/ibge/pop_estimada/',
'/raw/crw/ibge/pop_projetada_expectativa_txmortal/',
'/raw/crw/ibge/pop_projetada_indicadores_implicitos/',
'/raw/crw/ibge/pop_projetada_pessoas_prop/',
'/raw/crw/ibge/pop_projetada_pop_sex_idade/',
'/raw/crw/ibge/pop_projetada_txfecund/',
'/raw/crw/ibge/relatorio_dtb_brasil_municipio/',
'/raw/crw/ibge/scnt_conta_financeira_unificado/',
'/raw/crw/ibge/scnt_contas_economicas_unificado/',
'/raw/crw/ibge/scnt_valor_preco1995_saz_unificado/',
'/raw/crw/ibge/scnt_valor_preco1995_unificado/',
'/raw/crw/ibge/scnt_valor_preco_correntes_unificado/',
'/raw/crw/ibge/scnt_volume_saz_se_unificado/',
'/raw/crw/ibge/scnt_volume_se_unificado/',
'/raw/crw/ibge/scnt_volume_tx_unificado/',
'/raw/crw/inep_afd/brasil_regioes_e_ufs/',
'/raw/crw/inep_afd/escolas/',
'/raw/crw/inep_afd/municipios/',
'/raw/crw/inep_atu/brasil_regioes_e_ufs/',
'/raw/crw/inep_atu/escolas/',
'/raw/crw/inep_atu/municipios/',
'/raw/crw/inep_censo_escolar/curso_educacao_profissional/',
'/raw/crw/inep_censo_escolar/docente/',
'/raw/crw/inep_censo_escolar/escola/',
'/raw/crw/inep_censo_escolar/etapa_ensino/',
'/raw/crw/inep_censo_escolar/gestor/',
'/raw/crw/inep_censo_escolar/matriculas/',
'/raw/crw/inep_censo_escolar/turmas/',
'/raw/crw/inep_dsu/brasil_regioes_e_ufs/',
'/raw/crw/inep_dsu/escolas/',
'/raw/crw/inep_dsu/municipios/',
'/raw/crw/inep_enem/enem_escola/',
'/raw/crw/inep_enem/enem_escola_dicionario/',
'/raw/crw/inep_enem/microdados_enem/',
'/raw/crw/inep_had/brasil_regioes_e_ufs/',
'/raw/crw/inep_had/escolas/',
'/raw/crw/inep_had/municipios/',
'/raw/crw/inep_icg/escolas/',
'/raw/crw/inep_ideb/ideb/',
'/raw/crw/inep_inse/socioeconomico/',
'/tmp/dev/raw/crw/inep_iqes/cpc/',
'/tmp/dev/raw/crw/inep_iqes/icg/',
'/raw/crw/inep_ird/escolas/',
'/raw/crw/inep_prova_brasil/ts_quest_aluno/',
'/raw/crw/inep_prova_brasil/ts_resposta_aluno/',
'/raw/crw/inep_prova_brasil/ts_resultado_aluno/',
'/raw/crw/inep_rmd/brasil_regioes_e_ufs/',
'/raw/crw/inep_rmd/municipios/',
'/raw/crw/inep_saeb/prova_brasil_2013/',
'/raw/crw/inep_saeb/prova_brasil_2015/',
'/raw/crw/inep_saeb/prova_brasil_2017/',
'/raw/crw/inep_saeb/prova_brasil_2019/',
'/raw/crw/inep_saeb/saeb_aluno_unificado/',
'/raw/crw/inep_saeb/saeb_diretor_unificada/',
'/raw/crw/inep_saeb/saeb_escola_unificada/',
'/raw/crw/inep_saeb/saeb_professor_unificada/',
'/raw/crw/inep_saeb/saeb_resultado_brasil_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_municipio_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_regiao_unificado/',
'/raw/crw/inep_saeb/saeb_resultado_uf_unificado/',
'/raw/crw/inep_saeb/ts_quest_aluno/',
'/raw/crw/inep_saeb/ts_resposta_aluno/',
'/raw/crw/inep_saeb/ts_resultado_aluno/',
'/raw/crw/inep_taxa_rendimento/brasil_regioes_e_ufs/',
'/raw/crw/inep_taxa_rendimento/escolas/',
'/raw/crw/inep_taxa_rendimento/municipios/',
'/raw/crw/inep_tdi/brasil_regioes_e_ufs/',
'/raw/crw/inep_tdi/escolas/',
'/raw/crw/inep_tdi/municipios/',
'/raw/crw/infojobs/vagas/',
'/tmp/dev/raw/crw/inss/cat_copy/',
'/raw/crw/me/caged/',
'/raw/crw/me/caged_ajustes/',
'/raw/crw/me/novo_caged_exc/',
'/raw/crw/me/novo_caged_for/',
'/raw/crw/me/novo_caged_mov/',
'/tmp/dev/raw/crw/mec/pnp_efi/',
'/tmp/dev/raw/crw/mec/pnp_fin/',
'/tmp/dev/raw/crw/mec/pnp_mat/',
'/tmp/dev/raw/crw/mec/pnp_ser/',
'/tmp/dev/raw/crw/ms_sinan/acbi/',
'/tmp/dev/raw/crw/ms_sinan/acgr/',
'/tmp/dev/raw/crw/ms_sinan/canc/',
'/tmp/dev/raw/crw/ms_sinan/derm/',
'/tmp/dev/raw/crw/ms_sinan/lerd/',
'/tmp/dev/raw/crw/ms_sinan/ment/',
'/tmp/dev/raw/crw/ms_sinan/pair/',
'/tmp/dev/raw/crw/ocde/projecoes_economicas/',
'/tmp/dev/raw/crw/vagascertas/vagas/',
'/raw/usr/me/cadastro_cbo/',
'/raw/usr/me/rais_estabelecimento/',
'/raw/usr/me/rais_vinculo/']

dataset = []
for x in lista_de_endereco:
  #print(f'{var_adls_uri}{x}')
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(f'{var_adls_uri}{x}')
  list_schema_names = df.schema.names
  #print(list_schema_names)
  for y in list_schema_names:
    Column = {'Path':f'{x}','Nomes_das_colunas':y}
    dataset.append(Column)

sparkDF2 = spark.createDataFrame(pd.DataFrame(dataset)) 
#Tabela

sparkDF2.coalesce(1).write.mode("overwrite").option('header', True).csv('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/uniepro/data/PRM_COLUNAS')

# COMMAND ----------

          LG = localizacao.longitude
            LA = localizacao.latitude
        except:
            LG = 'None'
            LA = 'None'

        coord = {
            'cep':cep,
            'longitu':LG,
            'latit':LA
            }

    dataset.append(coord)
  
    return pd.DataFrame(dataset)

# COMMAND ----------

df = spark.read.format("parquet").option("header","true").option('sep',';').load(path)

# COMMAND ----------

df.schema.names

# COMMAND ----------

df.repartition(1).write.mode("overwrite").option('header', True).csv(path)


df.write.format("json").mode("overwrite).save(outputPath/file.json)

# COMMAND ----------

# /tmp/dev/raw/crw/banco_mundial/indicadores_selecionados/

# COMMAND ----------

df = ['raw/crw/inep_censo_escolar/etapa_ensino/',
 'raw/crw/inep_dsu/brasil_regioes_e_ufs/',
 'raw/crw/inep_saeb/prova_brasil_2017/',
 'raw/crw/inep_atu/municipios/',
 '/tmp/dev/raw/crw/fmi_proj/pais/',
 'raw/crw/inep_tdi/brasil_regioes_e_ufs/',
 'raw/crw/ibge/pnadc/',
 '/tmp/dev/raw/crw/catho/vagas/',
 '/raw/crw/ibge/pintec_rel_coop/',
 'tmp/dev/raw/crw/inep_iqes/cpc/',
 'raw/crw/ibge/pop_projetada_txfecund/',
 'tmp/dev/raw/crw/ms_sinan/ment/',
 '/tmp/dev/raw/usr/ibge/pintec/po/org_mkt_po/',
 '/raw/crw/ibge/pintec_tipo_inov_proj/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/org_mkt_cnae/',
 'raw/crw/me/caged_ajustes/',
 'raw/crw/inep_ideb/ideb/',
 'raw/crw/inep_saeb/prova_brasil_2019/',
 'raw/crw/inep_atu/brasil_regioes_e_ufs/',
 'raw/crw/inep_saeb/saeb_aluno_unificado/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/bio_nano_cnae/',
 'raw/crw/inep_ird/escolas/',
 '/tmp/dev/raw/crw/fmi_proj/grupo_paises/',
 'raw/crw/inep_taxa_rendimento/municipios/',
 '/raw/crw/ibge/deflatores/',
 'raw/crw/inep_enem/enem_escola/',
 'raw/crw/inep_saeb/saeb_escola_unificada/',
 '/tmp/dev/raw/crw/banco_mundial/documentacao_indica/',
 'raw/crw/infojobs/vagas/',
 'raw/crw/inep_saeb/ts_resposta_aluno/',
 'raw/crw/inep_saeb/saeb_resultado_regiao_unificado/',
 '/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
 'raw/crw/inep_prova_brasil/ts_quest_aluno/',
 'tmp/dev/raw/crw/mec/pnp_efi/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_cnae/',
 'raw/crw/inep_censo_escolar/matriculas/',
 'raw/crw/inep_tdi/escolas/',
 '/raw/crw/ibge/pintec_org_mkt/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_programa_cnae/',
 'tmp/dev/raw/crw/ms_sinan/lerd/',
 'tmp/dev/raw/crw/mec/pnp_ser/',
 '/raw/crw/ibge/pintec_resp_imp/',
 'raw/crw/inep_saeb/saeb_resultado_brasil_unificado/',
 '/tmp/dev/raw/crw/aneel/gera_energia_distrib/',
 '/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
 'raw/crw/inep_afd/escolas/',
 '/tmp/dev/raw/usr/ibge/pintec/po/grau_nov_imp_po/',
 'raw/crw/inep_prova_brasil/ts_resultado_aluno/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/',
 'tmp/dev/raw/crw/mec/pnp_mat/',
 'raw/crw/inep_afd/municipios/',
 'raw/crw/inep_tdi/municipios/',
 '/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
 '/raw/crw/ibge/pintec_rel_coop_loc/',
 '/raw/crw/ibge/pintec_grau_nov_imp/',
 'raw/crw/inep_rmd/brasil_regioes_e_ufs/',
 'raw/crw/inep_taxa_rendimento/brasil_regioes_e_ufs/',
 'raw/crw/me/novo_caged_exc/',
 'raw/crw/inep_inse/socioeconomico/',
 'tmp/dev/raw/crw/ocde/projecoes_economicas/',
 '/raw/crw/ibge/pintec_tipo_programa/',
 'raw/crw/inep_saeb/prova_brasil_2015/',
 'raw/crw/inep_censo_escolar/docente/',
 'raw/crw/inep_saeb/saeb_diretor_unificada/',
 '/tmp/dev/raw/crw/bne/vagas/',
 'raw/crw/inep_dsu/escolas/',
 'raw/crw/inep_saeb/saeb_resultado_municipio_unificado/',
 'raw/usr/me/rais_estabelecimento/',
 'tmp/dev/raw/crw/inss/cat_copy/',
 'tmp/dev/raw/crw/vagascertas/vagas/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_inoc_proj_cnae/',
 'raw/crw/inep_atu/escolas/',
 'raw/crw/inep_rmd/municipios/',
 'raw/crw/me/novo_caged_mov/',
 'raw/crw/ibge/relatorio_dtb_brasil_municipio/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/grau_nov_imp_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_loc_cnae/',
 '/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
 '/raw/crw/ibge/pintec_prod_vend_int/',
 'tmp/dev/raw/crw/ms_sinan/derm/',
 'raw/crw/me/novo_caged_for/',
 '/raw/crw/ibge/pintec_bio_nano/',
 '/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
 '/tmp/dev/raw/crw/banco_mundial/documentacao_paises/',
 'tmp/dev/raw/crw/inep_iqes/icg/',
 'tmp/dev/raw/crw/ms_sinan/pair/',
 '/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
 'raw/crw/inep_had/escolas/',
 'tmp/dev/raw/crw/mec/pnp_fin/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/resp_imp_cnae/',
 'raw/crw/inep_saeb/ts_resultado_aluno/',
 'raw/crw/inep_icg/escolas/',
 '/tmp/dev/raw/usr/ibge/pintec/cnae/prod_vend_int_cnae/',
 'raw/crw/inep_saeb/saeb_professor_unificada/',
 'raw/crw/inep_saeb/saeb_resultado_uf_unificado/',
 'raw/crw/inep_taxa_rendimento/escolas/',
 '/raw/crw/ibge/pintec_prob_obst/',
 'raw/crw/inep_enem/microdados_enem/',
 'raw/crw/inep_had/brasil_regioes_e_ufs/',
 '/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
 '/tmp/dev/raw/usr/ibge/pintec/po/bio_nano_po/',
 'raw/crw/inep_saeb/ts_quest_aluno/',
 'raw/crw/me/caged/',
 'raw/crw/inep_censo_escolar/turmas/',
 'raw/usr/me/rais_vinculo/',
 'raw/crw/inep_had/municipios/',
 'raw/crw/inep_saeb/prova_brasil_2013/',
 'raw/crw/inep_dsu/municipios/',
 'raw/crw/inep_prova_brasil/ts_resposta_aluno/',
 'raw/crw/inep_enem/enem_escola_dicionario/',
 'raw/crw/inep_censo_escolar/escola/',
 '/tmp/dev/raw/crw/banco_mundial/indicadores_selecionados/',
 'raw/crw/inep_censo_escolar/gestor/',
 'raw/crw/inep_censo_escolar/curso_educacao_profissional/',
 'tmp/dev/raw/crw/ms_sinan/acgr/',
 'tmp/dev/raw/crw/ms_sinan/acbi/',
 'raw/usr/me/cadastro_cbo/',
 'raw/crw/inep_afd/brasil_regioes_e_ufs/',
 'tmp/dev/raw/crw/ms_sinan/canc/']

lista_path = [f'/{x}' if x[0] != '/' else x for x in df]
for a in lista_path[0:119]:
  var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
  path = f'{var_adls_uri}{a}'
  df = spark.read.format("parquet").option("header","true").option('sep',';').load(path)
  print(f'{a[-25:]}')
  #df.write.format("parquet").saveAsTable(f'{a[-25:]}')

# COMMAND ----------

CREATE TABLE rais (
CD_MUNICIPIO BIGINT, 
CD_CNAE10_CLASSE BIGINT, 
FL_VINCULO_ATIVO_3112 BIGINT, 
CD_TIPO_VINCULO BIGINT, 
CD_MOTIVO_DESLIGAMENTO BIGINT, 
CD_MES_DESLIGAMENTO BIGINT, 
FL_IND_VINCULO_ALVARA BIGINT, 
CD_TIPO_ADMISSAO BIGINT, 
CD_TIPO_SALARIO BIGINT, 
CD_CBO94 FLOAT(53), 
CD_GRAU_INSTRUCAO BIGINT, 
CD_SEXO BIGINT, 
CD_NACIONALIDADE BIGINT, 
CD_RACA_COR BIGINT, 
FL_IND_PORTADOR_DEFIC BIGINT, 
CD_TAMANHO_ESTABELECIMENTO BIGINT, 
CD_NATUREZA_JURIDICA BIGINT, 
FL_IND_CEI_VINCULADO BIGINT, 
CD_TIPO_ESTAB BIGINT, 
FL_IND_ESTAB_PARTICIPA_PAT BIGINT, 
FL_IND_SIMPLES BIGINT, 
DT_DIA_MES_ANO_DATA_ADMISSAO BIGINT, 
VL_REMUN_MEDIA_SM FLOAT(53), 
VL_REMUN_MEDIA_NOM FLOAT(53), 
VL_REMUN_DEZEMBRO_SM FLOAT(53), 
VL_REMUN_DEZEMBRO_NOM FLOAT(53), 
NR_MES_TEMPO_EMPREGO FLOAT(53), 
QT_HORA_CONTRAT BIGINT, 
VL_REMUN_ULTIMA_ANO_NOM FLOAT(53), 
VL_REMUN_CONTRATUAL_NOM FLOAT(53), 
ID_PIS BIGINT, 
DT_DIA_MES_ANO_DATA_NASCIMENTO FLOAT(53), 
ID_CTPS BIGINT, 
ID_CPF BIGINT, 
ID_CEI_VINCULADO BIGINT, 
ID_CNPJ_CEI BIGINT, 
ID_CNPJ_RAIZ BIGINT, 
CD_TIPO_ESTAB_ID FLOAT(53), 
ID_NOME_TRABALHADOR TEXT, 
DT_DIA_MES_ANO_DIA_DESLIGAMENTO FLOAT(53), 
CD_CBO BIGINT, 
CD_CNAE20_CLASSE BIGINT, 
CD_CNAE20_SUBCLASSE BIGINT, 
CD_TIPO_DEFIC BIGINT, 
CD_CAUSA_AFASTAMENTO1 BIGINT, 
NR_DIA_INI_AF1 BIGINT, 
NR_MES_INI_AF1 BIGINT, 
NR_DIA_FIM_AF1 BIGINT, 
NR_MES_FIM_AF1 BIGINT, 
CD_CAUSA_AFASTAMENTO2 BIGINT, 
NR_DIA_INI_AF2 BIGINT, 
NR_MES_INI_AF2 BIGINT, 
NR_DIA_FIM_AF2 BIGINT, 
NR_MES_FIM_AF2 BIGINT, 
CD_CAUSA_AFASTAMENTO3 BIGINT, 
NR_DIA_INI_AF3 BIGINT, 
NR_MES_INI_AF3 BIGINT, 
NR_DIA_FIM_AF3 BIGINT, 
NR_MES_FIM_AF3 BIGINT, 
VL_DIAS_AFASTAMENTO` BIGINT, 
VL_IDADE BIGINT, 
CD_IBGE_SUBSETOR FLOAT(53), 
VL_ANO_CHEGADA_BRASIL FLOAT(53), 
ID_CEPAO_ESTAB FLOAT(53), 
CD_MUNICIPIO_TRAB FLOAT(53), 
ID_RAZAO_SOCIAL FLOAT(53), 
VL_REMUN_JANEIRO_NOM FLOAT(53), 
VL_REMUN_FEVEREIRO_NOM FLOAT(53), 
VL_REMUN_MARCO_NOM FLOAT(53), 
VL_REMUN_ABRIL_NOM FLOAT(53), 
VL_REMUN_MAIO_NOM FLOAT(53), 
VL_REMUN_JUNHO_NOM FLOAT(53), 
VL_REMUN_JULHO_NOM FLOAT(53), 
VL_REMUN_AGOSTO_NOM FLOAT(53), 
VL_REMUN_SETEMBRO_NOM FLOAT(53), 
VL_REMUN_OUTUBRO_NOM FLOAT(53), 
VL_REMUN_NOVEMBRO_NOM FLOAT(53), 
FL_IND_TRAB_INTERMITENTE FLOAT(53), 
FL_IND_TRAB_PARCIAL FLOAT(53), 
FL_SINDICAL FLOAT(53), 
VL_ANO_CHEGADA_BRASIL2 FLOAT(53), 
CD_CNAE20_DIVISAO BIGINT, 
CD_CBO4 BIGINT, 
CD_BAIRROS_FORTALEZA FLOAT(53), 
CD_BAIRROS_RJ FLOAT(53), 
CD_BAIRROS_SP FLOAT(53), 
CD_DISTRITOS_SP FLOAT(53), 
CD_FAIXA_ETARIA FLOAT(53), 
CD_FAIXA_HORA_CONTRAT FLOAT(53), 
CD_FAIXA_REMUN_DEZEM_SM FLOAT(53), 
CD_FAIXA_REMUN_MEDIA_SM FLOAT(53), 
CD_FAIXA_TEMPO_EMPREGO FLOAT(53), 
CD_REGIOES_ADM_DF FLOAT(53), 
DT_MES_ADMISSAO FLOAT(53), 
dh_insercao_trs TEXT, 
kv_process_control TEXT, 
ANO BIGINT, 
CD_UF BIGINT
) ]

# COMMAND ----------

/tmp/