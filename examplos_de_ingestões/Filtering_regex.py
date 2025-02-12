# Databricks notebook source
# Amostra 1% dos dados
df_sample = df.sample(withReplacement=False, fraction=0.0001)

df.sample(fraction=0.1).display()

# COMMAND ----------

df.sample(fraction=0.1).display()

# COMMAND ----------

import re

# Filter columns where the name contains 'CD_CNAE20' using regex
columns = [col for col in df.columns if re.search('CNAE', col)]

columns.display()

# COMMAND ----------

#import re

# Filter columns where the name contains 'CD_CNAE20' using regex
columns = [col for col in df.columns if re.search('JURIDICA', col)]

columns

# COMMAND ----------

df = df.withColumn('CD_NAT_JURIDICA', f.col('CD_NATUREZA_JURIDICA_RAIS'))\
  .withColumn('DS_NAT_JURIDICA', f.col('DS_NAT_JURIDICA_RAIS'))



# Databricks notebook source
# Amostra 1% dos dados
df_sample = df.sample(withReplacement=False, fraction=0.0001)

df.sample(fraction=0.1).display()

# COMMAND ----------

df.sample(fraction=0.1).display()

# COMMAND ----------

import re

# Filter columns where the name contains 'CD_CNAE20' using regex
columns = [col for col in df.columns if re.search('CNAE', col)]

columns.display()

# COMMAND ----------

#import re

# Filter columns where the name contains 'CD_CNAE20' using regex
columns = [col for col in df.columns if re.search('JURIDICA', col)]

columns

# COMMAND ----------

df = df.withColumn('CD_NAT_JURIDICA', f.col('CD_NATUREZA_JURIDICA_RAIS'))\
  .withColumn('DS_NAT_JURIDICA', f.col('DS_NAT_JURIDICA_RAIS'))





table = {"copy_sqldw":"false",
         "cadastro_estbl_f":"rfb_cnpj/cadastro_estbl_f",
         "cadastro_empresa_f":"rfb_cnpj/cadastro_empresa_f",
         "cadastro_simples_f":"rfb_cnpj/cadastro_simples_f",
         "rais_estab":"me/rais_estabelecimento",
         "rais_vinc":"me/rais_vinculo","trs_ipca_path":"ibge/ipca","uds_siafi_path":"oni/governo_federal/orcamento_da_uniao/rfb/siafi","exportadoras":"oni/mdic/comex/emp_bras_exp_imp/exportadoras","importadoras":"oni/mdic/comex/emp_bras_exp_imp/importadoras","class_inten_tec":"oni/ocde/ativ_econ/int_tec/class_inten_tec","cnaes_contrib":"oni/observatorio_nacional/cnae/ger_arrec_fin/cnaes_contrib",
         "cnae_ncm":"oni/ibge/ativ_econ/class_produto/cnae_ncm",
         "cnae_isic":"oni/ibge/ativ_econ/class_internacionais/cnae_isic",
         "nat_juridica":"rfb_cnpj/tabelas_auxiliares_f/nat_juridica/",
         "rfb_cnpj_municipio":"rfb_cnpj/tabelas_auxiliares_f/municipio",
         "rfb_cnpj_motivo":"rfb_cnpj/tabelas_auxiliares_f/motivo",
         "descricao_cnae":"ibge/cnae_subclasses",
"path_destination":"oni/base_unica_cnpjs/cnpjs_rfb_rais",
"destination":"/oni/base_unica_cnpjs/cnpjs_rfb_rais",
"databricks":{"notebook":"/biz/oni/base_unica_cnpjs/trs_biz_base_unificada_cnpjs"}}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

