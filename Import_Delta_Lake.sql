-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pyspark.sql.functions as f
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC #def lista():
-- MAGIC var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
-- MAGIC path = f'{var_adls_uri}/uds/uniepro/Planilha_de_levantamento_de_dados/'
-- MAGIC df = spark.read.format("csv").option("header","true").option('sep',';').load(path).select('Path Temp').dropDuplicates().rdd.flatMap(lambda x: x).collect()
-- MAGIC df = ['raw/crw/inep_censo_escolar/etapa_ensino/',
-- MAGIC  'raw/crw/inep_dsu/brasil_regioes_e_ufs/',
-- MAGIC  'raw/crw/inep_saeb/prova_brasil_2017/',
-- MAGIC  'raw/crw/inep_atu/municipios/',
-- MAGIC  '/tmp/dev/raw/crw/fmi_proj/pais/',
-- MAGIC  'raw/crw/inep_tdi/brasil_regioes_e_ufs/',
-- MAGIC  'raw/crw/ibge/pnadc/',
-- MAGIC  '/tmp/dev/raw/crw/catho/vagas/',
-- MAGIC  '/raw/crw/ibge/pintec_rel_coop/',
-- MAGIC  'tmp/dev/raw/crw/inep_iqes/cpc/',
-- MAGIC  'raw/crw/ibge/pop_projetada_txfecund/',
-- MAGIC  'tmp/dev/raw/crw/ms_sinan/ment/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/org_mkt_po/',
-- MAGIC  '/raw/crw/ibge/pintec_tipo_inov_proj/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/org_mkt_cnae/',
-- MAGIC  'raw/crw/me/caged_ajustes/',
-- MAGIC  'raw/crw/inep_ideb/ideb/',
-- MAGIC  'raw/crw/inep_saeb/prova_brasil_2019/',
-- MAGIC  'raw/crw/inep_atu/brasil_regioes_e_ufs/',
-- MAGIC  'raw/crw/inep_saeb/saeb_aluno_unificado/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/bio_nano_cnae/',
-- MAGIC  'raw/crw/inep_ird/escolas/',
-- MAGIC  '/tmp/dev/raw/crw/fmi_proj/grupo_paises/',
-- MAGIC  'raw/crw/inep_taxa_rendimento/municipios/',
-- MAGIC  '/raw/crw/ibge/deflatores/',
-- MAGIC  'raw/crw/inep_enem/enem_escola/',
-- MAGIC  'raw/crw/inep_saeb/saeb_escola_unificada/',
-- MAGIC  '/tmp/dev/raw/crw/banco_mundial/documentacao_indica/',
-- MAGIC  'raw/crw/infojobs/vagas/',
-- MAGIC  'raw/crw/inep_saeb/ts_resposta_aluno/',
-- MAGIC  'raw/crw/inep_saeb/saeb_resultado_regiao_unificado/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/tipo_inoc_proj_po/',
-- MAGIC  'raw/crw/inep_prova_brasil/ts_quest_aluno/',
-- MAGIC  'tmp/dev/raw/crw/mec/pnp_efi/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_cnae/',
-- MAGIC  'raw/crw/inep_censo_escolar/matriculas/',
-- MAGIC  'raw/crw/inep_tdi/escolas/',
-- MAGIC  '/raw/crw/ibge/pintec_org_mkt/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_programa_cnae/',
-- MAGIC  'tmp/dev/raw/crw/ms_sinan/lerd/',
-- MAGIC  'tmp/dev/raw/crw/mec/pnp_ser/',
-- MAGIC  '/raw/crw/ibge/pintec_resp_imp/',
-- MAGIC  'raw/crw/inep_saeb/saeb_resultado_brasil_unificado/',
-- MAGIC  '/tmp/dev/raw/crw/aneel/gera_energia_distrib/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/prob_obst_po/',
-- MAGIC  'raw/crw/inep_afd/escolas/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/grau_nov_imp_po/',
-- MAGIC  'raw/crw/inep_prova_brasil/ts_resultado_aluno/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/prob_obst_cnae/',
-- MAGIC  'tmp/dev/raw/crw/mec/pnp_mat/',
-- MAGIC  'raw/crw/inep_afd/municipios/',
-- MAGIC  'raw/crw/inep_tdi/municipios/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_po/',
-- MAGIC  '/raw/crw/ibge/pintec_rel_coop_loc/',
-- MAGIC  '/raw/crw/ibge/pintec_grau_nov_imp/',
-- MAGIC  'raw/crw/inep_rmd/brasil_regioes_e_ufs/',
-- MAGIC  'raw/crw/inep_taxa_rendimento/brasil_regioes_e_ufs/',
-- MAGIC  'raw/crw/me/novo_caged_exc/',
-- MAGIC  'raw/crw/inep_inse/socioeconomico/',
-- MAGIC  'tmp/dev/raw/crw/ocde/projecoes_economicas/',
-- MAGIC  '/raw/crw/ibge/pintec_tipo_programa/',
-- MAGIC  'raw/crw/inep_saeb/prova_brasil_2015/',
-- MAGIC  'raw/crw/inep_censo_escolar/docente/',
-- MAGIC  'raw/crw/inep_saeb/saeb_diretor_unificada/',
-- MAGIC  '/tmp/dev/raw/crw/bne/vagas/',
-- MAGIC  'raw/crw/inep_dsu/escolas/',
-- MAGIC  'raw/crw/inep_saeb/saeb_resultado_municipio_unificado/',
-- MAGIC  'raw/usr/me/rais_estabelecimento/',
-- MAGIC  'tmp/dev/raw/crw/inss/cat_copy/',
-- MAGIC  'tmp/dev/raw/crw/vagascertas/vagas/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/tipo_inoc_proj_cnae/',
-- MAGIC  'raw/crw/inep_atu/escolas/',
-- MAGIC  'raw/crw/inep_rmd/municipios/',
-- MAGIC  'raw/crw/me/novo_caged_mov/',
-- MAGIC  'raw/crw/ibge/relatorio_dtb_brasil_municipio/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/grau_nov_imp_cnae/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/rel_coop_loc_cnae/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/prod_vend_int_po/',
-- MAGIC  '/raw/crw/ibge/pintec_prod_vend_int/',
-- MAGIC  'tmp/dev/raw/crw/ms_sinan/derm/',
-- MAGIC  'raw/crw/me/novo_caged_for/',
-- MAGIC  '/raw/crw/ibge/pintec_bio_nano/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/rel_coop_loc_po/',
-- MAGIC  '/tmp/dev/raw/crw/banco_mundial/documentacao_paises/',
-- MAGIC  'tmp/dev/raw/crw/inep_iqes/icg/',
-- MAGIC  'tmp/dev/raw/crw/ms_sinan/pair/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/resp_imp_po/',
-- MAGIC  'raw/crw/inep_had/escolas/',
-- MAGIC  'tmp/dev/raw/crw/mec/pnp_fin/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/resp_imp_cnae/',
-- MAGIC  'raw/crw/inep_saeb/ts_resultado_aluno/',
-- MAGIC  'raw/crw/inep_icg/escolas/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/cnae/prod_vend_int_cnae/',
-- MAGIC  'raw/crw/inep_saeb/saeb_professor_unificada/',
-- MAGIC  'raw/crw/inep_saeb/saeb_resultado_uf_unificado/',
-- MAGIC  'raw/crw/inep_taxa_rendimento/escolas/',
-- MAGIC  '/raw/crw/ibge/pintec_prob_obst/',
-- MAGIC  'raw/crw/inep_enem/microdados_enem/',
-- MAGIC  'raw/crw/inep_had/brasil_regioes_e_ufs/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/tipo_programa_po/',
-- MAGIC  '/tmp/dev/raw/usr/ibge/pintec/po/bio_nano_po/',
-- MAGIC  'raw/crw/inep_saeb/ts_quest_aluno/',
-- MAGIC  'raw/crw/me/caged/',
-- MAGIC  'raw/crw/inep_censo_escolar/turmas/',
-- MAGIC  'raw/usr/me/rais_vinculo/',
-- MAGIC  'raw/crw/inep_had/municipios/',
-- MAGIC  'raw/crw/inep_saeb/prova_brasil_2013/',
-- MAGIC  'raw/crw/inep_dsu/municipios/',
-- MAGIC  'raw/crw/inep_prova_brasil/ts_resposta_aluno/',
-- MAGIC  'raw/crw/inep_enem/enem_escola_dicionario/',
-- MAGIC  'raw/crw/inep_censo_escolar/escola/',
-- MAGIC  '/tmp/dev/raw/crw/banco_mundial/indicadores_selecionados/',
-- MAGIC  'raw/crw/inep_censo_escolar/gestor/',
-- MAGIC  'raw/crw/inep_censo_escolar/curso_educacao_profissional/',
-- MAGIC  'tmp/dev/raw/crw/ms_sinan/acgr/',
-- MAGIC  'tmp/dev/raw/crw/ms_sinan/acbi/',
-- MAGIC  'raw/usr/me/cadastro_cbo/',
-- MAGIC  'raw/crw/inep_afd/brasil_regioes_e_ufs/',
-- MAGIC  'tmp/dev/raw/crw/ms_sinan/canc/']
-- MAGIC
-- MAGIC lista_path = [f'/{x}' if x[0] != '/' else x for x in df]
-- MAGIC for a in lista_path[0:119]:
-- MAGIC   var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
-- MAGIC   path = f'{var_adls_uri}{a}'
-- MAGIC   df = spark.read.format("parquet").option("header","true").option('sep',';').load(path)
-- MAGIC   print(f'{a[-25:]}')
-- MAGIC   #df.write.format("parquet").saveAsTable(f'{a[-25:]}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for a in lista_path:
-- MAGIC   var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
-- MAGIC   path = f'{var_adls_uri}{a}'
-- MAGIC   print(f'{a[-25:]}')
-- MAGIC   #print(path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC p = 'Python'
-- MAGIC p[-1:-2]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC len(lista_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #--armazenamento no dw do databricks de tabela do azure storage
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pyspark.sql.functions as f
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC
-- MAGIC for a in lista_path[0:119]:
-- MAGIC   var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
-- MAGIC   path = f'{var_adls_uri}{a}'
-- MAGIC   df = spark.read.format("parquet").option("header","true").option('sep',';').load(path)
-- MAGIC   df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2020t4")
-- MAGIC   print(path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #--armazenamento no dw do databricks de tabela do azure storage
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pyspark.sql.functions as f
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # Fase de chamada do arquivo ----------------------------------------------------------------------
-- MAGIC var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'
-- MAGIC path = f'{var_adls_uri}{lista_path[0]}'
-- MAGIC path
-- MAGIC
-- MAGIC df = spark.read.format("parquet").option("header","true").option('sep',';').load(path)
-- MAGIC
-- MAGIC df.write.format("parquet").saveAsTable("Inep_censo_escolar")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2018/TRIMESTRE=2018.3/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_catconsolidada")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2018/TRIMESTRE=2018.4/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2018t4")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2019/TRIMESTRE=2019.1/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2019t1")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2019/TRIMESTRE=2019.2/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2019t2")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2019/TRIMESTRE=2019.3/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2019t3")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2019/TRIMESTRE=2019.4/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2019t4")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2020/TRIMESTRE=2020.1/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2020t1")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2020/TRIMESTRE=2020.2/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2020t2")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2020/TRIMESTRE=2020.3/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2020t3")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2020/TRIMESTRE=2020.4/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2020t4")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2021/TRIMESTRE=2021.1/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2021t1")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2021/TRIMESTRE=2021.2/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2021t2")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/inss/cat_copy/ANO=2018/TRIMESTRE=2021.3/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)

df.write.format("parquet").saveAsTable("oni_homolog.saude_cat2021t3")

-- COMMAND ----------

--armazenamento no dw do databricks de tabela do azure storage

%python
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd

# Fase de chamada do arquivo ----------------------------------------------------------------------

path_origin = '/tmp/dev/raw/crw/fmi_proj/grupo_paises/' # <-----Para realizar consulta, alterar aqui endereço

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=path_origin)
df = spark.read.parquet(adl_sink)



df.write.format("parquet").saveAsTable("oni_homolog.indicadores_selecionados")

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2018t3

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2018t4

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2019t1

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2019t2

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2019t3

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2019t4

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2020t1

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2020t2

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2020t3

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2020t4

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2021t1

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2021t2

-- COMMAND ----------

--merge das bases trimestrais na base oni_homolog.saude_catconsolidada

insert into oni_homolog.saude_catconsolidada
select * from oni_homolog.saude_cat2021t3

-- COMMAND ----------

--Contagem de linhas das bases e conferência com a base consolidada

select  count(nm_arq_in) as Total_linhas_por_base, 
        '2018t3' as base 
from oni_homolog.saude_cat2018t3

union all

select  count(nm_arq_in), 
        '2018t4'
from oni_homolog.saude_cat2018t4

union all

select  count(nm_arq_in), 
        '2019t1'
from oni_homolog.saude_cat2019t1

union all

select  count(nm_arq_in), 
        '2019t2'
from oni_homolog.saude_cat2019t2

union all

select  count(nm_arq_in), 
        '2019t3'
from oni_homolog.saude_cat2019t3

union all

select  count(nm_arq_in), 
        '2019t4'
from oni_homolog.saude_cat2019t4

union all

select  count(nm_arq_in), 
        '2020t1'
from oni_homolog.saude_cat2020t1

union all

select  count(nm_arq_in), 
        '2020t2'
from oni_homolog.saude_cat2020t2

union all

select  count(nm_arq_in), 
        '2020t3'
from oni_homolog.saude_cat2020t3

union all

select  count(nm_arq_in), 
        '2020t4'
from oni_homolog.saude_cat2020t4

union all

select  count(nm_arq_in), 
        '2021t1'
from oni_homolog.saude_cat2021t1

union all

select  count(nm_arq_in), 
        '2021t2'
from oni_homolog.saude_cat2021t2

union all

select  count(nm_arq_in), 
        '2021t3'
from oni_homolog.saude_cat2021t3

union all

select  count(nm_arq_in), 
        'saude_catconsolidada'
from oni_homolog.saude_catconsolidada

-- COMMAND ----------

select count(nm_arq_in) from oni_homolog.saude_catconsolidada