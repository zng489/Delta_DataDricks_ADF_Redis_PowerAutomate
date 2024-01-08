# Databricks notebook source
import json
import re
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame

# COMMAND ----------

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

source_trs_ipca = "{uri}/trs/ibge/ipca/".format(uri=var_adls_uri)
source_cadastro_cbo = "{uri}/trs/me/cadastro_cbo/".format(uri=var_adls_uri)
source_cnae_subclasses = "{uri}/biz/oni/bases_referencia/cnae/cnae_20/cnae_subclasse/".format(uri=var_adls_uri)
source_contribuintes = "{uri}/trs/oni/bases_do_projeto/painel_dos_estados/contribuintes/".format(uri=var_adls_uri)
source_caged_antigo = "{uri}/trs/me/caged".format(uri=var_adls_uri)
source_uf = "{uri}/trs/oni/ibge/geo_uf/".format(uri=var_adls_uri)
source_novo_caged = "{uri}/trs/me/novo_caged/".format(uri=var_adls_uri)
source_instrucao = "{uri}/trs/oni/bases_do_projeto/painel_dos_estados/grau_instrucao_rais/".format(uri=var_adls_uri)

# COMMAND ----------

def rename_column(df: DataFrame, old_col: str, new_col: str) -> DataFrame:
  return df.withColumnRenamed(old_col, new_col)
  
def add_month_number(df: DataFrame, col_name:str) -> DataFrame:
  df = df.withColumn(col_name, 
          f.when(f.col(col_name)==f.lit('janeiro'), f.lit(1)).otherwise(
          f.when(f.col(col_name)==f.lit('fevereiro'), f.lit(2)).otherwise(
          f.when(f.col(col_name)==f.lit('março'), f.lit(3)).otherwise(
          f.when(f.col(col_name)==f.lit('abril'), f.lit(4)).otherwise(
          f.when(f.col(col_name)==f.lit('maio'), f.lit(5)).otherwise(
          f.when(f.col(col_name)==f.lit('junho'), f.lit(6)).otherwise(
          f.when(f.col(col_name)==f.lit('julho'), f.lit(7)).otherwise(
          f.when(f.col(col_name)==f.lit('agosto'), f.lit(8)).otherwise(
          f.when(f.col(col_name)==f.lit('setembro'), f.lit(9)).otherwise(
          f.when(f.col(col_name)==f.lit('outubro'), f.lit(10)).otherwise(
          f.when(f.col(col_name)==f.lit('novembro'), f.lit(11)).otherwise(
          f.when(f.col(col_name)==f.lit('dezembro'), f.lit(12))))))))))))))
  return df

# COMMAND ----------

window = Window.partitionBy()

df_ipca = (spark.read.parquet(source_trs_ipca))

df_ipca = rename_column(df_ipca, 'NR_MES_COMPETENCIA', 'MES_COMPETENCIA')
df_ipca = rename_column(df_ipca, 'NR_ANO', 'ANO')
df_ipca = df_ipca.orderBy("ANO","MES_COMPETENCIA").withColumn('base_fixa',f.last(f.col('VL_IPCA')).over(window))
df_ipca = df_ipca.withColumn('INDICE_FIXO_ULTIMO_ANO',f.col('VL_IPCA')/f.col('base_fixa'))
df_ipca = df_ipca.select('ANO',"MES_COMPETENCIA",'INDICE_FIXO_ULTIMO_ANO')

# COMMAND ----------

df_grau_instrucao = spark.read.parquet(source_instrucao)

# COMMAND ----------

df_cbo = (spark.read.parquet(source_cadastro_cbo))

# COMMAND ----------

df_cbo = df_cbo.select("cd_cbo1","ds_cbo1","cd_cbo2","ds_cbo2","cd_cbo4","ds_cbo4","ds_tipo_familia","cd_ocup_corporativa_industriais")
df_cbo = df_cbo.dropDuplicates()

# COMMAND ----------

df_cnae_subclasses = (spark.read.parquet(source_cnae_subclasses))
df_cnae= df_cnae_subclasses
df_cnae = df_cnae.select("cd_cnae_divisao", "nm_cnae_divisao").distinct()

# COMMAND ----------

df_contribuintes = (spark.read.parquet(source_contribuintes))
contribuintes = [data[0] for data in df_contribuintes.select('contribuintes').collect()]

# COMMAND ----------

df_caged_antigo = spark.read.parquet(source_caged_antigo)
df_novo_caged = spark.read.parquet(source_novo_caged)

# COMMAND ----------

df_caged_antigo = rename_column(df_caged_antigo,'CD_ANO_MOVIMENTACAO','ANO')
df_caged_antigo = df_caged_antigo.withColumn('MES_COMPETENCIA',f.substring(f.col('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO'),5,2).cast('integer'))\
                                 .withColumn('CD_CNAE20_SUBCLASSE',f.lpad(f.col('CD_CNAE20_SUBCLASSE'),7,'0'))\
                                 .withColumn('CD_CNAE_DIVISAO',f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))
df_caged_antigo = df_caged_antigo.join(df_ipca,['ANO','MES_COMPETENCIA'],'left')
df_caged_antigo = df_caged_antigo.withColumn('VL_SALARIO_MENSAL_REAL', f.col('VL_SALARIO_MENSAL')/f.col('INDICE_FIXO_ULTIMO_ANO'))
df_caged_antigo = df_caged_antigo.withColumn('VL_SALARIO_MENSAL_REAL', \
                                   f.when(((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000))),f.lit(0))\
                                    .otherwise(f.col('VL_SALARIO_MENSAL_REAL')))
df_caged_antigo = df_caged_antigo.withColumn("cd_cbo4",f.substring("CD_CBO",1,4)).drop("CD_CBO")
df_caged_antigo = rename_column(df_caged_antigo,'CD_CBO','cd_cbo4')
df_caged_antigo = df_caged_antigo.join(df_cbo,'cd_cbo4','left')
df_caged_antigo = df_caged_antigo.withColumnRenamed('CD_GRAU_INSTRUCAO','cd_grau_instrucao').join(df_grau_instrucao,'cd_grau_instrucao','left')
df_caged_antigo = (df_caged_antigo
                   .withColumn("cd_cbo_agrupamento", f.when((f.col('cd_cbo1')==f.lit(3)) | ((f.col('cd_cbo1')>f.lit(3)) \
                   & (f.col('cd_cbo1')<f.lit(10)) & (f.col('ds_tipo_familia')==f.lit('Técnicas'))), f.lit(1)) \
                     .otherwise(f.when((f.col('cd_cbo1')>f.lit(3)) & (f.col('cd_cbo1')<f.lit(10)), f.lit(2)) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(2), f.lit(3)) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(0), f.lit(4)) \
                     .otherwise(f.when(f.col('cd_cbo2')==f.lit(11), f.lit(5)) \
                     .otherwise(f.when((f.col('cd_cbo2')>f.lit(11)) & (f.col('cd_cbo2')<f.lit(15)), f.lit(6)) \
                     .otherwise(f.lit(7))))))))
                   .withColumn("ds_cbo_agrupamento", f.when((f.col('cd_cbo1')==f.lit(3)) | ((f.col('cd_cbo1')>f.lit(3)) \
                   & (f.col('cd_cbo1')<f.lit(10)) & (f.col('ds_tipo_familia')==f.lit('Técnicas'))), f.lit('Técnicos de nível médio')) \
                     .otherwise(f.when((f.col('cd_cbo1')>f.lit(3)) & (f.col('cd_cbo1')<f.lit(10)), f.lit('Trabalhadores auxiliares e operacionais')) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(2), f.lit('Especialistas e analistas')) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(0), f.lit('Forças armadas, policias e bombeiros militares')) \
                     .otherwise(f.when(f.col('cd_cbo2')==f.lit(11), f.lit('Membros superiores e dirigentes do setor público')) \
                     .otherwise(f.when((f.col('cd_cbo2')>f.lit(11)) & (f.col('cd_cbo2')<f.lit(15)), f.lit('Diretores e gerentes')) \
                     .otherwise(f.lit('Não informado')))))))))
df_caged_antigo = (df_caged_antigo
                   .withColumn('cd_grau_instrucao_agregado', f.when(f.col('cd_grau_instrucao_agregado').isNull(), f.lit(-1)).otherwise(f.col('cd_grau_instrucao_agregado')))
                   .withColumn("cd_faixa_etaria",f.when(((f.col("VL_IDADE")<=f.lit(20))),f.lit(1))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(20)) & (f.col("VL_IDADE")<=f.lit(30))),f.lit(2))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(30)) & (f.col("VL_IDADE")<=f.lit(40))),f.lit(3))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(40)) & (f.col("VL_IDADE")<=f.lit(50))),f.lit(4))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(50)) & (f.col("VL_IDADE")<=f.lit(60))),f.lit(5))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(60)) & (f.col("VL_IDADE")<=f.lit(70))),f.lit(6))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(70)) & (f.col("VL_IDADE")<=f.lit(80))),f.lit(7))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(80))),f.lit(8)).otherwise(f.lit(9))))))))\
                                        ))
                    .withColumn("ds_faixa_etaria",f.when(((f.col("VL_IDADE")<=f.lit(20))),f.lit('Até 20 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(20)) & (f.col("VL_IDADE")<=f.lit(30))),f.lit('21 a 30 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(30)) & (f.col("VL_IDADE")<=f.lit(40))),f.lit('31 a 40 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(40)) & (f.col("VL_IDADE")<=f.lit(50))),f.lit('41 a 50 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(50)) & (f.col("VL_IDADE")<=f.lit(60))),f.lit('51 a 60 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(60)) & (f.col("VL_IDADE")<=f.lit(70))),f.lit('61 a 70 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(70)) & (f.col("VL_IDADE")<=f.lit(80))),f.lit('71 a 80 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(80))),f.lit('Maiores de 80 anos')).otherwise('Não informado')))))))\
                                        )))                     
df_caged_antigo = df_caged_antigo.drop('INDICE_FIXO_ULTIMO_ANO')

df_caged_antigo = df_caged_antigo.withColumn('ds_sexo',f.when(f.col("CD_SEXO")==f.lit(1),"Masculino")\
                                  .otherwise(f.when(f.col("CD_SEXO")==f.lit(2),"Feminino")))

# COMMAND ----------

df_novo_caged = rename_column(df_novo_caged,'CD_ANO_COMPETENCIA','ANO')
df_novo_caged = df_novo_caged.withColumn('MES_COMPETENCIA',f.substring(f.col('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO'),5,2).cast('integer'))\
                             .withColumn('CD_CNAE20_SUBCLASSE',f.lpad(f.col('CD_CNAE20_SUBCLASSE'),7,'0'))\
                             .withColumn('CD_CNAE_DIVISAO',f.substring(f.col('CD_CNAE20_SUBCLASSE'),1,2))
df_novo_caged = df_novo_caged.join(df_ipca,['ANO','MES_COMPETENCIA'],'left') 
df_novo_caged = df_novo_caged.withColumn('VL_SALARIO_MENSAL_REAL', f.col('VL_SALARIO_MENSAL')/f.col('INDICE_FIXO_ULTIMO_ANO'))
df_novo_caged = df_novo_caged.withColumn('VL_SALARIO_MENSAL_REAL', \
                                   f.when(((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000))),f.lit(0))\
                                    .otherwise(f.col('VL_SALARIO_MENSAL_REAL')))
df_novo_caged=  df_novo_caged.withColumn("cd_cbo4",f.substring("CD_CBO",1,4)).drop("CD_CBO")
df_novo_caged = df_novo_caged.withColumnRenamed('CD_GRAU_INSTRUCAO','cd_grau_instrucao').join(df_grau_instrucao,'cd_grau_instrucao','left')
df_novo_caged = df_novo_caged.join(df_cbo,'cd_cbo4','left')
df_novo_caged = (df_novo_caged.withColumn("cd_cbo_agrupamento", f.when((f.col('cd_cbo1')==f.lit(3)) | ((f.col('cd_cbo1')>f.lit(3)) \
                   & (f.col('cd_cbo1')<f.lit(10)) & (f.col('ds_tipo_familia')==f.lit('Técnicas'))), f.lit(1)) \
                     .otherwise(f.when((f.col('cd_cbo1')>f.lit(3)) & (f.col('cd_cbo1')<f.lit(10)), f.lit(2)) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(2), f.lit(3)) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(0), f.lit(4)) \
                     .otherwise(f.when(f.col('cd_cbo2')==f.lit(11), f.lit(5)) \
                     .otherwise(f.when((f.col('cd_cbo2')>f.lit(11)) & (f.col('cd_cbo2')<f.lit(15)), f.lit(6)) \
                     .otherwise(f.lit(7)))))))).withColumn("ds_cbo_agrupamento", f.when((f.col('cd_cbo1')==f.lit(3)) | ((f.col('cd_cbo1')>f.lit(3)) \
                   & (f.col('cd_cbo1')<f.lit(10)) & (f.col('ds_tipo_familia')==f.lit('Técnicas'))), f.lit('Técnicos de nível médio')) \
                     .otherwise(f.when((f.col('cd_cbo1')>f.lit(3)) & (f.col('cd_cbo1')<f.lit(10)), f.lit('Trabalhadores auxiliares e operacionais')) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(2), f.lit('Especialistas e analistas')) \
                     .otherwise(f.when(f.col('cd_cbo1')==f.lit(0), f.lit('Forças armadas, policias e bombeiros militares')) \
                     .otherwise(f.when(f.col('cd_cbo2')==f.lit(11), f.lit('Membros superiores e dirigentes do setor público')) \
                     .otherwise(f.when((f.col('cd_cbo2')>f.lit(11)) & (f.col('cd_cbo2')<f.lit(15)), f.lit('Diretores e gerentes')) \
                     .otherwise(f.lit('Não informado')))))))))

df_novo_caged = (df_novo_caged
                 .withColumn('cd_grau_instrucao_agregado', f.when(f.col('cd_grau_instrucao_agregado').isNull(), f.lit(-1)).otherwise(f.col('cd_grau_instrucao_agregado')))
                 .withColumn("cd_faixa_etaria",f.when(((f.col("VL_IDADE")<=f.lit(20))),f.lit(1))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(20)) & (f.col("VL_IDADE")<=f.lit(30))),f.lit(2))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(30)) & (f.col("VL_IDADE")<=f.lit(40))),f.lit(3))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(40)) & (f.col("VL_IDADE")<=f.lit(50))),f.lit(4))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(50)) & (f.col("VL_IDADE")<=f.lit(60))),f.lit(5))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(60)) & (f.col("VL_IDADE")<=f.lit(70))),f.lit(6))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(70)) & (f.col("VL_IDADE")<=f.lit(80))),f.lit(7))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(80))),f.lit(8)).otherwise(f.lit(9))))))))\
                                        ))\
                 .withColumn("ds_faixa_etaria",f.when(((f.col("VL_IDADE")<=f.lit(20))),f.lit('Até 20 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(20)) & (f.col("VL_IDADE")<=f.lit(30))),f.lit('21 a 30 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(30)) & (f.col("VL_IDADE")<=f.lit(40))),f.lit('31 a 40 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(40)) & (f.col("VL_IDADE")<=f.lit(50))),f.lit('41 a 50 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(50)) & (f.col("VL_IDADE")<=f.lit(60))),f.lit('51 a 60 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(60)) & (f.col("VL_IDADE")<=f.lit(70))),f.lit('61 a 70 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(70)) & (f.col("VL_IDADE")<=f.lit(80))),f.lit('71 a 80 anos'))\
                                        .otherwise(f.when(((f.col("VL_IDADE")>f.lit(80))),f.lit('Maiores de 80 anos')).otherwise('Não informado')))))))\
                                        )))                     
df_novo_caged = df_novo_caged.drop('INDICE_FIXO_ULTIMO_ANO')

df_novo_caged = df_novo_caged.withColumn("CD_SEXO", f.when(df_novo_caged.CD_SEXO == 3, 2).otherwise(df_novo_caged.CD_SEXO))

df_novo_caged = df_novo_caged.withColumn('ds_sexo',f.when(f.col("CD_SEXO")==f.lit(1),"Masculino")\
                                  .otherwise(f.when(f.col("CD_SEXO")==f.lit(2),"Feminino")))

# COMMAND ----------

df_caged_antigo_saldo = (df_caged_antigo.withColumn("Admitidos", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.lit(1))\
                                          .otherwise(f.lit(0)))\
                                            .withColumn("Admitidos_ajuste", \
                                         f.when((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000)),f.lit(0))\
                                          .otherwise(f.when(f.col('CD_SALDO_MOV')==f.lit(1), f.col('CD_SALDO_MOV')).otherwise(f.lit(0))))\
                                       .withColumn("Sal_Admitidos", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('VL_SALARIO_MENSAL_REAL'))\
                                          .otherwise(f.lit(0)))\
                                       .withColumn("Desligados", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.lit(1))\
                                          .otherwise(f.lit(0)))\
                                            .withColumn("Horas_admitidos", \
                                         f.when((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000)),f.lit(0))\
                                          .otherwise(f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('QT_HORA_CONTRAT')*f.lit(4)).otherwise(f.lit(0))))\
                                       .withColumn('CD_CNAE_DIVISAO',f.lpad(f.col('CD_CNAE_DIVISAO'),2,'0'))
                                       .withColumnRenamed("CD_SEXO","cd_sexo")
                                      # .fillna('Não informado')
                                       .groupBy('CD_UF','ANO','CD_CNAE_DIVISAO','cd_faixa_etaria','ds_faixa_etaria','cd_cbo_agrupamento','ds_cbo_agrupamento',"cd_grau_instrucao_agregado",'nm_grau_instrucao_agregado','ds_sexo','cd_sexo')
                                             .agg(f.sum('CD_SALDO_MOV').alias('vl_saldo_caged'),\
                                               f.sum('Admitidos').alias('vl_admitidos'),\
                                                 f.sum('Admitidos_ajuste').alias('vl_admitidos_ajuste'),\
                                               f.sum('Desligados').alias('vl_desligados'),\
                                               f.sum('Horas_admitidos').alias('vl_massa_horas_adm'),\
                                               f.sum('Sal_Admitidos').alias('vl_massa_salarial_adm')))
                                                 

# saldo - CAGED novo - UF
df_caged_novo_saldo =(df_novo_caged.withColumn('CD_SALDO_MOV',\
                                     f.when(f.col('ORIGEM')=="CAGEDEXC",f.col('CD_SALDO_MOV')*f.lit(-1))\
                                      .otherwise(f.col('CD_SALDO_MOV')))\
                                   .withColumn("Admitidos", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.lit(1))\
                                          .otherwise(f.lit(0)))\
                                        .withColumn("Admitidos_ajuste", \
                                         f.when((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000)),f.lit(0))\
                                          .otherwise(f.when(f.col('CD_SALDO_MOV')==f.lit(1), f.col('CD_SALDO_MOV')).otherwise(f.lit(0))))\
                                       .withColumn("Sal_Admitidos", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('VL_SALARIO_MENSAL_REAL'))\
                                          .otherwise(f.lit(0)))\
                                       .withColumn("Desligados", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.lit(1))\
                                          .otherwise(f.lit(0)))\
                                       .withColumn("Horas_admitidos", \
                                         f.when((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000)),f.lit(0))\
                                          .otherwise(f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('QT_HORA_CONTRAT')*f.lit(4)).otherwise(f.lit(0))))\
                                       .withColumn('CD_CNAE_DIVISAO',f.lpad(f.col('CD_CNAE_DIVISAO'),2,'0'))
                                       .withColumnRenamed("CD_SEXO","cd_sexo")
                                      # .fillna('Não informado')
                                       .groupBy('CD_UF','ANO','CD_CNAE_DIVISAO','cd_faixa_etaria','ds_faixa_etaria','cd_cbo_agrupamento','ds_cbo_agrupamento',"cd_grau_instrucao_agregado",'nm_grau_instrucao_agregado','ds_sexo','cd_sexo')
                                             .agg(f.sum('CD_SALDO_MOV').alias('vl_saldo_caged'),\
                                               f.sum('Admitidos').alias('vl_admitidos'),\
                                                 f.sum('Admitidos_ajuste').alias('vl_admitidos_ajuste'),\
                                               f.sum('Desligados').alias('vl_desligados'),\
                                               f.sum('Horas_admitidos').alias('vl_massa_horas_adm'),\
                                               f.sum('Sal_Admitidos').alias('vl_massa_salarial_adm')
                                                 ))
                                             

#unifica todas as bases

df_caged = df_caged_antigo_saldo.union(df_caged_novo_saldo)
df_caged = rename_column(df_caged,'ANO','ano')
df_caged = rename_column(df_caged,'CD_UF','cd_uf')
df_caged = rename_column(df_caged,'CD_CNAE_DIVISAO','cd_cnae_divisao')
df_caged = df_caged.join(df_cnae,'cd_cnae_divisao','left')

# COMMAND ----------

df_caged = (df_caged.withColumn("vl_saldo_caged",f.col("vl_saldo_caged").cast("double"))\
                   .withColumn("vl_admitidos",f.col("vl_admitidos").cast("double"))\
                   .withColumn("vl_desligados",f.col("vl_desligados").cast("double"))\
                     .withColumn("vl_admitidos_ajuste",f.col("vl_admitidos_ajuste").cast("double"))\
                     .withColumn("vl_massa_horas_adm",f.col("vl_massa_horas_adm").cast("double"))\
                       .withColumn("vl_massa_salarial_adm",f.col("vl_massa_salarial_adm").cast("double")).drop('nm_cnae_divisao').fillna('Não informado')
                       .where(f.col('ano')>f.lit(2011)))

# COMMAND ----------

display(df_caged)

# COMMAND ----------

display(df_novo_caged
        .withColumn('CD_SALDO_MOV',\
                                     f.when(f.col('ORIGEM')=="CAGEDEXC",f.col('CD_SALDO_MOV')*f.lit(-1))\
                                      .otherwise(f.col('CD_SALDO_MOV')))\
                                   .withColumn("Admitidos", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.lit(1))\
                                          .otherwise(f.lit(0)))\
                                        .withColumn("Admitidos_ajuste", \
                                         f.when((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000)),f.lit(0))\
                                          .otherwise(f.when(f.col('CD_SALDO_MOV')==f.lit(1), f.col('CD_SALDO_MOV'))))\
                                       .withColumn("Sal_Admitidos", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('VL_SALARIO_MENSAL_REAL'))\
                                          .otherwise(f.lit(0)))\
                                       .withColumn("Desligados", \
                                         f.when(f.col('CD_SALDO_MOV')==f.lit(-1),f.lit(1))\
                                          .otherwise(f.lit(0)))\
                                       .withColumn("Horas_admitidos", \
                                         f.when((f.col('VL_SALARIO_MENSAL_REAL')<f.lit(300)) | (f.col('VL_SALARIO_MENSAL_REAL')>f.lit(30000)),f.lit(0))\
                                          .otherwise(f.when(f.col('CD_SALDO_MOV')==f.lit(1),f.col('QT_HORA_CONTRAT')*f.lit(4))))\
                                            .where(f.col('Sal_Admitidos').isNull()).select('VL_SALARIO_MENSAL').distinct())

# COMMAND ----------

display(df_caged.where(f.col('vl_massa_salarial_adm').isNull()).select('ano').distinct())

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count
df_caged.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_caged.columns]
   ).display()

# COMMAND ----------

  df_caged.select('ano').distinct().orderBy(f.asc('ano')).show()

# COMMAND ----------

#df_caged.write.parquet(var_adls_uri + '/uds/oni/observatorio_nacional/mulheres_industria_fnme/caged', mode="overwrite")
