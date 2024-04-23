# Databricks notebook source
dbutils.widgets.text("env","dev") #tmp ou prod
env = dbutils.widgets.get("env")
env_read = 'prod' #tmp ou prod
momento = 'oficial' #desenv ou oficial
env, env_read

# COMMAND ----------

# MAGIC %run ./__libs

# COMMAND ----------

groups = spark.read.format("parquet").load(path_pbi + '/'.join(['', 'azadata032_aad_groups']))
users = spark.read.format("parquet").load(path_pbi + '/'.join(['', 'azadata032_aad_users']))
users_groups = spark.read.format("parquet").load(path_pbi + '/'.join(['', 'azadata032_aad_users_groups']))

# COMMAND ----------

acl = spark.read.format("delta").load(path_acl)

# COMMAND ----------

# trata o dataframe de usuário e grupo, unindo os dois e criando uma listagem única de usuários/grupos do AAD
#ug_lista = groups.filter('excluded = 0').select(col('group_id').alias('id'), col('name'), lit('-').alias('login'), lit('g').alias('tip')) \
#                 .union(users.filter('excluded = 0').select(col('user_id').alias('id'), col('name'), col('user_principal_name').alias('login'), lit('u').alias('tip')))

ug_lista = groups.select(col('group_id').alias('id'), col('name'), lit('-').alias('login'), lit('g').alias('tip')) \
                 .union(users.select(col('user_id').alias('id'), col('name'), col('user_principal_name').alias('login'), lit('u').alias('tip')))

# cria usuário e grupo nível 1
ug = users_groups.select(col('group_id').alias('id'),col('user_id').alias('id_relac'))
ug = ug.union(users.select(col('user_id').alias('id'), lit('-').alias('id_relac')))

# cria usuário e grupo nível 2
df_ug = ug.alias('a')\
.join(ug.alias('b'), col('a.id_relac')==col('b.id'), 'left')\
.select( col('a.id').alias('n1_id'), col('b.id').alias('n2_id'), col('b.id_relac') )

# cria usuário e grupo nível 3
df_ug = df_ug.alias('a')\
.join(ug.alias('b'), col('a.id_relac')==col('b.id'), 'left')\
.select( col('a.n1_id'), col('a.n2_id'), col('b.id').alias('n3_id'), col('b.id_relac') )

# cria usuário e grupo nível 4
df_ug = df_ug.alias('a')\
.join(ug.alias('b'), col('a.id_relac')==col('b.id'), 'left')\
.select( col('a.n1_id'), col('a.n2_id'), col('a.n3_id'), col('b.id').alias('n4_id'), col('b.id_relac') )

# cria usuário e grupo nível 5 e nível 6
df_ug = df_ug.alias('a')\
.join(ug.alias('b'), col('a.id_relac')==col('b.id'), 'left')\
.select( col('a.n1_id'), col('a.n2_id'), col('a.n3_id'), col('a.n4_id'), col('b.id').alias('n5_id'), col('b.id_relac').alias('n6_id') )

# COMMAND ----------

# cria o dataframe de acl reçacionada aos grupos e usuários em níveis de hierarquia de acesso
# n1=acesso direto ao path, n2=acesso ao path via grupo n1, n3=acesso ao path via grupo n2 e consequentemente n1, n4...n5...n6...
df_acl = acl.alias('acl')\
.join(df_ug.alias('ug'), col('acl.id')==col('ug.n1_id'), 'left')\
.join(ug_lista.alias('l1'), col('n1_id')==col('l1.id'), 'left')\
.join(ug_lista.alias('l2'), col('n2_id')==col('l2.id'), 'left')\
.join(ug_lista.alias('l3'), col('n3_id')==col('l3.id'), 'left')\
.join(ug_lista.alias('l4'), col('n4_id')==col('l4.id'), 'left')\
.join(ug_lista.alias('l5'), col('n5_id')==col('l5.id'), 'left')\
.join(ug_lista.alias('l6'), col('n6_id')==col('l6.id'), 'left')\
.select(
  col('acl.path'), col('acl.id'), col('acl.ler'), col('acl.gravar'), col('acl.executar'), col('acl.created_at'),
  col('ug.n1_id'), col('ug.n2_id'), col('ug.n3_id'), col('ug.n4_id'), col('ug.n5_id'), col('ug.n6_id'),
  col('l1.tip').alias('n1_tip'), col('l1.name').alias('n1_name'), col('l1.login').alias('n1_login'),
  col('l2.tip').alias('n2_tip'), col('l2.name').alias('n2_name'), col('l2.login').alias('n2_login'),
  col('l3.tip').alias('n3_tip'), col('l3.name').alias('n3_name'), col('l3.login').alias('n3_login'),
  col('l4.tip').alias('n4_tip'), col('l4.name').alias('n4_name'), col('l4.login').alias('n4_login'),
  col('l5.tip').alias('n5_tip'), col('l5.name').alias('n5_name'), col('l5.login').alias('n5_login'),
  col('l6.tip').alias('n6_tip'), col('l6.name').alias('n6_name'), col('l6.login').alias('n6_login'),
)

# COMMAND ----------

df_acl_tmp = df_acl\
.select(
  col('*'),
  concat(expr("case when ler='1' then 'r' else '-' end")
        ,expr("case when gravar='1' then 'w' else '-' end")
        ,expr("case when executar='1' then 'x' else '-' end")).alias('permissao')
)

df_acl_check = df_acl_tmp\
.select( col('path'), col('permissao'), col('created_at').alias('data'), col('id').alias('id_acl'), col('n1_id').alias('id'),
         lit('1').alias('nivel_acl'), col('n1_tip').alias('tipo'), col('n1_name').alias('nome'), col('n1_login').alias('login'),
         concat(lit(' - ')).alias('hierarquia'))\
.union( df_acl_tmp\
.select( col('path'), col('permissao'), col('created_at').alias('data'), col('id').alias('id_acl'), col('n2_id').alias('id'),
         lit('2').alias('nivel_acl'), col('n2_tip').alias('tipo'), col('n2_name').alias('nome'), col('n2_login').alias('login'),
         concat(col('n1_name')).alias('hierarquia')))\
.union( df_acl_tmp\
.select( col('path'), col('permissao'), col('created_at').alias('data'), col('id').alias('id_acl'), col('n3_id').alias('id'),
         lit('3').alias('nivel_acl'), col('n3_tip').alias('tipo'), col('n3_name').alias('nome'), col('n3_login').alias('login'),
         concat(col('n2_name'),lit(' >> '),col('n1_name')).alias('hierarquia')))\
.union( df_acl_tmp\
.select( col('path'), col('permissao'), col('created_at').alias('data'), col('id').alias('id_acl'), col('n4_id').alias('id'),
         lit('4').alias('nivel_acl'), col('n4_tip').alias('tipo'), col('n4_name').alias('nome'), col('n4_login').alias('login'),
         concat(col('n3_name'),lit(' >> '),col('n2_name'),lit(' >> '),col('n1_name')).alias('hierarquia')))\
.union( df_acl_tmp\
.select( col('path'), col('permissao'), col('created_at').alias('data'), col('id').alias('id_acl'), col('n5_id').alias('id'),
         lit('5').alias('nivel_acl'), col('n5_tip').alias('tipo'), col('n5_name').alias('nome'), col('n5_login').alias('login'),
         concat(col('n4_name'),lit(' >> '),col('n3_name'),lit(' >> '),col('n2_name'),lit(' >> '),col('n1_name')).alias('hierarquia')))\
.union( df_acl_tmp\
.select( col('path'), col('permissao'), col('created_at').alias('data'), col('id').alias('id_acl'), col('n6_id').alias('id'),
         lit('6').alias('nivel_acl'), col('n6_tip').alias('tipo'), col('n6_name').alias('nome'), col('n6_login').alias('login'),
         concat(col('n5_name'),lit(' >> '),col('n4_name'),lit(' >> '),col('n3_name'),lit(' >> '),col('n2_name'),lit(' >> '),col('n1_name')).alias('hierarquia')))\

df_acl_check = df_acl_check.distinct()

# COMMAND ----------

df_acl_check = df_acl_check\
.withColumn("pasta_1", get_nivel_df(col("path"),lit(1)))\
.withColumn("pasta_2", get_nivel_df(col("path"),lit(2)))\
.withColumn("pasta_3", get_nivel_df(col("path"),lit(3)))\
.withColumn("pasta_4", get_nivel_df(col("path"),lit(4)))\
.withColumn("pasta_5", get_nivel_df(col("path"),lit(5)))\
.withColumn("pasta_6", get_nivel_df(col("path"),lit(6)))\
.withColumn("pasta_7", get_nivel_df(col("path"),lit(7)))\
.withColumn("pasta_8", get_nivel_df(col("path"),lit(8)))\
.withColumn("pasta_9", get_nivel_df(col("path"),lit(9)))

# COMMAND ----------

df_acl_check.write.format("parquet").mode("overwrite").save(path_adls_report + '/'.join(['','parquet']))
