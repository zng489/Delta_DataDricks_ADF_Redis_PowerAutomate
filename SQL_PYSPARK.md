###########
# To 2022 #
###########

df.createOrReplaceTempView('df_2022')

# Perform the SQL query to filter and drop columns
df_2022 = spark.sql(""" 
    SELECT * FROM df_2022 WHERE ANO == '2022'
""").drop('nm_arq_in', 'nr_reg', 'dh_insercao_raw', 'dh_arq_in')


########### 
# To 2021 #
###########

### SQL df.createOrReplaceTempView('df_2021')
### SQL df_2021 = spark.sql("""
### SQL    SELECT UF_DESC, CLASSES_DO_CNAE_2_0_DESC, UF, CLASSES_DO_CNAE_2_0 
### SQL    FROM (
### SQL        SELECT *, UF, 
### SQL               CLASSES_DO_CNAE_2_0, 
### SQL               split(UF, ':')[1] AS UF_DESC, 
### SQL               split(CLASSES_DO_CNAE_2_0, ':')[1] AS CLASSES_DO_CNAE_2_0_DESC
### SQL        FROM df_2021
### SQL        WHERE ANO != '2022'
### SQL    ) WHERE ANO = '2021'
### SQL """)


### SQL df_2022.createOrReplaceTempView("df_2022_view")
### SQL df_2021.createOrReplaceTempView("df_2021_view")
# Perform SQL join operation
### SQL df_joined = spark.sql("""
### SQL SELECT 
### SQL     DF_2021.UF,
### SQL     DF_2021.CLASSES_DO_CNAE_2_0,
### SQL     `1:ASSISTENCIA_MEDICA`,
### SQL     `2:INCAPACIDADE_MENOS_DE_15_DIAS`,
### SQL     `3:INCAPACIDADE_MAIS_DE_15_DIAS`,
### SQL     `4:INCAPACIDADE_PERMANENTE`,
### SQL     `5:OBITOS`,
### SQL     `TOTAL`,
### SQL     `9:SEM_CAT`,
### SQL     `ANO` 
### SQL FROM df_2022_view AS DF_2022 
### SQL LEFT JOIN df_2021_view AS DF_2021 
### SQL ON DF_2022.CLASSES_DO_CNAE_2_0 = DF_2021.CLASSES_DO_CNAE_2_0_DESC
### SQL """)

df_joined.createOrReplaceTempView("df_joined_view")
#df_joined.display()

#df_old = df.filter(df['ANO'] != '2022').drop('nm_arq_in','nr_reg','dh_insercao_raw', 'dh_arq_in')
#df_old = df_old.withColumn('UF_DESC', split(df_old.UF, ':').getItem(1)) \
#           .withColumn('CLASSES_DO_CNAE_2_0_DESC', split(df_old.CLASSES_DO_CNAE_2_0, ':').getItem(1))

# ETL for 2022
#df_2021 = df_old.filter(df_old['ANO'] == '2021').select('UF_DESC', 'CLASSES_DO_CNAE_2_0_DESC', 'UF', 'CLASSES_DO_CNAE_2_0')
#df_2021.display()

#df_old = df.filter(df['ANO'] != '2022').drop('nm_arq_in','nr_reg','dh_insercao_raw', 'dh_arq_in')
#df_old = df_old.withColumn('UF_DESC', split(df_old.UF, ':').getItem(1)) \
#           .withColumn('CLASSES_DO_CNAE_2_0_DESC', split(df_old.CLASSES_DO_CNAE_2_0, ':').getItem(1))

# ETL for 2022
#df_2021 = df_old.filter(df_old['ANO'] == '2021').select('UF_DESC', 'CLASSES_DO_CNAE_2_0_DESC', 'UF', 'CLASSES_DO_CNAE_2_0')
#df_2021.display()

df_joined = df_joined.select(*__transform_columns())
df_joined = df_joined.filter(f.col("DS_CLASSES_DO_CNAE_2_0") != "Total")
df_joined = df_joined.filter(f.col("NM_UF") != "Total")

df_joined = df_joined.withColumn("NM_UF", f.regexp_replace(f.col("NM_UF"), "[:\\d+]", ""))

df_joined = df_joined.withColumn('DS_CLASSES_DO_CNAE_2_0', 
                   f.when(f.col('DS_CLASSES_DO_CNAE_2_0') == '{ñ class}', f.lit('00:Cnae Zerado'))
                     .otherwise(f.col('DS_CLASSES_DO_CNAE_2_0')))

df_joined = df_joined.withColumn('CD_CLASSES_DO_CNAE_2_0', f.split(df_joined['CD_CLASSES_DO_CNAE_2_0'], ':').getItem(0)) \
       .withColumn('DS_CLASSES_DO_CNAE_2_0', f.split(df_joined['DS_CLASSES_DO_CNAE_2_0'], ':').getItem(1))

df_2022 = df.filter(df['ANO'] == '2022')
new_columns = ['nm_arq_in',
 'UF',
 'DIVISAO_DO_CNAE_2_0',
 'ASSISTENCIA_MEDICA',
 'INCAPACIDADE_MENOS_DE_15_DIAS',
 'INCAPACIDADE_MAIS_DE_15_DIAS',
 'INCAPACIDADE_PERMANENTE',
 'OBITOS',
 'TOTAL',
 '9:SEM_CAT',
 'nr_reg',
 'dh_insercao_raw',
 'dh_arq_in',
 '1:ASSISTENCIA_MEDICA',
 '2:INCAPACIDADE_MENOS_DE_15_DIAS',
 '3:INCAPACIDADE_MAIS_DE_15_DIAS',
 '4:INCAPACIDADE_PERMANENTE',
 '5:OBITOS',
 'ANO']

df_2022 = df_2022.toDF(*new_columns)