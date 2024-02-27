# Databricks notebook source
CD_CNAE20_SUBCLASSE_SECUNDARIA = df.filter(df.CD_CNAE20_SUBCLASSE_SECUNDARIALength > 4000)
CD_CNAE20_SUBCLASSE_SECUNDARIA.display()

# COMMAND ----------

NCM = df.filter(df.NCMLength > 4000).select('NCM','NCMLength')
NCM.display()

# COMMAND ----------


DESCRICAO_NCM_2012 = df.filter(df.DESCRICAO_NCM_2012Length > 4000).select('DESCRICAO_NCM_2012','DESCRICAO_NCM_2012Length')

DESCRICAO_NCM_2012.display()

# COMMAND ----------

CD_ISIC40 = df.filter(col("CD_ISIC40Length") > 4000 )

CD_ISIC40.display()

# COMMAND ----------

DS_ISIC40 = df.filter(col("DS_ISIC40Length") > 4000 )
DS_ISIC40.display()

# COMMAND ----------

