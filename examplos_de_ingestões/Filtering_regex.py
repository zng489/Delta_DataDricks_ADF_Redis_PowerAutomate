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
