# Databricks notebook source
# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE CATALOG if not exists lab_oni;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG lab_oni;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE CATALOG if not exists lab_oni.default.vw__pintec_bio_nano;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG lab_oni.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema extended information_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lab_oni.default.biz__oni__monitor_de_vagas__dim_cbo_familia

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG lab_oni;
# MAGIC USE SCHEMA default;
# MAGIC SELECT * FROm vw__pintec_bio_nano;

# COMMAND ----------

datalake__biz.oni.biz__oni__base_unica_cnpjs__cnpjs_rfb_rais_dw