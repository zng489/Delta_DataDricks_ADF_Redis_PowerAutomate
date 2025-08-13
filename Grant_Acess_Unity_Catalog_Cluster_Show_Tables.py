# Databricks notebook source
spark.sql("""
ALTER TABLE oni_dev.ambiente_teste.tabela_de_teste
SET TAGS (
    'classification' = 'confidential',
    'owner' = 'finance-team'
)
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get catalog, schema and table

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLEs IN datalake__trs.oni;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


spark = SparkSession.builder.getOrCreate()

# Define catálogo e schema
catalog = "datalake__trs"
schema = "oni"

# Usar o catálogo e schema corretamente
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Obter tabelas no schema
tables_df = spark.sql("SHOW TABLES IN oni")

# Construir nomes completos no formato desejado
full_table_names = tables_df.withColumn(
    "full_table_name",
    expr(f"'{catalog}.{schema}.' || tableName")
).select("full_table_name")

# Exibir resultado
full_table_names.display()

# COMMAND ----------

table_names = [row['full_table_name'] for row in full_table_names.collect()]
print(table_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get cluster settings

# COMMAND ----------

import requests
import pandas as pd

# Seu workspace
workspace_url = "https://adb-6523536500265509.9.azuredatabricks.net"
# Seu token de acesso pessoal (ideal usar dbutils.secrets.get())
token = ""

# Endpoint da API para listar clusters
url = f"{workspace_url}/api/2.0/clusters/list"

headers = {
    "Authorization": f"Bearer {token}"
}

# Faz a requisição
response = requests.get(url, headers=headers)

if response.status_code == 200:
    clusters = response.json().get("clusters", [])
    dados = []
    for c in clusters:
        dados.append({
            "Nome": c.get("cluster_name"),
            "Status": c.get("state"),
            "Criado por": c.get("creator_user_name"),
            "Tipo de Nó": c.get("node_type_id"),
            "Última Atividade": c.get("last_activity_time")
        })

    # Criar DataFrame
    df_clusters = pd.DataFrame(dados)
    display(df_clusters)
else:
    print(f"Erro {response.status_code}: {response.text}")



# COMMAND ----------

import requests
import os

# URL do workspace e token (pega do Databricks → User Settings → Access Tokens)
workspace_url = "https://<seu-workspace>.databricks.com"
token = dbutils.secrets.get("scope_name", "token_key")  # ou direto no código, mas não é recomendado

# Endpoint da API para listar clusters
url = f"{workspace_url}/api/2.0/clusters/list"

headers = {
    "Authorization": f"Bearer {token}"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    clusters = response.json().get("clusters", [])
    for c in clusters:
        print(f"Nome: {c['cluster_name']} | Status: {c['state']} | Criado em: {c['creator_user_name']}")
else:
    print(f"Erro {response.status_code}: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grants

# COMMAND ----------

# Concessão de acesso em ativos de dados via script Python
from unity_grants import unity_grants
 
# Lista de ativos para os quais o acesso será concedido e revogado
ativos = ["datalake__raw_usr.oni.oni_observatorio_nacional_oferta_demanda_senai__relacao_cursos_cbo","datalake__trs.oni.oni_observatorio_nacional_oferta_demanda_senai__relacao_cursos_cbo","datalake__biz.oni.oni_bases_referencia__relacao_cursos_cbo"]
# Lista de usuários para os quais o acesso será concedido e revogado
users = [
    "zhang.yuan@senaicni.com.br",
    "vitor.rebello@sesicni.com.br",
    "alaine.santos@senaicni.com.br"
    #"t-franciele.padoan@cni.com.br"
]
 
# Loop através de cada ativo
for ativo in ativos:
    print(ativo)
    # Loop através de cada usuário
    for user in users:
        print(user)
        # Concede acesso ao ativo para o usuário
        unity_grants.grant_access(ativo, user)
        # REVOGA acesso ao ativo para o usuário
        # unity_grants.revoke_access(ativo, user)

# COMMAND ----------

# Concessão de acesso em ativos de dados via script Python
from unity_grants import unity_grants
 
# Lista de ativos para os quais o acesso será concedido e revogado
ativos = ["datalake__raw_usr.oni.oni_observatorio_nacional_oferta_demanda_senai__relacao_cursos_cbo"]
# Lista de usuários para os quais o acesso será concedido e revogado
users = [
    "zhang.yuan@senaicni.com.br"
    #"t-franciele.padoan@cni.com.br"
]
 
# Loop através de cada ativo
for ativo in ativos:
    print(ativo)
    # Loop através de cada usuário
    for user in users:
        print(user)
        # Concede acesso ao ativo para o usuário
        unity_grants.grant_access(ativo, user)
        # REVOGA acesso ao ativo para o usuário
        # unity_grants.revoke_access(ativo, user)