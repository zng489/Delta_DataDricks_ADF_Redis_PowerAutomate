# Databricks notebook source
try:
  import geopandas as gpd
except:
  %pip install geopandas
  import geopandas as gpd

# COMMAND ----------

import re

path = "/Volumes/oni_lab/default/uds_oni_observatorio_nacional/oni/mapa_georreferenciamento_industria/"

arquivos = {}

for base in dbutils.fs.ls(path):
    base_name = base.name.rstrip("/")  # pega o nome da pasta, ex: 'base_aero'
    for file in dbutils.fs.ls(base.path):
        if re.search(r"\.shp$", file.name, re.IGNORECASE):
            arquivos[base_name] = file.path.replace("dbfs:/", "")  # adiciona ao dicionário

# verificar resultado
print(arquivos)

dfs_dict = {}
# Loop para ler cada arquivo e adicionar ao dicionário
for nome, caminho in arquivos.items():
    #dfs_dict[nome] = spark.read.option("header", True).csv(caminho)
    dfs_dict[nome] = gpd.read_file(f'/{caminho}')

# COMMAND ----------

dfs_dict['base_ferro']

# COMMAND ----------

# TRANSFORMAÇÃO GEOJSON
import geopandas as gpd
import os

layers = {
    "ferrovias": {
        "original_name": "ferroviario",
        "file": "BaseFerro/BaseFerro.shp",
        "relevant_vars": ["nome", "bitola", "tip_situac", "geometry"],
    },
    "rodovias": {
        "original_name": "rodoviario",
        "file": "SNV_202507A.shp",
        "relevant_vars": [
            "vl_br",
            "nm_tipo_tr",
            "ds_tipo_ad",
            "ds_legenda",
            "geometry",
        ],
    },
    "aeroportos": {
        "original_name": "aeroportos",
        "file": "BaseAero/BaseAero.shp",
        "relevant_vars": [
            "nome",
            "situação",
            "superfíci",
            "compriment",
            "largura1",
            "altitude",
            "passageiro",
            "concedidos",
            "empresa",
            "município",
            "geometry",
        ],
    },
    "portos": {
        "original_name": "portuario",
        "file": "BaseHidroPortos/BaseHidroPortos.shp",
        "relevant_vars": [
            "nome",
            "companhia",
            "tipo",
            "gestao",
            "loc_izacao",
            "geometry",
        ],
    },
    "cabotagem": {
        "original_name": "cabotagem",
        "file": "cabotagem.geojson",
        "relevant_vars": ["geometry"],
    },
    "hidrovias": {
        "original_name": "hidroviario",
        "file": "fc_hidro_hidrovia_antaq.shp",
        "relevant_vars": [
            "nome",
            "tipo",
            "cla_icacao",
            "nome_rio",
            "geometry",
        ],
    },
}

user = os.getenv("ONEDRIVE_LOCATION")
path = os.path.join(
    user,
    r"CNI - Confederação Nacional da Indústria\Observatório [Equipe Interna] - Observatorio\Produtos_Projetos\2025_Mapa_Georreferenciamento_Industrias\ETL\Logística de transportes",
)

for layer in layers.keys():
    file = os.path.join(
        path, "2024", layers[layer]["original_name"], layers[layer]["file"]
    )
    gdf = gpd.read_file(file)
    gdf = gdf[layers[layer]["relevant_vars"]]
    if layer == "ferrovias":
        gdf = gdf.loc[gdf["tip_situac"] == "Em Operação"]
    if layer == "aeroportos":
        gdf = gdf.loc[gdf["situação"] != "Interditado"]
    if layer in ["ferrovias", "rodovias", "hidrovias"]:
        gdf["geometry"] = gdf["geometry"].simplify(0.001)
    gdf.to_file(os.path.join(path, f"{layer}.geojson"), driver="GeoJSON")