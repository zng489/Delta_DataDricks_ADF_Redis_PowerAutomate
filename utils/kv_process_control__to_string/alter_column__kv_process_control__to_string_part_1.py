# Databricks notebook source
# MAGIC %md
# MAGIC # Alter column kv_process_control to string
# MAGIC 
# MAGIC ## Why?
# MAGIC This is required for the usage with catalog tool Azure purview. Currently \(december 2022\) it cannot deal with complex types (json, for example).
# MAGIC 
# MAGIC 
# MAGIC ------
# MAGIC 
# MAGIC ## How to do it?
# MAGIC This implementation inherits a lot from [bigdatadlsreporter](https://dev.azure.com/CNI-STI/ENSI-BIG%20DATA/_git/ENSI-BIGDATADLSREPORTER).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part 1: list all tables in all layers
# MAGIC Listing includes file format and partitions (when applicable)

# COMMAND ----------

# DBTITLE 1,Imports
import re
import json
from rich.console import Console
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

# COMMAND ----------

# MAGIC %md
# MAGIC Configs

# COMMAND ----------

ADLS = {
    "uri": "https://cnibigdatadlsgen2.dfs.core.windows.net",
    "file_system": "datalake",
    "schema": {
        "ls": ["name", "accessTime", "modificationTime"],
    },
    "exclude": {
        "folders": [
            "ach",
            "log",
            "raw/gov",
        ],
    },
    "levels": {
        "biz": {
            "default": 2,
            "indicadores": 4,
            "oni/monitor_do_emprego_kpis": 4,
            "uniepro/fta_rfb_cno": 3,
        },
        "trs": {
            "default": 2,
            "indicadores": 4,
            "me/comex": 3,
            "mtd/corp": 3,
            "mtd/externo": 3,
            "mtd/monitor_de_vagas": 4,
            "mtd/senai": 3,
            "mtd/sesi": 3,
            "rfb/cno": 3,
        },
        "raw": {
            "usr/fiesc": 5,
            "default": 3,
            "crw/me/comex": 4,
            "crw/rfb/cno": 4,
            "crw/rfb_cnpj/tabelas_auxiliares": 4,
            "usr/uniepro/monitor_de_vagas/uf": 4,
            "usr/uniepro/monitor_de_vagas/dicionarios": 5,
            "usr/uniepro/monitor_de_vagas/padroes_regex": 5,
            "usr/uniepro/rfb": 4,
        },
    },
}

# COMMAND ----------

AZURE = {
    "tenant_id": "6d6bcc3f-bda1-4f54-af1d-86d4b7d4e6b8",
    "authority_host_uri": "https://login.microsoftonline.com",
    "resource_uri": "https://management.core.windows.net/",
    "client_id": "7814beec-ee8f-429a-af11-f618e6ce4ffd",  # cnibigdataapp
    "client_secret": "PUT YOUR SECRET HERE",  # cnibigdataapp
}

# COMMAND ----------

KEY_VAULT = {
    "uri": "https://cnibigdatakeyvault.vault.azure.net/",
    "user_secret": "generalserviceuser",
    "password_secret": "generalserviceuserpassword",
    "datalake_secret": "crawleradlgen2pass",
}

# COMMAND ----------

# DBTITLE 1,List tables
class AdlsGen(object):

    console = Console()

    def __init__(self):
        """
        :param env: 'dev' or 'prod'. This will allow to change the paths in adls.
        """
        self.adls = DataLakeServiceClient(
            account_url=ADLS["uri"],
            credential=self.__get_key_vault_client_credential(
                KEY_VAULT["datalake_secret"]
            ),
        )
        self.adls = self.adls.get_file_system_client(file_system=ADLS["file_system"])

    def __get_key_vault_client_credential(self, secret_name: str):
        credential = ClientSecretCredential(
            tenant_id=AZURE["tenant_id"],
            client_id=AZURE["client_id"],
            client_secret=AZURE["client_secret"],
        )
        client = SecretClient(
            vault_url=KEY_VAULT["uri"],
            credential=credential,
        )
        # azure key vault The policy requires the caller to use on-behalf-of (OBO) flow
        return client.get_secret(secret_name).value

    def list_tables_with_properties(self, layer: str, path: str, tables=[]) -> list:
        self.console.log(f"[yellow]Listing path[/yellow] '{path}' in layer '{layer}'")
        level_list = list(self.adls.get_paths(path, recursive=False))  # Returns dict

        for i in level_list:
            name = i.name
            schema = "default"
            for schema_iter in ADLS["levels"][layer]:
                schema_start_pos = len(layer) + 1
                schema_end_pos = schema_start_pos + len(schema_iter)
                # Out of range in string does not generate any issues
                schema_maybe = name[schema_start_pos:schema_end_pos]
                if schema_maybe == schema_iter:
                    schema = schema_maybe
                    break

            # If counting of '/' is the same, then it's a valid table.
            if len(re.findall("/", name)) == ADLS["levels"][layer][schema]:
                self.console.log(f"[gray]Evaluating table:[/gray] {name}")
                table = {"name": name}
                # If ther
                # e are more levels than the allowed one, this is probably an error in development
                # Example: 'biz/uniepro/fta_rfb_cno/cno_biz'
                table_listing = self.adls.get_paths(name, recursive=True)
                table["format"] = "parquet"
                table["partitions"] = []
                # Get format and partitions
                for j in table_listing:
                    if j.is_directory and "_delta_log" in j.name:
                        table["format"] = "delta"
                    if j.is_directory and "=" in j.name:
                        partitions = j.name.replace(name + "/", "").split(
                            "/"
                        )  # Gets table-level subfolders
                        partitions = [
                            p.split("=")[0] for p in partitions
                        ]  # Gets the names of the partitions
                        if (
                            partitions > table["partitions"]
                        ):  # The most complex partition wins, as expected
                            table["partitions"] = partitions
                if len(table["partitions"]) == 0:
                    table.pop("partitions")
                self.console.log(f"[cyan]Appending table to results:[/cyan] {table}")
                tables.append(table)

            # Else, recurse over
            else:
                self.list_tables_with_properties(layer=layer, path=name, tables=tables)

        return tables


# Test and debug


def list_tables_in_layer(layer: str) -> list:
    adls = AdlsGen()
    tables = adls.list_tables_with_properties(layer=layer, path=layer)
    return tables
    del adls

# COMMAND ----------

def save_json_to_file(data, layer:str) -> None:
  with open(f"/dbfs/temp/alter__kv_process_control__{layer}__tables.json", "w") as f:
    f.write(json.dumps(data, indent=2))

# COMMAND ----------

var_raw_tables = list_tables_in_layer("raw")
save_json_to_file(var_raw_tables, "raw")

# COMMAND ----------

var_trs_tables = list_tables_in_layer("trs")
#save_json_to_file(var_trs_tables, "trs")  

# COMMAND ----------

var_biz_tables = list_tables_in_layer("biz")
save_json_to_file(var_biz_tables, "biz")
