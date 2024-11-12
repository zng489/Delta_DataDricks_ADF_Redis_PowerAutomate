from functools import partial
from core.bot import log_status
from core.redis import call_redis
from core.adls import  upload_file
from core.string_utils import normalize_bool
from core.string_utils import normalize_replace

# COMMAND ----------

# Verifica se a chave existe

key_name = 'VALUE'

exists_key_main = call_redis(dbutils,"exists", key_name)
exists_key_main

# COMMAND ----------

# Getting the value based on the key_name, in this case datatime.today()

key_name = 'VALUE'
last_update_str: str = call_redis(dbutils, "get", key_name).decode()
last_update_str

# COMMAND ----------

# Reseting values

params = {}
params["reset"] = "RESET"

if params["reset"] is True:
  print('Parâmetro `reset` recebido. Removendo chave do redis.')
  call_redis(dbutils, "delete", key_name)
  print('Chave removida com sucesso')

# COMMAND ----------

# Setting key_name

key_name = 'VALUE'
call_redis(dbutils, "set", key_name, str(date.today()))

# COMMAND ----------
