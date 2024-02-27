# Databricks notebook source
import pandas as pd
import numpy as np
import asyncio
import random
import time

try:
  import aiohttp
except:
  dbutils.library.installPyPI("aiohttp")
  import aiohttp
  
  
  
async def fetch(url):
  
  try:
    async with aiohttp.ClientSession() as session:
      async with session.get(url) as response:
        get_session = response.status
        #print(get_session)
        #return get_session
    
  except Exception as e:
    get_session = str(e)
    #return get_session
    print('!!!!!! WARNING !!!!')
    print(str(e))
    
  return get_session

  #return print(get_session)
  #await save_product(get_session)
  
  
      
async def coroutine_task_01(paths):
  #product_info = {row.th.text: row.td.text for row in rows}
    tasks = []
    for iteraction in paths:
      tasks.append(asyncio.create_task(fetch(iteraction)))
      results = await asyncio.gather(*tasks)
    return results

def parse(results):
  dados = []
  for result in results:
    diction = {'Response_Status':result}
    dados.append(diction)
    
  df = pd.DataFrame(dados)
  sparkDF=spark.createDataFrame(df) 
  return display(sparkDF)

    
if __name__ == '__main__':
  paths = ['http://200.152.38.155/CNO/', 'http://www.google.com']
  #asyncio.run(coroutine_task_01(paths))
  
  
  #asyncio.run(coroutine_task_01(paths))
  parse(asyncio.run(coroutine_task_01(paths)))

# COMMAND ----------

#######################

# COMMAND ----------

