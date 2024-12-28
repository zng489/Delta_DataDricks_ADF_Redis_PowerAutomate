import os
import json
import re
# Path to the folder

file_path = r'C:\Users\PC\Desktop\mapeamento_adf\all_pipelines.json'
with open(file_path, 'r') as file:
    data = json.load(file)
    #print(data[0])
    word = 'org'
    layer = 'org_raw'

    result_list = []
    for each_one in data:
        #print(each_one['name'])   
        #word = 'org'
        layer = 'lnd_org'
        if each_one['name'].startswith(layer):
            #each_one
            #print(each_one['name'])
            name_pipeline = each_one.get('name', {})
            folder = each_one.get('properties', {}).get('folder', {}).get('name', {})
            bot_script = each_one.get('properties', {}).get('parameters', {}).get('bot', {}).get('defaultValue', {})
            print(name_pipeline)
            print(folder)

            dados = {
                'NAME_PIPELINE': name_pipeline,
                'FOLDER': folder,
                'BOT_SCRIPT_NAME': bot_script}