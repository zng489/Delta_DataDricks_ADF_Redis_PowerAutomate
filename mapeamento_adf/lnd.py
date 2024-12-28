import os
import json
import re
# Path to the folder

folder_path = r'C:\Users\PC\Desktop\mapeamento_adf\pipeline\pipeline'

# List all files in the folder
files = os.listdir(folder_path)
# ['biz_biz_analitico_ep.json', 'biz_biz_base_unimercado_ep.json', 'biz_biz_base_unimercado_ssi.json', 'biz_biz_base_unimercado_sti.json', ...

result_list = [] 
for file_name in files:
    # biz_biz_analitico_ep.json
    # biz_biz_base_unimercado_ep.json
    # biz_biz_base_unimercado_ssi.json
    # biz_biz_base_unimercado_sti.json
    # biz_biz_calcula_coluna.json


    file_path = os.path.join(folder_path, file_name)
    # 'C:\Users\PC\Desktop\mapeamento_adf\pipeline\pipeline' + ['biz_biz_analitico_ep.json', 'biz_biz_base_unimercado_ep.json', 'biz_biz_base_unimercado_ssi.json', 'biz_biz_base_unimercado_sti.json', ...
    with open(file_path, 'r') as file:
        data = json.load(file)

        # Now 'data' contains the JSON data as a Python dictionary
        # Convert the JSON object to a text string
        json_string = json.dumps(data, indent=2)
        word = 'lnd'
        layer = 'lnd_org_raw'
        if word in json_string:
            # if re.search(rf'\b{word}\b', json_string, re.IGNORECASE) and not file_name.startswith('wkf') and file_name.startswith(layer):
            if word in json_string and file_name.startswith(layer) and not file_name.startswith('wkf'): 
                #>> it doesn't work, cause the expression 'if word in json_string' ain't looking for the 'word in the specific sequence'
                #print(file_name)
                with open(file_path, 'r') as file:
                    #data = json.load(file)
                    #print(data)
                    name_pipeline = data.get('name', {})
                    folder = data.get('properties', {}).get('folder', {}).get('name', {})
                    bot_script = data.get('properties', {}).get('parameters', {}).get('bot', {}).get('defaultValue', {})
                    #.get('parameters', {}).get('tables', {}).get('defaultValue', {}).get('path_destination')
                    #print(name_pipeline)
                    #print(folder)
                    #print(bot_script)

                    dados = {
                    'NAME_PIPELINE': name_pipeline,
                    'FOLDER': folder,
                    'BOT_SCRIPT_NAME': bot_script}

                    result_list.append(dados)