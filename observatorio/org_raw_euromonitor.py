import gc
import os
import re
import requests
import shlex
import shutil
import subprocess
import time
import traceback
from datetime import date
from threading import Timer
from unicodedata import normalize
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.chrome.webdriver import WebDriver, Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import TimeoutException

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import Select
import json


from time import sleep
import pandas as pd







import redis
from azure.storage.filedatalake import FileSystemClient


def authenticate_datalake() -> FileSystemClient:
    from azure.identity import ClientSecretCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    credential = ClientSecretCredential(
        tenant_id=os.environ['AZURE_TENANT_ID'],
        client_id=os.environ['AZURE_CLIENT_ID'],
        client_secret=os.environ['AZURE_CLIENT_SECRET'])

    adl = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", os.environ['AZURE_ADL_STORE_NAME']), credential=credential)

    return adl.get_file_system_client(file_system=os.environ['AZURE_ADL_FILE_SYSTEM'])


def __normalize(string: str) -> str:
    return normalize("NFKD", string.strip()).encode("ASCII", "ignore").decode("ASCII")


def __normalize_replace(string: str) -> str:
    return re.sub(
        r"[.,;:{}()\n\t=]",
        "",
        __normalize(string)
        .replace(" ", "_")
        .replace("-", "_")
        .replace("|", "_")
        .replace("/", "_")
        .replace(".", "_")
        .upper(),
    )


def __check_path_exists(adl, path) -> bool:
    try:
        next(adl.get_paths(path, recursive=False, max_results=1))
        return True
    except:
        return False


def __read_in_chunks(file_object, chunk_size=100 * 1024 * 1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 100Mb."""
    offset, length = 0, 0
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break

        length += len(data)
        yield data, offset, length
        offset += chunk_size


def __upload_bs(adl, lpath, rpath) -> None:
    file_client = adl.get_file_client(rpath)
    try:
        with open(lpath, mode="rb") as file:
            for chunk, offset, length in __read_in_chunks(file):
                if offset > 0:
                    file_client.append_data(data=chunk, offset=offset)
                    file_client.flush_data(length)
                else:
                    file_client.upload_data(data=chunk, overwrite=True)
    except Exception as e:
        file_client.delete_file()
        raise e


def __create_directory(schema=None, table=None, year=None) -> str:
    if year:
        return f"{LND}/{schema}__{table}/{year}"
    return f"{LND}/{schema}__{table}"


def __drop_directory(adl, schema=None, table=None, year=None) -> None:
    adl_drop_path = __create_directory(schema=schema, table=table, year=year)
    if __check_path_exists(adl, adl_drop_path):
        adl.delete_directory(adl_drop_path)


def __upload_file(adl, schema, table, year, file) -> None:
    split = os.path.basename(file).split(".")
    filename = __normalize_replace(split[0])
    file_type = list(
        map(str.lower, [_str for _str in map(str.strip, split[1:]) if len(_str) > 0])
    )

    directory = __create_directory(schema=schema, table=table, year=year)
    file_type = ".".join(file_type)
    adl_write_path = f"{directory}/{filename}.{file_type}"

    __upload_bs(adl, file, adl_write_path)


def __download_file(url, output) -> None:
    request = f"wget --no-check-certificate --progress=bar:force {url} -O {output}"
    process = subprocess.Popen(
        shlex.split(request), stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    timer = Timer(300, process.kill)
    try:
        timer.start()
        process.communicate()
    finally:
        timer.cancel()


def __call_redis(host, password, function_name, *args):
    db = redis.Redis(
        host=host,
        password=password,
        port=6379,
        db=0,
        socket_keepalive=True,
        socket_timeout=2,
    )
    try:
        method_fn = getattr(db, function_name)
        return method_fn(*args)
    except Exception as _:
        raise _
    finally:
        db.close()


def main(**kwargs):
    """
    Função principal de download dos dados.
    Este crawler usa o Selenium para extrair os dados.
    Dentro de um for ele percorre todos os botões necessários para chegar no download,
    baixa o arquivo para uma pasta temporária, transforma para parquet e faz o upload para o ADL.
    Obs: Por algum motivo o proxy apresentou problemas no ADF e precisou ser modificado para ser
    utilizado nessa extração, por isso observa-se codigos como: 'httpProxy' : f'http://:@{proxy_addr}',
    onde usa-se apenas o endereço de ip e porta da variável de ambiente deixando o usuário e senha vazio.
    """

    host, passwd = kwargs.pop("host"), kwargs.pop("passwd")

    schema = "euromonitor"
    table = "marketsizes"
    year = None

    key_name = f"org_raw_{schema}_{table}"
    tmp = f"/tmp/{key_name}/"
    os.makedirs(tmp, mode=0o777, exist_ok=True)
    driver = None

    try:
        if kwargs["reset"]:
            __call_redis(host, passwd, "delete", key_name)

        if kwargs["reload"] is not None:
            raise NotImplementedError("Esta base não suporta o parâmetro reload")

        #if __call_redis(host, passwd, "exists", key_name):
        #    _date_str = __call_redis(host, passwd, "get", key_name).decode()
        #    last_update = datetime.strptime(_date_str, "%Y-%m-%d").date()
        #else:
        last_update = None

        if not last_update or relativedelta(date.today(), last_update).months >= 1:
            adl = authenticate_datalake()

            proxy = os.environ.get("https_proxy", "http://squid:JCmeuK6h3mfSHTGp7S4f@191.234.184.239:3128")
            proxy_auth, proxy_addr = proxy.split('@')

            opts = Options()
            prefs = {
                "download.default_directory": tmp,
                "download.propt_for_download": False,
            }
            opts.add_experimental_option("prefs", prefs)
            opts.add_argument("--no-sandbox")
            opts.add_argument("--window-size=1920,1080")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)
            opts.add_argument("--disable-gpu")
            opts.add_argument("--headless")
            opts.add_argument("--ignore-certificate-errors")

            cap = DesiredCapabilities.CHROME
            cap['goog:loggingPrefs'] = {'browser': 'ALL'}
            cap['proxy'] = {
                'httpProxy' : f'http://:@{proxy_addr}',
                'ftpProxy' : f'http://:@{proxy_addr}',
                'sslProxy' : f'http://:@{proxy_addr}',
                'noProxy' : None,
                'proxyType' : "MANUAL",
                'class' : "org.openqa.selenium.Proxy",
                'autodetect' : False
            }

            driver = WebDriver(options=opts, desired_capabilities=cap)

            driver.get('https://api-portal.euromonitor.com/')
            #driver.get('https://api.euromonitor.com/catalog/category')

        
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div/div[2]/nav/div/div[2]/ul[2]/li/a"))
                )

            euromonitor = driver.find_element(By.XPATH, "/html/body/div/div[2]/nav/div/div[2]/ul[2]/li/a").click()

            username = driver.find_element(By.XPATH,"//*[@id='Username']")
            username.send_keys("zhang.yuan@senaicni.com.br")

            password = driver.find_element(By.XPATH, "//*[@id='Password']")
            password.send_keys("Euromonitor12345")

            euromonitor = driver.find_element(By.XPATH, "//*[@id='submit']").click()
            time.sleep(3)
            euromonitor = driver.find_element(By.XPATH, "//*[@id='navigation']/div/div[2]/ul[1]/li[2]/a").click()
            time.sleep(3)
            euromonitor = driver.find_element(By.XPATH, "//*[@id='ap-container']/div[3]/div/div/main/ul/li[1]/h3/a").click()
            time.sleep(3)
            euromonitor = driver.find_element(By.XPATH, "//*[@id='btnOpenConsole']").click()
            time.sleep(3)
            euromonitor = Select(driver.find_element(By.XPATH, "//*[@id='authenticationType']"))
            time.sleep(3)
            euromonitor.select_by_visible_text("Authorization code")
            time.sleep(3)
            euromonitor = driver.find_element(By.XPATH, "//*[@id='httpRequest']/span").click()
            time.sleep(3)


            token =  driver.find_element(By.XPATH, "/html/body/div/div[3]/div/div[2]/main/article/div[4]/pre/code").text

            time.sleep(3)
            euromonitor = driver.find_element("xpath", "/html/body/div/div[3]/div/div[2]/main/article/button").click()
            time.sleep(5)
            euromonitor = driver.find_element("xpath", "/html/body/div/div[3]/div/div[2]/main/article/button").click()
            time.sleep(3)
            euromonitor = driver.find_element("xpath", "/html/body/div/div[3]/div/div[2]/main/div[2]/p").text

            #print(euromonitor)

            #print(token[220:1779])
            #'''

            #token = 'Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjdBNzNGNzMzMDYwRDhEMTQ2Q0I1QkE4NUFBNkNGMjNDQkJBODA1NjciLCJ0eXAiOiJKV1QiLCJ4NXQiOiJlblAzTXdZTmpSUnN0YnFGcW16eVBMdW9CV2MifQ.eyJuYmYiOjE2NzU2ODY3NDEsImV4cCI6MTY3NTc3MzE0MSwiaXNzIjoiaHR0cHM6Ly9sb2dpbi5ldXJvbW9uaXRvci5jb20iLCJhdWQiOlsiaHR0cHM6Ly9sb2dpbi5ldXJvbW9uaXRvci5jb20vcmVzb3VyY2VzIiwiRXVyb21vbml0b3IuU3RhdGlzdGljcy5DYXRhbG9nU2VydmljZSIsIkV1cm9tb25pdG9yLk1lbWJlcnNoaXAuVXNlclNlcnZpY2UiLCJFdXJvbW9uaXRvci5TdGF0aXN0aWNzLk1hcmtldFNpemVTZXJ2aWNlIiwiRXVyb21vbml0b3IuU3RhdGlzdGljcy5BdXRob3Jpc2F0aW9uU2VydmljZSIsIkV1cm9tb25pdG9yLlN0YXRpc3RpY3MuQnJhbmRTaGFyZVNlcnZpY2UiLCJFdXJvbW9uaXRvci5TdGF0aXN0aWNzLkNvbXBhbnlTaGFyZVNlcnZpY2UiXSwiY2xpZW50X2lkIjoiNjFjMTBiZGY3MjZkNDRhODkyNDZhNDk1MDQ0N2RhNjkiLCJzdWIiOiIyMmJmODM2MC0yMTJlLTQ3OTUtYTFiYi1lYmIyNWVhNTU0MmQiLCJhdXRoX3RpbWUiOjE2NzU2ODY3MTMsImlkcCI6ImxvY2FsIiwic2NvcGUiOlsib3BlbmlkIiwiRXVyb21vbml0b3IuU3RhdGlzdGljcy5DYXRhbG9nU2VydmljZSIsIkV1cm9tb25pdG9yLk1lbWJlcnNoaXAuVXNlclNlcnZpY2UiLCJFdXJvbW9uaXRvci5TdGF0aXN0aWNzLk1hcmtldFNpemVTZXJ2aWNlIiwiRXVyb21vbml0b3IuU3RhdGlzdGljcy5BdXRob3Jpc2F0aW9uU2VydmljZSIsIkV1cm9tb25pdG9yLlN0YXRpc3RpY3MuQnJhbmRTaGFyZVNlcnZpY2UiLCJFdXJvbW9uaXRvci5TdGF0aXN0aWNzLkNvbXBhbnlTaGFyZVNlcnZpY2UiXSwiYW1yIjpbInB3ZCJdfQ.ruT55osmp-eSS5h_6-O-rNu0rdbIOFqr3LZV5f5SXNmoPw23qZXGhNXxsWgXA_BnNLQFa3vXGIjxIJc2BSj2yWgDq8sY7QDUaUXoL5klzfLvm9N6zsZdfd0DtyrHBUnHuDD5vSSM9UunizRaT5DehAuyFob_PKMZ9N6H7z6PnKEX5IEaeyhaLJXPVyfJVwQZictEOFMUrsPdKSt8T6_fGOm6CY7eE-NKxqw1IvLNiy4g2cRqouKBcrgFAgnEW_J3s1UY3d2ORn8qnyayu99WI-Vaq0TfL4BcjmZ392VttLDv_hjY0NlNCnUyDopYrGAldSx3Oep3fLHIGmGVqeMAmg'
            tk = token[220:1779]
            #__call_redis(host, passwd, "set", key_name, str(date.today()))
            # 
            headers = {
                'Accept':'application/json; api-version=1.0',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Ocp-Apim-Subscription-Key':'388b898900e04232b57412beaea09f24',
                'Authorization': tk} 
                
            #resp = requests.get('https://api.euromonitor.com/catalog/category', headers=headers)
            #print(resp.text)

            resp = requests.get('https://api.euromonitor.com/statistics/marketsizes/?GeographyIds=195,259,310,380,182,389&Limit=100000&CategoryIds=84', headers=headers)
            #print(resp.status_code)
            dataLocation = json.loads(resp.text)
            
            marketSizes = pd.json_normalize(dataLocation['marketSizes'], 'data', ['researchYear','geographyId','geographyName','categoryId','categoryName','industry',
            'dataTypeId','dataType','unitName','inflationType','exchangeRateName','perCapitaName','unitMultiplier','isDefaultDataType'], record_prefix='issue_level_problem_')
            #print(marketSizes)

            df = pd.DataFrame(marketSizes)
            time.sleep(5)   
            df['date'] = pd.to_datetime('today').strftime("%d/%m/%Y")

            parquet_output = tmp + 'euromonitor_marketsizes' + '.parquet'
            df.to_parquet(parquet_output, index=False)
            print(parquet_output)

            __drop_directory(adl, schema, table)
            time.sleep(5)
            __upload_file(adl, schema, table,year=year,file=parquet_output)





        return {"exit": 200}
    except TimeoutException as e:
        if driver:
            #log = driver.get_log('browser') # Imprimir variável de log para debugar erros do navegador
            raise TimeoutException("Erro de Timeout. Certifique-se de que a página está acessível e que os elementos continuão iguais.")
        else:
            raise Exception(f" {str(e)}")
    except Exception as e:
        raise e
    finally:
        shutil.rmtree(tmp)
        if driver:
            driver.quit()


def execute(**kwargs):
    global DEBUG, LND

    DEBUG = bool(int(os.environ.get('DEBUG', 1)))
    LND = '/tmp/dev/lnd/crw' if DEBUG else '/lnd/crw'

    start = time.time()
    metadata = {'finished_with_errors': False}
    try:
        log = main(**kwargs)
        if log is not None:
            metadata.update(log)
    except Exception as e:
        raise e
        metadata['exit'] = 500
        metadata['finished_with_errors'] = True
        metadata['msg'] = str(e)
        metadata['traceback'] = traceback.format_exc()
    finally:
        metadata['execution_time'] = time.time() - start

    if kwargs['callback'] is not None:
        requests.post(kwargs['callback'], json=metadata)

    return metadata


DEBUG, LND = None, None
if __name__ == '__main__':
    import dotenv
    ROOT_PATH = os.path.dirname(__file__)
    #from app import app
    dotenv.load_dotenv(ROOT_PATH + '/debug.env')
    #dotenv.load_dotenv(app.ROOT_PATH + '/debug.env')
    exit(execute(host='localhost', passwd=None, reload=None, reset=False, callback=None))
