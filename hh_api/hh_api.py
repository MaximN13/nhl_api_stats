import requests
import json
import time
import os
from db_sql.sql_hh import *
from alive_progress import alive_bar
from typing import Dict

CACHE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "cache")
__SETTINGS_PATH = "settings.json"
__API_BASE_URL = "https://api.hh.ru"
__APP_TOKEN = 'APPLNQ2DNUPEPOSBNI5UGMRMGNEHNUBFT1AIGAQHDOM6RMU9PNGS2OBEQCG6UKTR'

def loading(i, len:int):
    with alive_bar(total=len) as bar:
        for i in range(i):
            time.sleep(.005)
            bar()

def main_hh_api():
    load_files_vacancies_json()
    ins_to_db()

def config():
    config_path = "./hh_api/settings.json"
    with open(config_path, "r") as cfg:
        config: Dict = json.load(cfg)

    for key, value in config.items():
        #print(key, config[key])
        if isinstance(config[key], dict) and key == 'params':
            params = config[key]
    return params

def load_files_vacancies_json():
    js_objs = []

    for page in range(0, 1):
        js_obj = json.loads(get_page(page))
        if "items" not in js_obj:
            break
        js_objs.extend(js_obj["items"])
        print(f"found items: %s"%js_obj["found"])
        if (js_obj["pages"] - page) <= 1:
            break
        time.sleep(0.5)

    with open('file1.json', 'w') as f:
        json.dump(js_objs, f, ensure_ascii=False)

def ins_to_db():
    dbsql_hh = DbSql_hh(host="localhost", user="headhunter_su", password="headhunter_su", db="headhunter")
    with open('file1.json', 'r') as f:
        js_objs = json.load(f)

    df_vacancies = pd.DataFrame(js_objs)
    df_vacancies = df_vacancies.drop_duplicates(subset="id")

    lst_cols = list(df_vacancies.columns).copy()
    dbsql_hh.insert_rows(schema='headhunter', table='vacancies', rows=df_vacancies.values, target_fields=lst_cols)

def get_page(page=0):
    """page is index str, start 0"""
    #params = config()
    #print(params)
    params = {
        'text': 'Testing',
        'area': 1,
        'page': page,
       ## 'pages': 10,
        'per_page': 20
    }
    try:
        req = requests.get(__API_BASE_URL + '/vacancies', params)
    except:
        print("requests error")
    data = req.content.decode()
    req.close()
    return data

def get_oauth():
    params = {
        #'response_type': 'code',
        'client_id': 'QTFQQVA7QKCJJ7MG6AKN0RN1EM0KPUGBDDIR69QVP2HL40HOG3QSKRK16KCAGS6P',
        'client_secret': 'NFKPIA1L5AGV047DBQE88CJN8K1A4RN8LFISL1QEDF4E3UE9C8ELVJ1LA0DN4KG0',
        'grant_type': 'client_credentials'
        #'state':
        #'redirect_uri': 'https://localhost'
    }

    url = 'https://hh.ru/oauth/token'
    resp = requests.post(url=url, params=params)
    print(f'response oauth: {resp.text}')