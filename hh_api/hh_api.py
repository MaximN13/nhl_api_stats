import requests
import json
import time
import os
from db_sql.sql import *
from alive_progress import alive_bar
from typing import Dict

CACHE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "cache")
__SETTINGS_PATH = "settings.json"
__API_BASE_URL = "https://api.hh.ru"

def loading(i, len:int):
    with alive_bar(total=len) as bar:
        for i in range(i):
            time.sleep(.005)
            bar()

def hh_api():
    load_files_vacancies_json()
    ins_to_db()
    pass

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

    for page in range(0, 20):
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
    with open('file1.json', 'r') as f:
        js_objs = json.load(f)

    df_vacancies = pd.DataFrame(js_objs)
    df_vacancies = df_vacancies.drop_duplicates(subset="id")

    lst_cols = list(df_vacancies.columns).copy()
    insert_rows(schema='headhunter', table='vacancies', rows=df_vacancies.values, target_fields=lst_cols)

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

