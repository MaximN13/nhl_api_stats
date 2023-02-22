import pandas as pd
import requests
import json
import time
import os
from sql import *
from alive_progress import alive_bar

CACHE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "cache")
__SETTINGS_PATH = "settings.json"
__API_BASE_URL = "https://api.hh.ru/"

def loading(i, len:int):
    with alive_bar(total=len) as bar:
        for i in range(i):
            time.sleep(.005)
            bar()

def hh_api():
    js_objs = []

    for page in range(0, 10):
        js_obj = json.loads(get_page(page))
        js_objs.extend(js_obj["items"])
        if (js_obj["pages"] - page) <= 1:
            break
        time.sleep(0.5)

    df_vacancies = pd.DataFrame(js_objs)
    df_vacancies = df_vacancies.drop_duplicates(subset="id")
    lst_cols = list(df_vacancies.columns).copy()

    insert_rows(schema='headhunter', table='vacancies', rows=df_vacancies.values, target_fields=lst_cols)
def get_page(page=0):
    """page is index str, start 0"""

    params = {
        'text': 'Python Developer',
        'area': 1,
        'page': page,
       ## 'pages': 10,
        'per_page': 20
    }

    req = requests.get('https://api.hh.ru/vacancies', params)
    data = req.content.decode()
    req.close()
    return data

