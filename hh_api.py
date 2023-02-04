import pandas as pd
import requests
import json
import time


def hh_api():
    js_objs = []

    for page in range(0, 5):
        js_obj = json.loads(get_page(page))
        js_objs.extend(js_obj["items"])
        if (js_obj["pages"] - page) <= 1:
            break
        time.sleep(0.5)

    df = pd.DataFrame(js_objs)
    print(df.head())

def get_page(page=0):
    """page is index str, start 0"""

    params = {
        'text': 'NAME:analytic',
        'area': 1,
        'page': page,
        'per_page': 100
    }

    req = requests.get('https://api.hh.ru/vacancies', params)
    data = req.content.decode()
    req.close()
    return data

