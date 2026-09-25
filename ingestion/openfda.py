import os
from dotenv import load_dotenv
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DEFAULT_START_DATE
from paths import BRONZE_DATA, BRONZE_LAST_LOAD_STATE
import requests
import json
from datetime import date, datetime, timedelta
import time
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


def str_to_date(s) -> date:
    return datetime.strptime(s, '%Y-%m-%d').date()


def get_next_date_str(s) -> str:
    d = str_to_date(s)
    next_d = d + timedelta(days=1)
    return next_d.strftime('%Y-%m-%d')


def strip_url_api_key(url):
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params.pop('api_key', None)
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def build_new_url(api_url, api_key, api_ingest_date_str):
    api_ingest_date_apifortmat = api_ingest_date_str.replace('-', '')
    return (
        f'{api_url}'
        f'?api_key={api_key}'
        f'&search=transmissiondate:[{api_ingest_date_apifortmat}+TO+{api_ingest_date_apifortmat}]'
        f'&sort=transmissiondate:asc&limit=100'
    )


def write_last_load_state(file_path, date_str, page_n, page_url):
    """
    Writes the last load state to a JSON file.
    Page url is the url for the next page to load, if there are more pages to load for the same date.
    Otherwise, it is None.
    """
    with open(file_path, 'w') as f:
        json.dump({
            'date': date_str,
            'last_page': page_n,
            'page_url': strip_url_api_key(page_url)
        }, f)


def init_last_load_state(file_path, api_url, api_key, default_date_str):
    """
    Initializes the last load state JSON file with default values if it does not exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            json.dump({
                'date': default_date_str,
                'last_page': 0,
                'page_url': strip_url_api_key(build_new_url(api_url, api_key, default_date_str))
            }, f)


def get_last_load_state(file_path, api_key):
    """
    Reads the last load state from a JSON file.
    Returns a tuple of (date_str, page_n, page_url).
    If the file does not exist, returns (None, None, None).
    """
    if not os.path.exists(file_path):
        return None, None, None
    
    with open(file_path, 'r') as f:
        data = json.load(f)
        data['page_url'] = data['page_url'] + f'&api_key={api_key}'
        return data['date'], data['last_page'], data['page_url']

def main():
    
    load_dotenv()
    api_key = os.getenv('OPENFDA_API_KEY')
    os.makedirs(BRONZE_DATA, exist_ok=True)
    API_URL = 'https://api.fda.gov/drug/event.json'
    default_start_date_str = DEFAULT_START_DATE
    api_last_date = datetime.today().date()
    
    
    api_ingest_date_str, date_page, url = get_last_load_state(BRONZE_LAST_LOAD_STATE, api_key)
    if not api_ingest_date_str:
        init_last_load_state(
            BRONZE_LAST_LOAD_STATE, API_URL, api_key, default_start_date_str
        )
        api_ingest_date_str, date_page, url = get_last_load_state(BRONZE_LAST_LOAD_STATE, api_key)
    
    retry = 0
    while str_to_date(api_ingest_date_str) <= api_last_date:
        
        write_last_load_state(
            BRONZE_LAST_LOAD_STATE, api_ingest_date_str, date_page, url
        )
        try:
            response = requests.get(url, timeout=30)
        except requests.exceptions.ReadTimeout as e:
            if retry > 5:
                raise Exception(
                    f'API call timed out after 5 retries. Message: {str(e)}'
                )
            retry += 1
            time.sleep(5)
            continue
        if response.status_code in [500, 429]:
            if retry > 5:
                raise Exception(
                    f'API call failed with status code {response.status_code} after 5 retries.'
                    f'Message: {response.json()}'
                )
            retry += 1
            time.sleep(5)
            continue
        retry = 0

        if response.status_code == 200:
            data = response.json()
            with open(os.path.join(BRONZE_DATA, f'{api_ingest_date_str}_{date_page}.json'), 'w') as f:
                json.dump(data, f)
                
            # check if there are more records to fetch for the same date
            if response.headers.get('Link', None):
                url = response.headers['Link'].split(';')[0].strip('<>')
                url = url + f'&api_key={api_key}'
                date_page += 1
            else:
                api_ingest_date_str = get_next_date_str(api_ingest_date_str)  
                date_page = 0
                url = build_new_url(API_URL, api_key, api_ingest_date_str)

        else:
            if response.json()['error']['message'] != 'No matches found!':
                raise Exception(
                    f'API call failed with status code {response.status_code} and message: {response.json()}'
                )
                
            api_ingest_date_str = get_next_date_str(api_ingest_date_str) 
            url = build_new_url(API_URL, api_key, api_ingest_date_str)
        
        # to avoid hitting the rate limit
        time.sleep(0.25)
        

if __name__ == "__main__":
    main()