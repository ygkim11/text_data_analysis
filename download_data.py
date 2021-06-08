"""Download Data

Usage:
  download_data.py
  download_data.py [--from <date>] [--to <date>]
  download_data.py (-h | --help)
  download_data.py (-v | --version)

Options:
  -h --help         Show this screen.
  -v --version      Show version.
  --from <date>     First date from which data will be downloaded
  --to <date>       Last date to which data will be downloaded

"""
import os
import sys
import pdb
import datetime as dt

import requests
import json

from docopt import docopt
from dateutil.relativedelta import relativedelta

from config import *

PAGE_SIZE = 10

def parse_argument():
    argument = docopt(__doc__, version='0.1')

    if argument['--to']:
        date_to = dt.datetime.fromisoformat(argument['--to'])
    else:
        date_to = dt.datetime.now()

    if argument['--from']:
        date_from = dt.datetime.fromisoformat(argument['--from'])
    else:
        date_from = date_to - relativedelta(days=1)

    return date_from, date_to


def fetch_news_docs(date_from, date_to, page):
    
    query = {
        "query" : {
            "range" : {
                "created_at" : {
                    "gte":date_from.isoformat(),
                    "lte":date_to.isoformat(),
                }
            }
        },
        "size": PAGE_SIZE,
        "from": page * PAGE_SIZE,
    }

    headers = { 'Content-Type': "application/json"}

    resp = requests.get(
        f'{ELASTIC_SEARCH_URL}/news/_search',
        headers=headers,
        data=json.dumps(query),
        auth=ELASTIC_SEARCH_AUTH
    )

    # print(resp.status_code)
    # print(resp.text)

    assert resp.status_code == 200

    data = json.loads(resp.text)
    hits = data['hits']['hits']

    return hits

def download_data(date_from, date_to):
    print('Downloading data from ElasticSearch server')

    # data 폴더가 없는 경우 만들기
    if not os.path.exists('data'):
        os.makedirs('data')

    for page in range(10**6):
        print('.', end='', flush=True)

        hits = fetch_news_docs(date_from, date_to, page)

        if len(hits) == 0:
            break

        for doc in hits:
            filname = 'data/{}.txt'.format(doc['_id'])

            with open(filname, 'w', encoding='utf8') as f:
                text = json.dumps(doc['_source'], ensure_ascii=False)
                f.write(text)

    print()
    print('** Completed!! **')

# Example calls
#   python download_data.py -h
#   python download_data.py -v
#   python download_data.py --from 2021-05-01 --to 2021-05-20

if __name__ == '__main__':
    #print( sys.argv )

    date_from, date_to = parse_argument()

    # print( date_from )
    # print( date_to )
    
    data = download_data(date_from, date_to)