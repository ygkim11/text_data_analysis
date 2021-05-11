import pdb
import datetime as dt
import requests
import boto3
import json
import time
import traceback

from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from dateutil.relativedelta import relativedelta

def fetch_news_contents(msg):
    item = json.loads(msg.body)

    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }


    url = f'https://news.naver.com/main/read.nhn?mode=LSD&mid=shm&sid1=101&oid={item["oid"]}&aid={item["aird"]}'

    r = requests.get(url, headers=headers)

    soup = BeautifulSoup(r.text, 'html.parser')
    node = soup.find("meta", {"property": "me2:category1"})
    publisher = node['content']
    
    # print(publisher)

    datestr_list = parse_meta_info(soup)

    print(datestr_list)

    if len(datestr_list) == 1:
        created_at = parse_datestr(datestr_list[0])
        updated_at = created_at
    elif len(datestr_list) == 2:
        created_at = parse_datestr(datestr_list[0])
        created_at = parse_datestr(datestr_list[1])
    else:
        print(url)
        print(datestr_list)
        raise RuntimeError("Wrong Date String List")

    pdb.set_trace()

def parse_datestr(date_span):
    date_str = date_span.text

    date_str = date_str.replace("오전", "AM").replace("오후", "PM")
    date= dt.datetime.strptime(date_str, "%Y.%m.%d. %p %I:%M")
    # print(date)

    return date

def parse_meta_info(soup):
    sponsor = soup.find('div', {"class": "sponsor"})
    if sponsor:
        datestr_list = sponsor.find_all('span', {"class": "t11"})
        return datestr_list
    
    raise RuntimeError('Meta info is not found')
    

if __name__ == "__main__":
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName="naver-news-list")

    while True:
        print(f"Fetching News {dt.datetime.now()}", end='', flush=True)

        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )

        for msg in messages:
            msg.delete()

        buffer = []

        for msg in messages:
            print('.', end="", flush=True)

            try:
                news = fetch_news_contents(msg)
                buffer.append(news)
            except Exception as e:
                print('*** Exception occurred!! ***')
                print(e)
                print(msg.body)
                print(traceback.format_exc())
                raise e

        pdb.set_trace()