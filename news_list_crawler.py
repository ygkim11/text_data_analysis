import pdb
import datetime as dt
import requests
import boto3
import json
import time

from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from dateutil.relativedelta import relativedelta

def fet_news_list(datestr, page, prev_last_id):
    print(f"Fetching page {page}")


    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }

    url = f"https://news.naver.com/main/main.nhn?mode=LSD&mid=sec&sid1=101&date={datestr}&page={page}"

    r = requests.get(url, headers=headers)

    # print(r.status_code)
    # print(r.text)

    soup = BeautifulSoup(r.text, 'html.parser')

    list_body = soup.find("div", {"class": "list_body"})

    buffer = []

    list_body.find_all("li")

    for item in list_body.find_all("li"):
        # print(item)

        link = item.find_all("a")[-1]
        title = link.text.strip()

        parsed_url = urlparse(link["href"])
        qs = parse_qs(parsed_url.query)

        # print(title)
        # print(qs)

        message_id = f"{qs['oid'][0]}_{qs['aid'][0]}"

        if message_id == prev_last_id:
            break

        body = {
            'oid': qs['oid'][0],
            'aird': qs['aid'][0],
            'title': title
                    }

        # pdb.set_trace()

        entry = {
            "Id": message_id,
            "MessageBody": json.dumps(body),
        }

        # print(entry)
        # pdb.set_trace()

        buffer.append(entry)

    return buffer


def fetch_news_list_for_date(queue, date):
    prev_last_id = None

    while True:
        datestr = date.strftime('%Y%m%d')
        print(f"Fetching news list on {datestr}")

        buffer = []

        # for page in range(1,1000):
        for page in range(1,20):
            entries = fet_news_list(datestr, page, prev_last_id)

            if len(entries) == 0:
                break
            
            if len(buffer) > 0 and buffer[-1]["Id"] == entries[-1]['Id']:
                break


            buffer += entries

        print(f'Total Length : {len(buffer)}')

        prev_last_id = buffer[0]["Id"]

        buffer.reverse()

        temp = {x['Id']: x for x in buffer} # 중복 제거.
        buffer = list(temp.values())

        print('Pushing to AWS SQS', end='')

        if len(buffer) > 0:
            for idx in range(0, len(buffer), 10):
                print('.', end='', flush=True)
                chunk = buffer[idx:idx+10]
                queue.send_messages(Entries=chunk)

            print('successfully pushed to AWS SQS')

        # pdb.set_trace()

        diff_from_now = dt.datetime.now() - date

        if diff_from_now.days < 1:
            time.sleep(60)
        else:
            return

def fetch_last_news_article(): # 프로그렘 비정상 종료 시 끊겻던곳 부터 시작 할 수 있게 하기위해. 
    return


if __name__ == "__main__":
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName="naver-news-list")

    # pdb.set_trace()

    base_date = dt.datetime(2021,5,1)

    for d in range(30):
        date = base_date + relativedelta(days=d)
        print(date)
        fetch_news_list_for_date(queue, date)
