import pdb
import re
import time
import datetime as dt
import requests
import json
import boto3
import traceback

from bs4 import BeautifulSoup

from config import *


# 결국
# 1. 뉴스 주소 모으기
# 2. 주소에서 뉴스내용 까지 다 모은 후 Dict로 정리
# 3. Dict들을 모아서 elastic search에 업로드.


def fetch_news_contents(msg):
    print()
    # print(msg.message_id)
    # print(msg.body)

    item = json.loads(msg.body)
    # print(item)

    url = 'https://news.naver.com/main/read.nhn?mode=LSD&mid=sec&sid1=101&oid={}&aid={}'.format(
        item['oid'], item['aid']
    )

    # print(url)

    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }

    r = requests.get(url, headers=headers)

    # print(r.status_code)
    # print(r.text)

    soup = BeautifulSoup(r.text, 'html.parser')

    node = soup.find("meta", { "property": "me2:category1" })
    publisher = node['content']
    # print(publisher)

    datestr_list, source_url = parse_meta_info(soup)

    # print(datestr_list)
    # print(source_url)

    if len(datestr_list) == 1:
        created_at = parse_datestr(datestr_list[0])
        updated_at = created_at
    elif len(datestr_list) == 2:
        created_at = parse_datestr(datestr_list[0])
        updated_at = parse_datestr(datestr_list[1])
    else:
        print(url)
        print(datestr_list)
        raise RuntimeError('Wrong date-string list')

    # print(created_at)

    body = soup.find("div", { "id": "articleBodyContents" })
    assert body # None 이면 Error 나게함.

    body_text = clean_text_body(body)

    images = body.find_all("img")
    image_urls = [re.sub(r'\?.*$', '', x['src']) for x in images]
    image_urls = list(set(image_urls))

    # print(image_urls)

    reporter_name, reporter_email = extract_reporter(body_text)

    # print(reporter_name)
    # print(reporter_email)

    entry = {
        'id': 'nn-{}-{}'.format(item['oid'], item['aid']),
        'title': item['title'],
        'section': 'economy',
        'naver_url': url,
        'source_url': source_url,
        'image_urls': image_urls,
        'created_at': created_at.isoformat(),
        'updated_at': updated_at.isoformat(),
        'reporter_name': reporter_name,
        'reporter_email': reporter_email,
        'body': body_text,
    }

    # print(entry)

    return entry


def extract_reporter(body_text):
    reporter = re.findall(r'([\wㄱ-ㅎ가-힣]+)\s*(특파원|기자)\s*\(?([\w\.]+@[\w\.]+\w+)\)?', body_text)
    if len(reporter) > 0:
        return reporter[0][0], reporter[0][2]

    # print(body_text[-200:])

    # raise RuntimeError("Failed to extract reporter name")

    return '', ''

def clean_text_body(body):
    body_text = body.get_text("\n")
    body_text = body_text.strip()

    body_text = re.sub(r'동영상 뉴스\s*', '', body_text)
    body_text = re.sub(r'▶\s*.*\n?', '', body_text)
    body_text = re.sub(r'\[기사제보.*\]', '', body_text)

    buffer = []

    for line in body_text.split('\n'):
        line = line.strip()
        if len(line) > 0:
            buffer.append(line)

    body_text = '\n'.join(buffer)

    # print(body_text)

    return body_text

def parse_datestr(date_span):
    datestr = date_span.text
    datestr = datestr.replace("오전", "AM").replace("오후", "PM")

    date = dt.datetime.strptime(datestr, '%Y.%m.%d. %p %I:%M')
    # print(date)

    return date

def upload_to_elastic_search(buffer):
    if len(buffer) == 0:
        return

    data = ''

    for x in buffer:
        index = {
            'index': {
                '_id': x['id']
            }
        }

        data += json.dumps(index) + '\n'

        del x['id']

        data += json.dumps(x) + '\n'

    # print(data)

    headers = { 'Content-Type': 'application/json' } # 어떤걸 던져줄지 알려줌

    resp = requests.post(
        f'{ELASTIC_SEARCH_URL}/news/_bulk?pretty&refresh',
        headers=headers,
        data=data,
        auth=ELASTIC_SEARCH_AUTH
    )

    print(resp.status_code)

def parse_meta_info(soup):
    sponsor = soup.find('div', { "class": "sponsor"})
    if sponsor:
        datestr_list = sponsor.find_all("span", { "class": "t11" })

        link = sponsor.find("a", {"class": "btn_artialoriginal"})
        source_url = link['href'] if link else ''

        return datestr_list, source_url

    raise RuntimeError('Meta info is not found')


if __name__ == "__main__":
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='naver-news-list')

    while True:
        print('[{}] Fetching news'.format(dt.datetime.now()), end='', flush=True)

        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )

        # for msg in messages:
        #     msg.delete()

        buffer = []

        for msg in messages:
            print('.', end='', flush=True)

            try:
                news = fetch_news_contents(msg)
                buffer.append(news)

            except Exception as e:
                print('*** Exception occurred!!! ***')
                print(e)
                print(msg.body)
                print(traceback.format_exc())

                raise e

        upload_to_elastic_search(buffer)

        print('!!')
