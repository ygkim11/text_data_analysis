import pdb
import datetime as dt
from dateutil.relativedelta import relativedelta

from download_data import fetch_news_docs

class EsCorpusReader(object):
    def __init__(self, date_from=None, date_to=None) -> None:
        if date_to:
            self.date_to = date_to
        else:
            self.date_to = dt.datetime.now()
        
        if date_from:
            self.date_from = date_from
        else:
            self.date_from = self.date_to - relativedelta(days=1)
        
        self._buffer = []
        self._next_page = 0

    def clear(self):
        self._buffer = []
        self._next_page = 0

    def fetch_next_page(self):
        docs = fetch_news_docs(self.date_from, self.date_to, self._next_page)
        
        self._next_page += 1
        self._buffer += docs

    def docs(self, n):
        self.clear()

        while True:
            if n == 0:
                return

            if len(self._buffer) == 0:
                self.fetch_next_page()
            if len(self._buffer) == 0:
                return

            n -= 1

            doc = self._buffer.pop(0)

            yield doc

    def titles(self, n=-1): # docs를 하기엔 데이터가 너무 크다.
        for doc in self.docs(n):
            yield doc['_source']['title']

    def ids(self):
        for doc in self.docs(n):
            yield doc['_id']


if __name__ == "__main__":
    reader = EsCorpusReader(date_to=dt.datetime(2021,5,8))
    
    for titles in reader.titles():
        print(titles)