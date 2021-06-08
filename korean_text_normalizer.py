import pdb
import re
import datetime as dt

from konlpy.tag import Hannanum
from sklearn.base import BaseEstimator, TransformerMixin

from es_corpus_reader import EsCorpusReader

class KoreanTextNormalizer(BaseEstimator, TransformerMixin):
    def __init__(self) -> None:
        super().__init__()

        self.hannanum = Hannanum()

    def fit(self, X, y=None):
        return self
    
    def transform(self, docs):
        def generator():
            for doc in docs:
                doc = re.sub(r'[^\wㄱ-ㅎ가-힣美中&%]', ' ', doc)

                # token = self.hannanum.morphs(doc) # Normalizer 문제로 나중에 조사들이 keyword로 뽑히게 됨..
                token = self.hannanum.pos(doc)
                token = [x[0] for x in token if x[1] not in ['E', 'J']] # 조사와 어미 버리기!

                yield token
        return list(generator())


if __name__ == "__main__":
    reader = EsCorpusReader(
        date_from=dt.datetime(2021,5,10),
        date_to=dt.datetime(2021,5,11)

    )

    # for doc in reader.titles(n=10):
    #     print(doc)

    print('Loop #1')

    corpus = list(reader.titles(n=10))

    print('Loop #2')

    normalizer = KoreanTextNormalizer()
    normalized = normalizer.fit_transform(corpus)

    for idx, x in enumerate(normalized):
        print(f'{idx} : {x}')

    print('hello world')