import pdb
import datetime as dt
from re import T
import gensim
from joblib.compressor import _XZ_PREFIX

from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, TransformerMixin
from gensim.matutils import sparse2full

from es_corpus_reader import EsCorpusReader
from korean_text_normalizer import KoreanTextNormalizer


class GensimBowVectorizer(BaseEstimator, TransformerMixin):
    def __init__(self):
        super().__init__()
        self.lexicon = None

    def fit(self, docs, labels=None):
        self.lexicon = gensim.corpora.Dictionary(docs)
        return self

    def transform(self, docs):
        def generator():
            for doc in docs:
                vec = self.lexicon.doc2bow(doc)
                yield sparse2full(vec, len(self.lexicon))
        print('##### ',list(generator()))
        return list(generator())

class GensimTfidfVectorizer(BaseEstimator, TransformerMixin):
    def __init__(self):
        super().__init__()
        self.lexicon = None
        self.tfidf = None

    def fit(self, docs, labels=None):
        self.lexicon = gensim.corpora.Dictionary(docs)
        self.tfidf = gensim.models.TfidfModel(dictionary=self.lexicon, normalize=True)
        return self

    def transform(self, docs):
        def generator():
            for doc in docs:
                vec = self.lexicon.doc2bow(doc)
                vec = self.tfidf[vec]
                yield sparse2full(vec, len(self.lexicon))
        print('##### ',list(generator()))
        return list(generator())



if __name__ == "__main__":
    print('gensim_vectorizer')

    reader = EsCorpusReader(
        date_from=dt.datetime(2021,5,10),
        date_to=dt.datetime(2021,5,11)
    )

    corpus = list(reader.titles(n=10))

    for idx, x in enumerate(corpus):
        print(f'{idx} : {x}')

    # 순서대로 실행해줌 fit-transform 두번
    model = Pipeline([
        ('normalizer', KoreanTextNormalizer()),
        # ('vectorizer', GensimBowVectorizer()),
        ('vectorizer', GensimTfidfVectorizer()),
    ])

    results = model.fit_transform(corpus)

    for x in results:
        print(x)