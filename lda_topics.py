import pdb
import datetime as dt
from tokenize import group
import nltk
from nltk import classify
from nltk.cluster import kmeans
from pprint import pprint

from collections import defaultdict
from nltk.util import pr
from sklearn.pipeline import Pipeline
from sklearn.decomposition import LatentDirichletAllocation


from es_corpus_reader import EsCorpusReader
from korean_text_normalizer import KoreanTextNormalizer
from gensim_vectorizer import GensimTfidfVectorizer




if __name__ == "__main__":
    print('gensim_vectorizer')

    reader = EsCorpusReader(
        date_from=dt.datetime(2021,5,10),
        date_to=dt.datetime(2021,5,11)
    )

    corpus = list(reader.titles(n=10))

    for idx, x in enumerate(corpus):
        print(f'{idx} : {x}')

    n_components = 10

    # 순서대로 실행해줌 fit-transform 두번
    pipe = Pipeline([
        ('normalizer', KoreanTextNormalizer()),
        ('vectorizer', GensimTfidfVectorizer()),
        ('lda', LatentDirichletAllocation(n_components=n_components)),
    ])

    classified = defaultdict(list)
    results = pipe.fit_transform(corpus)

    for doc, probs in zip(corpus, results):
        temp = [(p,i) for i,p in enumerate(probs)]
        prob, group = max(temp)

        classified[group].append((prob, doc))

    pprint(classified)

    lexicon = pipe['vectorizer'].lexicon
    probs = pipe['lda'].components_

    for i in range(len(probs)):
        words = [(p, lexicon[i]) for i, p in enumerate(probs[i])]
        words.sort(reverse=True)
        words = [x[1] for x in words[:10]]

        print(f'Topic {i} : {words}')
