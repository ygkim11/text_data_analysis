import pdb
import datetime as dt
import nltk
from nltk import classify
from nltk.cluster import kmeans
from pprint import pprint

from collections import defaultdict
from nltk.cluster.api import ClusterI
from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, ClassifierMixin, ClusterMixin

from es_corpus_reader import EsCorpusReader
from korean_text_normalizer import KoreanTextNormalizer
from gensim_vectorizer import GensimTfidfVectorizer

class KmeansTopics(BaseEstimator, ClusterMixin):
    def __init__(self, num_means=10, distance=None) -> None:
        if not distance:
            distance = nltk.cluster.cosine_distance # call by reference
        
        self.num_means = num_means
        self.distance = distance

        self.kmeans = nltk.cluster.KMeansClusterer(
            num_means=num_means, distance=distance, avoid_empty_clusters=True,
        )
    def fit(self, vectors, labels=None):
        self.kmeans.cluster(vectors)
        return self

    def predict(self, vectors):
        for vec in vectors:
            group = kmeans.classify(vec)
            mean = kmeans.means()[group]
            dist = distance(vec, mean)

            yield group, dist


if __name__ == "__main__":
    print('gensim_vectorizer')

    reader = EsCorpusReader(
        date_from=dt.datetime(2021,5,10),
        date_to=dt.datetime(2021,5,11)
    )

    corpus = list(reader.titles(n=10))

    for idx, x in enumerate(corpus):
        print(f'{idx} : {x}')


    num_means = 10
    distance = nltk.cluster.cosine_distance

    # 순서대로 실행해줌 fit-transform 두번
    pipe = Pipeline([
        ('normalizer', KoreanTextNormalizer()),
        ('vectorizer', GensimTfidfVectorizer()),
        ('clustrer', KmeansTopics(num_means, distance)),
    ])

    pipe.fit(corpus)

    results = pipe.predict(corpus)

    classified = defaultdict(list)

    for doc, (group,dist) in zip(corpus, results):
        classified[group].append((dist,doc))

    for group in classified.keys():
        classified[group].sort()

    pprint(classified)

