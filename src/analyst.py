import pandas as pd
import multiprocessing as mp
import time
from os import listdir
from os.path import isfile, join

from constants import *
from article import Article
from article import SourceNullException


class AnalyzerProcess(mp.Process):
    def __init__(self, queue, files):
        mp.Process.__init__(self)
        self.files = files
        self.queue = queue

    def run(self):
        self.loadArticles(self.files, verbose=True)
        articlesQueue.task_done()

    def loadArticles(self, files, verbose=False):
        if verbose:
            print('Loading {} total articles'.format(len(files)))
        # Make cnt global
        cnt = 0
        for f in files:
            cnt += 1
            try:
                article = Article(f)
                self.queue.put(article)
            except SourceNullException:
                print('skipping {}'.format(f))


def dump_queue(queue):
    """
    Empties all pending items in a queue and returns them in a list.
    """
    result = []
    while not queue.empty():
        result.append(queue.get())
    return result


class Analyst():
    def __init__(self, queue):
        self.docsDF = pd.DataFrame()

        # A list of article objects
        self.articles = dump_queue(queue)
        print(self.articles)

    def fillTf(self):
        for article in self.articles:
            df = article.createDataFrame()
            df['tf'] = df['words'].apply(article.tf)
            article.df = df

    def getDocOcc(self, term):
        return int(self.docsDF['doc_occurance'].loc[self.docsDF['words'] == term])

    def fillIdf(self):
        for article in self.articles:
            print('fillIdf {} of {}'.format(
                self.articles.index(article), len(self.articles)))
            df = article.createDataFrame()
            article.df['idf'] = df['words'].apply(lambda row: article.idf(
                row, self.getDocOcc(row), len(self.articles)))
            article.df['tfidf'] = df['tf'] * df['idf']

    def mergeDocData(self):
        result = pd.DataFrame()
        for article in self.articles:
            print('merging {} of {}'.format(article.title, len(self.articles)))
            result = result.append(article.createDataFrame())
        result['doc_occurance'] = result.groupby(
            ['words'])['words'].transform('count')
        result = result.drop('count', 1)
        result = result.drop_duplicates()
        result.to_csv('doc_data.csv', sep=';')
        self.docsDF = result

    def exportArticleData(self, outputDir):
        for a in self.articles:
            a.createDataFrame().to_csv(join(outputDir, a.title))


def main():
    workers_list = list()
    d = HTML_SAVE_FOLDER
    THREADS = 8

    files = [join(d, f) for f in listdir(d) if isfile(join(d, f))]
    cnt = len(files)
    files = [files[cnt // THREADS * i:cnt //
                   THREADS * (i + 1)] for i in range(THREADS)]
    for f in files:
        worker = AnalyzerProcess(articlesQueue, f)
        workers_list.append(worker)
        worker.daemon = True
        worker.start()

    for worker in workers_list:
        worker.join()

    analyst = Analyst(articlesQueue)
    print('done')
    analyst.mergeDocData()
    analyst.fillTf()
    analyst.fillIdf()
    analyst.exportArticleData(ANALYZED_ARTICLES)

if __name__ == '__main__':
    articlesQueue = mp.Manager().Queue()

    main()
