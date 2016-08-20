from constants import *

import pandas as pd
from bs4 import BeautifulSoup
import re
import math

BEGIN_ID = 0


class SourceNullException(Exception):
    def __str__(self):
        return 'Source is null'


class Article():

    def __init__(self, sourcePath):
        self.title = sourcePath[len(HTML_SAVE_FOLDER):]
        source = open(sourcePath, 'r').read()
        if len(source) == 0:
            raise SourceNullException()
        self.words = self._getWords(source)
        self._mostFrequentTermCount = max(
            [self.words.count(x) for x in self.words])
        self.df = pd.DataFrame()

    def loadFromCsv(self, path):
        self.df = pd.read_csv(path, sep=';')
        self.title = path[path.find('articles/') +
                          len('articles/'):len(path) - 4]

        result = list(self.df['words'])

        self.words = result
        self._mostFrequentTermCount = self.df['count'].max()
        self.dfInited = True

    def _getPlainText(self, source):
        soup = BeautifulSoup(source, "lxml")
        return soup.get_text()

    def _getWords(self, source):
        text = self._getPlainText(source)
        result = []
        for word in text.split(" "):
            word = word.lower()
            if (len(word) < MAX_VALID_WORD_LENGTH and
                    re.match(r"^[a-zA-Z]+$", word) and
                    word not in BLACKLIST):
                result.append(word)

        return sorted(result)

    # TF can be evaluated only if the dataframe is in format word : count
    def tf(self, term):
        return self.words.count(term) / self._mostFrequentTermCount

    def idf(self, term, docOccurance, docCount):
        return math.log(docCount / docOccurance)

    def createDataFrame(self):
        if not self.df.empty:
            return self.df
        df = pd.DataFrame(self.words, columns=['words'])
        df['count'] = df.groupby(['words'])['words'].transform('count')
        df = df.drop_duplicates(['words'])
        self.df = df
        return df

    def fillIdf(self, globalData, docCount):
        self.df['idf'] = pd.Series(
            [self.idf(x, globalData, docCount) for x in self.words])

    def toCsv(self):
        self.df.to_csv('{}{}.csv'.format(ARTICLE_EXPORT_DIR,
                                         self.title), sep=';', index=False)

if __name__ == '__main__':
    main()
