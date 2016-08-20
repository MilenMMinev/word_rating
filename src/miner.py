import urllib.request
import urllib.response
import time
import re
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread

from constants import *

# amount of links crawler visits per page
CRAWLER_WIDTH = 5

visited = []
visitQueue = Queue()
visitQueue.put(ARTICLE_URL)


class Miner(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            self.minePage(self.queue.get())
            self.queue.task_done()

    def getArticleTitle(self, source):
        return re.findall(r"<title>(.*) - Wikipedia", source)[0]

    def getArticleLinks(self, source):
        relativeLinks = []
        wikiDomain = "http://wikipedia.org"

        soup = BeautifulSoup(source, "lxml")
        links = list(soup.find_all('a', href=True))
        for link in links:
            link = str(link)
            if re.search(r'href=\"/wiki/\w+\"', link):
                link = re.search(r'href=\"/wiki/\w+\"', link).group()
                link = re.match(r'href=\"(.*)\"', link).group(0)

                relativeLinks.append(wikiDomain + link[6:-1])

        return relativeLinks

# saves article text to file, and adds links to a dictionary
    def minePage(self, url, TIME=False):
        global visited
        visited.append(url)
        source = urllib.request.urlopen(url).read()
        source = source.decode('utf-8')

        filename = "{}{}.html".format(
            HTML_SAVE_FOLDER, self.getArticleTitle(source))
        f = open(filename, 'w')
        f.write(source)
        f.close()

        allLinks = self.getArticleLinks(source)
        for link in allLinks[:CRAWLER_WIDTH]:
            if link not in visited:
                global visitQueue
                visitQueue.put(link)

def main():
    for x in range(1000):
        miner = Miner(visitQueue)
        miner.start()

if __name__ == '__main__':
    main()
