import asyncio
import random
import requests
from bs4 import BeautifulSoup
import lxml
import re
import urllib
from urllib.parse import urlparse
import pathlib
import os
import logging
import signal
import urllib


from crawly.identifier import Identified
from crawly.parser import Parsed
from crawly.downloader import Downloaded
from crawly.crawler import *


class History(object):
    __attrs = {
        'the_items': set(),
        'the_links': {'': set()},
    }

    def __init__(self):
        self.__dict__ = History.__attrs

    def __contains__(self, item):
        return item in self.the_items

    def __iter__(self):
        return iter(self.the_items)

    def __str__(self):
        ret = f'History: ['

        for item in self:
            ret = ret + f'\n  [[link: {item[0]}] [url: {item[1]}]]'

        ret += f"\n  [the_links: [{' '.join( f'[{k}: {v}]' for k, v in self.the_links.items() )}]]"
        ret += f"\n]"
        return ret

    def add(self, item):
        self.the_items.add(item)
        if not item[1] in self.the_links:
            self.the_links[item[1]] = set()
        self.the_links[item[1]].add(item[0])

    def item(self, link, url):
        new_item = (link, url)
        if url in self.the_links:
            if not self.the_links[url]:
                self.the_links[url] = set()
            self.the_links[url].add(link)

        return new_item

    @property
    def items(self):
        for item in self.the_items:
            yield item

    def make(self, link, url):
        item = self.item(link, url)
        return self.add(item)

    def links_for(self, url):
        url_links = []
        for item_link in self.links(url):
            url_links.append(item_link)
        return url_links

    def links(self, url):
        if url in self.the_links:
            for link in self.the_links[url]:
                yield link
        else:
            for item_link, item_url in self.items:
                if url == item_url:
                    yield item_link

    def urls(self):
        previous = set()
        for item_link, item_url in self.items:
            if item_url not in previous:
                previous.add(item_url)
                yield item_url


def eyeball_history():
    History().make("http://example.com/", '')
    History().make("http://example.com/one/one", 'http://example.com/')
    History().make("http://example.com/one/two", 'http://example.com/')
    History().make("http://example.com/one/three", 'http://example.com/')

    History().make('http://one.one.com/one/one/one', "http://example.com/one/one")
    History().make('http://one.two.com/one/one/two', "http://example.com/one/one")
    History().make('http://one.three.com/one/one/three', "http://example.com/one/one")

    History().make('http://foo.com/', 'http://example.com/one/three')
    History().make('http://bar.com/', 'http://example.com/one/three')
    History().make('http://foobar.com/', 'http://example.com/one/three')

    History().make('http://alpha.com', 'http://foobar.com')
    History().make('http://beta.com/', 'http://foobar.com')

    print(History())
    print()

    url2 = 'http://example.com/'

    for url in History().urls():
        print(f'url: {url}')

    print()

    for url in History().urls():
        links2 = ' '.join(f'[link: {url}]' for url in History().links_for(url))
        print(f'[[url: {url}] [links: {links2}]]')

    print()
    # exit()


here = pathlib.Path(__file__).parent.parent

visited_path = here.joinpath('visited.urls')
config_path = here.joinpath('config.json')
log_path = here.joinpath('crawly.log')


history = set()


def get_history():
    global history
    return history


def mirror(consumed: Downloaded, crawler: Crawler):
    dir_path, file_path = consumed.identity.host_paths(crawler.output_path)

    if consumed.identity.is_downloadable:
        if not pathlib.Path(file_path).exists():
            os.makedirs(dir_path, exist_ok=True)
            pathlib.Path(file_path).touch(exist_ok=True)
            consumed.save_chunks(file_path)

            crawler.say(f"mirror: [file_path = {file_path}]: downloaded")
        else:
            crawler.say(f"mirror: [file_path = {file_path}]: already exists")
    return True


class Saved(Downloaded):
    def __init__(self, path, **kwargs):
        self.path = path
        super().__init__(**kwargs)
        self.init()

    def init(self, **kwargs):
        super().init(kwargs)

    def prepare(self, download, output_path):
        dir_path, file_path = self.host_paths(download.identity.host_paths(output_path))
        if not pathlib.Path(self.path).exists():
            os.makedirs(dir_path, exist_ok=True)
            pathlib.Path(file_path).touch(exist_ok=True)


def save_download(download: Downloaded, crawler: Crawler):
    if download.identity.is_downloadable:
        dir_path, file_path = download.identity.host_paths(crawler.output_path)
        if not pathlib.Path(file_path).exists():
            os.makedirs(dir_path, exist_ok=True)
            pathlib.Path(file_path).touch(exist_ok=True)
            download.save_chunks(file_path)

            crawler.say(f"save_download: [file_path = {file_path}]: downloaded")
        else:
            crawler.say(f"save_download: [file_path = {file_path}]: already exists")
    return True


def register_url(url: str, linked_by: str, context: Crawler):
    global visited_path

    with open(visited_path, "a") as f:
        f.write(f"{url}")
        if linked_by:
            f.write(f"\t{linked_by}")
        f.write(f"\n")
    return True


def history_accept(url, linked_by, crawler: Crawler):
    ret = True
    hist = get_history()
    item = url
    if item in hist:
        ret = False
        crawler.say(f'history_accept: [url: {url}][link: {linked_by}] in history. -> Skipping')
    else:
        crawler.say(f'history_accept: [url: {url}][link: {linked_by}] not in history <- Adding')
        hist.add(item)
    return ret


def deny_back(url, linked_by, crawler: Crawler):
    """ This acceptor denies back-linking """
    ret = True

    if linked_by:
        joined = urllib.parse.urlparse(urllib.parse.urljoin(linked_by, url))
        url_parsed = urllib.parse.urlparse(url)

        # if joined.netloc != url.netloc:
        #    return False
        if len(joined) < len(url):
            ret = False
    if not ret:
        crawler.say(f'url: {url} is denied!')
    else:
        crawler.say(f'url: {url} is accepted')
    return ret


def crawl(n_producers,
          n_consumers,
          n_downloaders,
          inputs,
          output_path):

    crawler = Crawler(config=config_path,
                      output_path=output_path,
                      log_filepath=log_path,
                      shutdown=shutdown,
                      inputs=inputs,
                      n_producers=n_producers,
                      n_consumers=n_consumers,
                      n_downloaders=n_downloaders,
                      sleep_time=0.01,
                      chunk_size=32*1024,
                      parsing_handlers=[],
                      download_handlers=[save_download],
                      visitors=[register_url],
                      acceptors=[
                          history_accept,
                          # deny_back,
                      ],
                      validators=[],
                      )
    crawler.start()


if __name__ == "__main__":
    import sys
    start_inputs = sys.argv[1]
    start_path = sys.argv[2]
    crawl(1, 3, 3, start_inputs, start_path)
