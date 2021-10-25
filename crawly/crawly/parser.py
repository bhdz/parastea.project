import requests
from bs4 import BeautifulSoup


class Parsed(object):
    def __init__(self, response: requests.Response, parser="lxml"):
        self.response = response
        self.parser = parser
        self.soup = BeautifulSoup(self.response.text, features=self.parser)

    @property
    def url(self):
        return self.response.url

    @property
    def urls_anchors(self):
        for tag in self.soup.find_all('a'):
            value = tag.get('href')

            if value:
                yield value
            value = tag.get('data-imageurl')
            if value:
                yield value

    @property
    def urls_images(self):
        for tag in self.soup.find_all('img'):
            value = tag.get('src')
            yield value

            data_src = tag.get('data-src')
            if data_src:
                for src in data_src.split("|"):
                    if src:
                        yield src

    @property
    def urls(self):
        for link in self.urls_images:
            yield link
        for link in self.urls_anchors:
            yield link

    def __str__(self):
        return f"Parsed: [url = {self.url}]"
