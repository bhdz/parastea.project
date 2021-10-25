from urllib.parse import urlparse, urlsplit


class Url:
    def __init__(self, url):
        self.url = url
        self.links = []
        self.linked_by = []
        self.parsed = urlparse(url)
        self.split = urlsplit(url)

    def __str__(self):
        return self.url

    def __eq__(self, another):
        return self.url == another.url

    def __ne__(self, another):
        return not self == another

    def __hash__(self):
        return hash(self.url)

    def promote(self, link: str):
        url_split = self.split
        if link.startswith('/'):
            return Url(url_split.scheme + '://' + self.parsed.netloc + link)
        elif link.startswith("http://") or link.startswith("https://"):
            return Url(link)
        else:
            splat = self.parsed.path.split("/")
            if len(splat) > 0:
                return Url(url_split.scheme + '://' + self.parsed.netloc + "/".join(splat[0:-1]) + link)
            else:
                return Url(url_split.scheme + '://' + self.parsed.netloc + link)

    def link_add(self, link: str):
        link_url = self.promote(link)
        if link_url not in self.links:
            self.links.append(link_url)
            link_url.linked_by.append(self)
        return link_url


def makeurl(url: Url, link: str):
    url_split = urlsplit(url.url)
    if link.startswith('/'):
        return Url(url_split.scheme + '://' + url.parsed.netloc + link)
    elif link.startswith("http://") or link.startswith("https://"):
        return Url(link)
    else:
        splat = url.parsed.path.split("/")
        if len(splat) > 0:
            return Url(url_split.scheme + '://' + url.parsed.netloc + "/".join(splat[0:-1]) + link)
        else:
            return Url(url_split.scheme + '://' + url.parsed.netloc + link)
