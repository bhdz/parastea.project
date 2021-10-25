import requests
from urllib.parse import urlparse
import os


class Identified(object):
    def __init__(self, url='127.0.0.1/identify'):
        self.url = url
        self.links = set()
        self.head = None
        self.parsed_url = None
        self.head = None
        self.init(url)

    def init(self, url):
        self.url = url
        self.parsed_url = urlparse(self.url)

    @property
    def headers(self):
        if not self.head:
            self.head = requests.head(self.url, allow_redirects=True)
        return self.head.headers

    @property
    def server(self):
        return self.parsed_url.netloc

    @property
    def path(self):
        return self.parsed_url.path

    @property
    def basename(self):
        return os.path.basename(self.path)

    @property
    def dirname(self):
        return os.path.dirname(self.path)

    @property
    def segment(self):
        """ The path segment between .server and .path"""
        if self.filename and not self.extension:
            segment = self.path
        else:
            segment = self.dirname

        if segment and len(segment) >= 1 and segment[-1] == os.path.sep:
            segment = segment[0:-1]

        if segment and len(segment) >= 1 and segment[0] == os.path.sep:
            segment = segment[1:]

        return segment

    @property
    def filename(self):
        splat = self.basename.split(".")
        return self.basename if len(splat) <= 1 else '.'.join(splat[0:-1])

    @property
    def extension(self):
        splat = self.basename.split(".")
        if len(splat) <= 1:
            return ''
        else:
            return splat[-1]

    @property
    def content_type(self):
        return self.headers.get('content-type')

    @property
    def is_downloadable(self):
        if self.content_type:
            content_type = self.content_type.lower()
            if 'text' in content_type:
                return False
            if 'html' in content_type:
                return False
            return True
        return False

    @property
    def is_text(self):
        if self.content_type:
            content_type = self.content_type.lower()
            if 'text' in content_type:
                return True
            if 'html' in content_type:
                return True
            return False
        return False

    @property
    def is_parsable(self):
        if self.content_type:
            content_type = self.content_type.lower()
            if 'text' in content_type and 'html' in content_type:
                return True
            return False
        return False

    def host_paths(self, path_prefix='/home'):
        dir_path = os.path.join(path_prefix, self.server, self.segment)

        if self.parsed_url.query:
            dir_path = os.path.join(dir_path, self.parsed_url.query)

        if dir_path.find(':') != -1:
            dir_path = dir_path.replace(':', '_')

        filename = self.filename
        extension = self.extension

        if not extension:
            if self.is_text:
                filename = 'index'
                extension = 'html'
            elif self.is_downloadable:
                filename = 'index'
                extension = self.content_type.split('/')[1]
            else:
                filename = 'index'
                extension = self.content_type.split('/')[1]

            file_path = os.path.join(dir_path, f'{filename}.{extension}')
        else:
            file_path = os.path.join(dir_path, self.basename)

        return dir_path, file_path

    def __contains__(self, link):
        if link in self.links:
            return True
        return False

    def __str__(self):
        return f'Identified: {self.url} [parsed_url = {self.parsed_url}]'