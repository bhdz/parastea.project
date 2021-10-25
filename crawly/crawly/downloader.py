import requests
from crawly.identifier import Identified
import os


class Downloaded(object):
    DEFAULT_CHUNK_SIZE = 1024 * 64

    def __init__(self, url_identity, **kwargs):
        self.identity: Identified = url_identity
        self.response = None
        self.chunk_size = kwargs.pop('chunk_size', Downloaded.DEFAULT_CHUNK_SIZE)
        self.init(url_identity, **kwargs)

    @property
    def url(self):
        return self.identity.url

    def init(self, url_identity, **kwargs):
        if isinstance(url_identity, str):
            self.identity = Identified(url_identity)

        kwargs['allow_redirects'] = True
        self.response = requests.get(self.url, **kwargs)
        return self.response

    def __str__(self):
        if self.response:
            status_code = f"[response/status_code = {self.response.status_code}]"
        else:
            status_code = f""

        return f"Downloaded: [url={self.url}] {status_code}"

    def save_chunks(self, host_path):
        with open(host_path, 'wb') as fd:
            for chunk in self.response.iter_content(chunk_size=self.chunk_size):
                fd.write(chunk)

    def save(self, host_path):
        with open(host_path, 'wb') as fd:
            fd.write(self.response.content)

    def host_paths(self, path_prefix='/home'):
        dir_path = os.path.join(path_prefix, self.identity.server, self.identity.segment)

        if self.identity.parsed_url.query:
            dir_path = os.path.join(dir_path, self.identity.parsed_url.query)

        if dir_path.find(':') != -1:
            dir_path = dir_path.replace(':', '_')

        filename = self.identity.filename
        extension = self.identity.extension

        if not extension:
            if self.identity.is_text:
                filename = 'index'
                extension = 'html'
            elif self.identity.is_downloadable:
                filename = 'index'
                extension = self.identity.content_type.split('/')[1]
            else:
                filename = 'index'
                extension = self.identity.content_type.split('/')[1]

            file_path = os.path.join(dir_path, f'{filename}.{extension}')
        else:
            file_path = os.path.join(dir_path, self.identity.basename)

        return dir_path, file_path
