from ioctools.www.url import Url
from ioctools.www.html import Parser
import aiohttp
import aiofiles
from ioctools.misc import ChunkReader
from typing import Any, Callable, Union, Collection, Optional


def filename(url: Url):
    parsed = url.parsed
    return "./output/" + parsed.netloc + "--" + parsed.path.replace("/", "_")


# handler for the handlers :-D
class Handler:
    def __init__(self):
        self.flag = None

    def __call__(self, *args, **kwargs):
        pass


# text
class TextHandler(Handler):
    def __init__(self):
        super().__init__()

    async def __call__(self, url, text):
        links = []
        link_parser = Parser(text)
        for link in link_parser.urls:
            linked_url = url.link_add(str(link))
            print(f"in: {str(url)} -> {str(linked_url)}")
            # await self.queue.put(linked_url)
            links.append(linked_url)
        return links


# binaries
class BinaryHandler(Handler):
    def __init__(self, chunk_size):
        super().__init__()
        self.chunk_reader = ChunkReader(chunk_size)

    async def __call__(self, url, chunk, content: aiohttp.StreamReader):
        async with aiofiles.open(filename(url), mode='wb') as f:
            async for chunk in self.chunk_reader(content):
                await f.write(chunk)
        return "not available? yet"


# header handler; todo? place moar options
class HeaderHandler(Handler):
    def __init__(self, response, *handlers, **hack_the_system):
        super().__init__()
        self.response = response
        self.handlers = handlers

    async def __call__(self, url, header, **kwargs):
        if header.status == 200:
            return 'OK'
        elif header.status == 404:
            return 'NOK'


class Handle:
    def __init__(self, handlers: Collection[Handler]):
        self.handlers: Collection = handlers  # Y o u stole my bits!

    async def __call__(self, url, response: aiohttp.ClientResponse):
        links = []
        for handler in self.handlers:
            print(f":: {str(url)}")
            links = await handler(url, await response.text())
        return links
