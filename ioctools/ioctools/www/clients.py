from ioctools.www.url import Url
from ioctools.www.html import Parser
import aiohttp
from ioctools.www.handlers import ChunkHandler, TextHandler, Handle


class Client:
    def __init__(self, session_pool, queue_a, queue_b, chunk_size, text_handlers, binary_handlers):
        self.session_pool = session_pool
        self.queue_a = queue_a
        self.queue_b = queue_b
        self.chunk_size = chunk_size
        self.text_handlers = text_handlers
        self.binary_handlers = binary_handlers
        self.handle = Handle(text_handlers, binary_handlers)

    async def request(self, method, url, **kwargs):
        links = []
        try:
            async with self.session_pool.place(url) as session:
                async with session.request(method=method, url=str(url), **kwargs) as response:
                    handle = Handle(self.text_handlers, self.binary_handlers)
                    links = await self.handle(url, response)
        except RuntimeError as error:
            print(f"RuntimeError: {url}; {error}")
        except aiohttp.ClientConnectionError as error:
            print(f"aiohttp.ClientConnectionError: {url}; {error}")
        return links, url
