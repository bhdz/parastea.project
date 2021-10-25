import aiohttp
from ioctools.www.url import Url


class SessionPool:
    def __init__(self):
        self.netloc_sessions = {}

    def place(self, url: Url):
        parsed = url.parsed
        print(f"parsed.netloc: {url.parsed.netloc}")

        if True:  # parsed.netloc not in self.netloc_sessions:
            session = aiohttp.ClientSession()
            self.netloc_sessions[parsed.netloc] = session
        else:
            session = self.netloc_sessions[parsed.netloc]
        return session
