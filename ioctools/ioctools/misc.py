import aiofiles
import os
import pathlib
import aiohttp


class InputFile:
    def __init__(self, filename):
        self.filename = filename

    async def readlines(self):
        async with aiofiles.open(os.path.abspath(pathlib.Path(self.filename))) as f:
            lines = [line.strip() for line in await f.readlines() if line.strip()]
            for line in lines:
                if line[0] == "#":
                    continue

                yield line


class InputDir:
    def __init__(self, path):
        self.path = path

    async def __call__(self, pattern):
        path = pathlib.Path(self.path)
        for p in path.rglob(pattern):
            print(p.name)
            yield path / p.name


class ChunkReader:
    def __init__(self, chunk_size):
        self.chunk_size = chunk_size

    async def __call__(self, content: aiohttp.StreamReader):
        chunk = await content.read(self.chunk_size)
        while chunk:
            yield chunk
            chunk = await content.read(self.chunk_size)
