#!/usr/bin/env python3
# areq.py

"""Asynchronously get links embedded in multiple pages' HMTL."""
import copy
import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
import os

import aiofiles
import aiohttp
from aiohttp import ClientSession

from crawly.identifier import Identified

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True

HREF_RE = re.compile(r'href="(.*?)"')
SRC_RE = re.compile(r'src="(.*?)"')
DATA_IMAGEURL_RE = re.compile(r'data-imageurl="(.*?)"')
DATA_SRC_RE = re.compile(r'data-src="(.*?)"')


class Crawler(object):
    def __init__(self, download_path: str, chunk_size: int):
        self.download_path = download_path
        self.chunk_size = chunk_size


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.
    """
    kwargs2 = copy.deepcopy(kwargs)
    kwargs2.pop('crawler')
    resp = await session.request(method="GET", url=url, **kwargs2)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()
    return html


def gather_links(html, url):
    for link in HREF_RE.findall(html):
        try:
            abslink = urllib.parse.urljoin(url, link)
        except (urllib.error.URLError, ValueError):
            logger.exception("Error parsing URL: %s", link)
            pass
        else:
            yield abslink

    for link in SRC_RE.findall(html):
        try:
            abslink = urllib.parse.urljoin(url, link)
        except (urllib.error.URLError, ValueError):
            logger.exception("Error parsing URL: %s", link)
            pass
        else:
            yield abslink

    for link in DATA_IMAGEURL_RE.findall(html):
        try:
            abslink = urllib.parse.urljoin(url, link)
        except (urllib.error.URLError, ValueError):
            logger.exception("Error parsing URL: %s", link)
            pass
        else:
            yield abslink

    for link in DATA_SRC_RE.findall(html):
        for data_src_link in link.split("|"):
            try:
                abslink = urllib.parse.urljoin(url, data_src_link)
            except (urllib.error.URLError, ValueError):
                logger.exception("Error parsing URL: %s", link)
                pass
            else:
                yield abslink


async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""
    found = set()
    try:
        html = await fetch_html(url=url, session=session, **kwargs)
    except (
        aiohttp.ClientError,
        aiohttp.http.HttpProcessingError
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return found
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return found
    else:
        for link in gather_links(html, url):
            found.add(link)

        logger.info("Found %d links for %s", len(found), url)
        return found


async def download(identity: Identified, session: ClientSession, crawler: Crawler, **kwargs):
    try:
        response = await session.request(method="GET", url=identity.url, **kwargs)
        response.raise_for_status()
        logger.info("Got response [%s] for URL: %s", response.status, identity.url)

        dir_path, file_path = identity.host_paths(crawler.download_path)
        logger.info(f"dir_path = [{dir_path}]; file_path = [{file_path}]")

        if not pathlib.Path(file_path).exists():
            os.makedirs(dir_path, exist_ok=True)
            pathlib.Path(file_path).touch(exist_ok=True)

            with open(file_path, 'wb') as fd:
                while True:
                    chunk = await response.content.read(crawler.chunk_size)
                    if not chunk:
                        break
                    fd.write(chunk)
    except (
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientError,
        aiohttp.http.HttpProcessingError
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            identity.url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
    else:
        pass


def is_forbidden_link(link):
    if link.startswith('mailto:'):
        return True
    if link.startswith('data:image/gif'):
        return True
    if link.startswith('data:'):
        return True
    return False


async def handle_url(file: IO, url: str, **kwargs) -> set:
    identity = Identified(url)

    if not identity.is_text:
        the_links = set()
        await download(identity=identity, **kwargs)
    else:
        the_links = await parse(url=url, **kwargs)
        for link in the_links:
            if not is_forbidden_link(link):
                link_id = Identified(link)
                if not link_id.is_text:
                    await download(identity=link_id, **kwargs)
    return the_links


async def write_one(file: IO, url: str, **kwargs) -> None:
    """Write the found HREFs from `url` to `file`."""

    res = await handle_url(file=file, url=url, **kwargs)
    if not res:
        return None
    async with aiofiles.open(file, "a") as f:
        for p in res:
            await f.write(f"{url}\t{p}\n")
        logger.info("Wrote results for source URL: %s", url)


async def bulk_crawl_and_write(file: IO, urls: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                write_one(file=file, url=url, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent
    the_crawler = Crawler(here.joinpath('output'), 10240)

    with open(here.joinpath("urls.txt")) as infile:
        urls = set(map(str.strip, infile))

    outpath = here.joinpath("foundurls.txt")
    with open(outpath, "w") as outfile:
        outfile.write("source_url\tparsed_url\n")

    asyncio.run(bulk_crawl_and_write(file=outpath, urls=urls, crawler=the_crawler))
