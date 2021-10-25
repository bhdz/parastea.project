import datetime
import signal

import logging
import os
import pathlib
import time

import asyncio
import aioconsole

import urllib.error
import urllib.parse

from crawly.identifier import Identified
from crawly.downloader import Downloaded
from crawly.parser import Parsed


class Crawler(object):
    def __init__(self, **kwargs):
        self.timeout = kwargs.pop('timeout', 0.1)
        self.quit = asyncio.Event()
        self.inputs = kwargs.pop('inputs', [])
        self.output_path = kwargs.pop('output_path', './output')

        self.acceptors = kwargs.pop('acceptors', [])
        self.visitors = kwargs.pop('visitors', [])

        self.download_handlers = kwargs.pop('download_handlers', [])
        self.parsing_handlers = kwargs.pop('parsing_handlers', [])
        self.link_cleaners = kwargs.pop('link_cleaners', [])

    def accept(self, url: str, linked_by: str):
        for acceptor in self.acceptors:
            if not acceptor(url, linked_by, self):
                return False
        return True

    def visit(self, url, linked_by):
        for visitor in self.visitors:
            if not visitor(url, linked_by, self):
                return False
        return True

    def handle_download(self, downloaded):
        # print(f'handle_download: {downloaded.identity.url}')
        for handler in self.download_handlers:
            if not handler(downloaded, self):
                return False
        return True

    def handle_parsing(self, downloaded):
        urls = []

        def yield_all():
            if downloaded.identity.is_parsable:
                parsed = Parsed(downloaded.response, parser="lxml")

                for url in parsed.urls_images:
                    yield url

                for url in parsed.urls_anchors:
                    yield url

            for parsing_handler in self.parsing_handlers:
                for url in parsing_handler(downloaded):
                    yield url

        for link in yield_all():
            link = self.clean_url(link, downloaded)
            urls.append(link)
        return urls

    def clean_url(self, link, downloaded):
        if not link:
            return ''
        link = link.strip()
        link = urllib.parse.urljoin(downloaded.identity.url, link)

        if self.link_cleaners:
            for cleaner in self.link_cleaners:
                link = cleaner(link)

        if not link.startswith('http://') and not link.startswith('https://'):
            link = 'http://' + link
        return link


def start(loop, tasks, crawler: Crawler):
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        def handler():
            shutdown_task = loop.create_task(shutdown(s, loop, tasks, crawler))
            return shutdown_task

        loop.add_signal_handler(s, handler)
    try:
        loop.run_forever()
    finally:
        loop.close()


def stop(crawler: Crawler):
    crawler.quit.set()
    pid = os.getpid()
    os.kill(pid, signal.SIGTERM)


async def input_loop(crawler: Crawler):
    while True:
        the_input: str = await aioconsole.ainput('> ')
        the_input = the_input.strip()
        print(f"input_loop: command = [{the_input}]")
        if the_input == "s" or the_input == "stop":
            print("Stop command received")
            break
    print('Exiting...')
    print("Bye")
    stop(crawler)


async def producer(name: str, queue, queue_out, crawler: Crawler, **kwargs):
    print(f'producer [name: {name}]: begin')

    break_it = False
    while not break_it:
        url, linked_by = await queue.get()
        queue.task_done()

        if crawler.accept(url, linked_by):
            await queue_out.put((Identified(url), Identified(linked_by)))

        await asyncio.sleep(crawler.timeout)

        if crawler.quit.is_set():
            break_it = True

    print(f'producer [name: {name}]: end')


async def consumer(name: str, queue, queue_out, crawler: Crawler, **kwargs):
    print(f'consumer [name: {name}]: begin')
    break_it = False
    while not break_it:
        url_id, linked_by_id = await queue_out.get()
        queue_out.task_done()

        downloaded: Downloaded = Downloaded(url_id, chunk_size=64*1024)
        crawler.handle_download(downloaded)

        if downloaded.identity.is_parsable:
            new_links = crawler.handle_parsing(downloaded)
            for new_link in new_links:
                await queue.put((new_link, downloaded.identity.url))

        crawler.visit(url_id.url, linked_by_id.url)

        await asyncio.sleep(crawler.timeout)
        if crawler.quit.is_set():
            break_it = True
    print(f'consumer [name: {name}]: end')


async def shutdown(s: signal, loop, tasks, crawler: Crawler):
    print('shutdown: begin')

    crawler.quit.set()

    [the_task.cancel() for the_task in tasks]

    print(f"shutdown: cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)

    loop.stop()
    print('shutdown: end')


async def initial(queue, crawler: Crawler):
    print(f'initial: begin')
    if isinstance(crawler.inputs, str):
        if crawler.inputs.startswith('.') or crawler.inputs.startswith('..') or crawler.inputs.startswith('/'):
            with open(os.path.abspath(pathlib.Path(crawler.inputs))) as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
                for line in lines:
                    if line[0] == "#":
                        continue
                    await queue.put((line, ''))

    else:
        try:
            it = iter(crawler.inputs)
            for i in it:
                await queue.put((i, ''))
        except TypeError:
            pass
    print(f'initial: end')


visited_path = '/home/borko/devel/crawly/visited.urls'


def register_url(url: str, linked_by: str, context: Crawler):
    global visited_path
    print(f'register_url: [url: {url}] [linked_by: {linked_by}]')

    with open(visited_path, "a") as f:
        f.write(f"{url}")
        if linked_by:
            f.write(f"\t{linked_by}")
        f.write(f"\n")
    return True


def save_download(download: Downloaded, crawler: Crawler):
    if download.identity.is_downloadable:
        output_path = pathlib.Path(crawler.output_path).absolute()
        dir_path, file_path = download.identity.host_paths(output_path)

        if not pathlib.Path(file_path).exists():
            os.makedirs(dir_path, exist_ok=True)
            pathlib.Path(file_path).touch(exist_ok=True)
            download.save_chunks(file_path)
    return True


def main():
    n_producers = 2
    n_consumers = 6

    tasks = []
    producer_tasks = []
    consumer_tasks = []

    loop = asyncio.get_event_loop()
    queue: asyncio.Queue = asyncio.Queue()
    queue_out: asyncio.Queue = asyncio.Queue()

    the_crawler = Crawler(timeout=0.01,
                          loop=loop,
                          inputs='/home/borko/devel/crawly/inputs/test.urls',
                          output_path='/home/borko/devel/crawly/output',
                          acceptors=[],
                          visitors=[register_url],
                          download_handlers=[save_download],
                          parsing_handlers=[],
                          )

    input_task: asyncio.Task = loop.create_task(input_loop(the_crawler))
    tasks.append(input_task)

    for i in range(n_producers):
        task = loop.create_task(producer(str(i), queue, queue_out, the_crawler))
        tasks.append(task)
        producer_tasks.append(task)

    for i in range(n_consumers):
        task = loop.create_task(consumer(str(i), queue, queue_out, the_crawler))
        tasks.append(task)
        consumer_tasks.append(task)

    tasks.append(loop.create_task(initial(queue, the_crawler)))

    start(loop, tasks, the_crawler)


if __name__ == "__main__":
    main()
