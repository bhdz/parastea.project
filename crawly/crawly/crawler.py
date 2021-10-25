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

from .identifier import Identified
from .downloader import Downloaded
from .parser import Parsed


class Crawler(object):
    SLEEP_TIME = 0.01

    def __init__(self, **kwargs):
        self.config = kwargs.pop('config', '')
        self.log_filepath = kwargs.pop('log_filepath', 'example.log')
        self.output_path = kwargs.pop('output_path', '../output')

        self.sleep_time = kwargs.pop('sleep_time', Crawler.SLEEP_TIME)
        self.n_producers = kwargs.pop('n_producers', 1)
        self.n_consumers = kwargs.pop('n_consumers', 1)
        self.n_downloaders = kwargs.pop('n_downloaders', 1)

        self.parsing_handlers = kwargs.pop('parsing_handlers', [])
        self.download_handlers = kwargs.pop('download_handlers', [])

        self.chunk_size = kwargs.pop('chunk_size', Downloaded.DEFAULT_CHUNK_SIZE)
        self.loop = None
        self.queue = None
        self.queue_down = None
        self.queue_urls = None
        self.tasks = []

        self.inputs = kwargs.pop('inputs', '../input')
        self.input_task = None
        self.input_loop = kwargs.pop('input_loop', input_loop)

        self.shutdown = kwargs.pop('shutdown', shutdown)
        self.shutdown_task = None

        self.producer = kwargs.pop('producer', producer)
        self.producer_tasks = []

        self.consumer = kwargs.pop('consumer', consumer)
        self.consumer_tasks = []

        self.downloader = kwargs.pop('downloader', downloader)
        self.downloader_tasks = []

        self.link_cleaners = kwargs.pop('cleaners', [])
        self.link_validators = kwargs.pop('validators', [])
        self.visitors = kwargs.pop('visitors', [])
        self.acceptors = kwargs.pop('acceptors', [])

        self.producers_quit = False
        self.consumers_quit = False
        self.quit = False
        self.history = set()
        self.init()

    def init(self, **kwargs):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
            filename=self.log_filepath,
            datefmt="%H:%M:%S",
            level=logging.INFO
        )

        self.output_path = os.path.abspath(pathlib.Path(self.output_path))

        self.loop = asyncio.get_event_loop()

        self.queue_urls: asyncio.Queue = asyncio.Queue()    # format: (link, contained_by)
        self.queue_down: asyncio.Queue = asyncio.Queue()
        self.queue: asyncio.Queue = asyncio.Queue()

        self.tasks = []
        self.input_task: asyncio.Task = self.loop.create_task(self.input_loop(self))

        for i in range(self.n_producers):
            task = self.loop.create_task(self.producer(str(i), self))
            self.tasks.append(task)
            self.producer_tasks.append(task)

        for i in range(self.n_downloaders):
            task = self.loop.create_task(self.downloader(str(i), self))
            self.tasks.append(task)
            self.downloader_tasks.append(task)

        for i in range(self.n_consumers):
            task = self.loop.create_task(self.consumer(str(i), self))
            self.tasks.append(task)
            self.consumer_tasks.append(task)

        self.tasks.append(self.loop.create_task(initial(self.inputs, self)))

    def start(self):
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            def handler():
                self.shutdown_task = self.loop.create_task(self.shutdown(s, self))
                return self.shutdown_task
            self.loop.add_signal_handler(s, handler)

        try:
            self.loop.run_forever()
        finally:
            self.loop.close()
            self.say("successfully shutdown the Crawly service.")

    def exit(self):
        self.quit = True
        pid = os.getpid()
        os.kill(pid, signal.SIGTERM)

    def say(self, *args, **kwargs):
        line = " ".join(args)
        logging.info(line, **kwargs)

    def gather_links(self, downloaded: Downloaded, parsed: Parsed):
        if downloaded.identity.is_parsable:
            for url in parsed.urls_images:
                yield url

            for url in parsed.urls_anchors:
                yield url

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

    def valid_link(self, link, in_url=''):
        if not link:
            return False
        for validator in self.link_validators:
            if not validator(link):
                return False
        return True

    def handle_parsing(self, downloaded: Downloaded, *args, **kwargs):
        self.say(f'Crawler/handle_parsing: begin')
        self.say(f'Crawler/handle_parsing: Parsing {downloaded.identity.url}')
        urls = []
        parsed = Parsed(downloaded.response, parser="lxml")

        def yield_all():
            for url in self.gather_links(downloaded, parsed):
                yield url

            for parsing_handler in self.parsing_handlers:
                for url in parsing_handler(downloaded, *args, **kwargs):
                    yield url

        for link in yield_all():
            link = self.clean_url(link, downloaded)
            if self.valid_link(link):
                urls.append(link)
                # yield link
        self.say(f'Crawler/handle_parsing: end')
        return urls

    def handle_download(self, downloaded):
        for handler in self.download_handlers:
            if not handler(downloaded, self):
                return False
        return True

    def accept(self, url: str, linked_by: str):
        for acceptor in self.acceptors:
            if not acceptor(url, linked_by, self):
                return False
        return True

    def visit(self, url: str, linked_by: str):
        for visitor in self.visitors:
            if not visitor(url, linked_by, self):
                return False
        return True

    @property
    def workers(self):
        for task in self.tasks:
            yield task

    def __str__(self):
        return f'Crawler:' \
               f' [inputs = {self.inputs}]' \
               f' [output_path = {self.output_path}]' \
               f' [sleep_time = {self.sleep_time}]' \
               f' [n_producers = {self.n_producers}]' \
               f' [n_consumers = {self.n_consumers}]'


async def input_loop(crawler: Crawler):
    crawler.say('input_loop: begin')
    while True:
        the_input: str = await aioconsole.ainput('> ')
        the_input = the_input.strip()
        print(f"input_loop: command = [{the_input}]")
        if the_input == "q" or the_input == "quit":
            print("Quit command received")
            break
    print('Exiting...')
    crawler.say('input_loop: exit')
    print("Bye")
    crawler.exit()


async def initial(init, crawler: Crawler):
    crawler.say(f'initial: [init = {init}] [crawler = {crawler}]')
    if isinstance(init, str):
        if init.startswith('.') or init.startswith('..') or init.startswith('/'):
            with open(os.path.abspath(pathlib.Path(crawler.inputs))) as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
                for line in lines:
                    if line[0] == "#":
                        continue
                    crawler.say(f'initial: [line = {line}]')
                    await crawler.queue_urls.put((line, ''))
                    crawler.say(f'initial: [line = {line}]: started!')

    else:
        try:
            it = iter(init)
            for i in it:
                await crawler.queue_urls.put((i, ''))
        except TypeError:
            pass
    crawler.say('initial: exit')


async def shutdown(the_signal: signal, crawler: Crawler):
    crawler.say(f'shutdown: begin: received exit from {the_signal.name}')

    crawler.quit = True

    tasks = [t for t in crawler.tasks if t is not
             crawler.shutdown_task]

    [task.cancel() for task in tasks]

    crawler.say(f"shutdown: cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    crawler.say(f"shutdown: flushing metrics")
    crawler.loop.stop()
    crawler.say(f"shutdown: exit")


async def producer(name, crawler: Crawler):
    crawler.say(f"producer: [name = {name}]: start")

    while not crawler.quit:
        crawler.say(f"producer: [name = {name}]: getting new url data")
        url, linked_by = await crawler.queue_urls.get()
        crawler.queue_urls.task_done()
        crawler.say(f"producer: [name = {name}]: getting new url data: done! [url = {url}] [linked_by = {linked_by}]")

        identity = Identified(url)
        crawler.say(f"producer: [name = {name}] [identity: {identity}]")

        crawler.say(f"producer: [name = {name}]: accepting [url = {url}] [linked_by = {linked_by}]")
        if crawler.accept(url, linked_by):
            crawler.say(f'producer: [name = {name}]: accept = True')
            crawler.say(f"producer: [name = {name}]: Putting {identity.url} for download")
            await crawler.queue_down.put((identity, linked_by))
            await asyncio.sleep(crawler.sleep_time)
            crawler.say(f"producer: [name = {name}]: Putting {identity.url} for download: done!")
        else:
            crawler.say(f'producer: [name = {name}]: accept = False!')
    crawler.say(f'producer [name = {name}]: exit')


async def downloader(name, crawler: Crawler):
    crawler.say(f'downloader: [name = {name}]: start')
    while not crawler.quit:
        crawler.say(f'downloader: [name = {name}]: Getting data')
        identity, linked_by = await crawler.queue_down.get()
        crawler.queue_down.task_done()
        crawler.say(f'downloader: [name = {name}]: Getting data: done! [identity = {identity}] [linked_by = {linked_by}]')
        await asyncio.sleep(crawler.sleep_time)

        crawler.say(f'downloader: [name = {name}]: handle_download')
        begin = datetime.datetime.now()
        download: Downloaded = Downloaded(identity, chunk_size=crawler.chunk_size)
        crawler.handle_download(download)

        end = datetime.datetime.now()
        time_delta = end - begin
        crawler.say(f'downloader: [name = {name}]: handle_download -> time_delta = {time_delta.total_seconds()}:{time_delta.microseconds}')

        if download.identity.is_parsable:
            crawler.say(f"downloader: [name = {name}]: Putting [{identity.url}] for parsing")
            await crawler.queue.put((download, linked_by))
            crawler.say(f"downloader: [name = {name}]: Putting [{identity.url}] for parsing: done!")
        else:
            crawler.say(f"downloader: [name = {name}]: Visiting [{identity.url}] [{linked_by}]")
            crawler.visit(identity.url, linked_by)

    crawler.say(f'downloader: [name = {name}]: exit')


async def consumer(name, crawler: Crawler):
    crawler.say(f"consumer: [name = {name}]: start")
    while not crawler.quit:
        download, linked_by = await crawler.queue.get()
        crawler.queue.task_done()
        await asyncio.sleep(crawler.sleep_time)

        new_links = crawler.handle_parsing(download)
        for new_link in new_links:
            await crawler.queue_urls.put((new_link, download.identity.url))

        crawler.visit(download.identity.url, linked_by)
    crawler.say(f'consumer: [name = {name}]: exit')
