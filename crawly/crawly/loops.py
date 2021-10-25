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


def exit(self):
    self.quit = True
    pid = os.getpid()
    os.kill(pid, signal.SIGTERM)


class Controller(object):
    pass


class Loop(object):
    def __init__(self, **kwargs):
        pass

    def looping(self):
        pass


class Producers(Loop):
    DEF_N_TASKS = 2

    def __init__(self, **kwargs):
        self.context = kwargs.pop('context', {})

        self.loop = kwargs.pop('loop', asyncio.get_event_loop())

        self.transformer = kwargs.pop('transformer', None)
        self.asyncf = kwargs.pop('asyncf', None)
        self.n_tasks = kwargs.pop('n_tasks', Producers.DEF_N_TASKS)
        self.queue_in = kwargs.pop('queue_in', asyncio.Queue())
        self.queue_out = kwargs.pop('queue_out', asyncio.Queue())

        self.tasks = []
        self.quit: asyncio.Event = kwargs.get('quit_event', asyncio.Event())
        self.acceptors = kwargs.get('acceptors', [])

        super().__init__(**kwargs)

        self.init()

    @property
    def sleep_time(self):
        return self.context.get('sleep_time', 0.0)

    def init(self, **kwargs):
        self.tasks.clear()
        if not self.queue_in:
            self.queue_in: asyncio.Queue = asyncio.Queue()

        if not self.queue_out:
            self.queue_in: asyncio.Queue = asyncio.Queue()

        for i in range(self.n_tasks):
            task = self.loop.create_task(self.asyncf(str(i), self, **kwargs))
            self.tasks.append(task)

    def accept(self, *args, **kwargs):
        for acceptor in self.acceptors:
            if not acceptor(*args, **kwargs):
                return False
        return True

    async def looping(self, *args, **kwargs):
        while not self.quit.is_set():
            item = await self.queue_in.get()
            self.queue_in.task_done()

            if self.accept(item):
                transformed = self.transformer(item, *args, **kwargs)
                # crawler.say(f"producer: [name = {name}] [identity: {identity}]")

                await self.queue_out.put(transformed)
                await asyncio.sleep(self.sleep_time)

                self.visit(item)
                # await asyncio.sleep(crawler.sleep_time)
            await asyncio.sleep(self.sleep_time)



class Consumers(object):
    def __init__(self):
        pass


    def loop(self):
        pass


async def basic_producer(name: str, quit: asyncio.Event, **kwargs):
    pass


async def advanced_loop(name: str, context):
    await asyncio.sleep(context['sleep_time'])


if __name__ == "__main__":
    p1 = Producers(function=basic_producer)
