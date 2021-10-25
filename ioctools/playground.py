import asyncio
from typing import Any, Union, Callable, Collection, Dict, Coroutine

import aiofiles
import os
import pathlib
import aiohttp
import random
from ioctools.www.handlers import TextHandler, ChunkHandler

from ioctools.base import Tasker, LoopIO, CallableIO, RoutineIO, SplitIO, ComposeIO
from ioctools.workers import ProducerIO, ConsumerIO, VectorIO
from ioctools.misc import InputDir, InputFile, ChunkReader
from ioctools.www.url import Url
from ioctools.www.sessions import SessionPool
from ioctools.www.html import Parser
from ioctools.www.clients import Client


async def say(what: str, who: str, whom: str) -> int:
    seconds = random.randrange(1, 5)
    await asyncio.sleep(seconds)
    print(f"{who} says '{what}' to {whom}")
    return seconds


class Say(RoutineIO):
    async def prepare(self, *args, **kwargs):
        print(f"Say.prepare: [args={args}] [kwargs={kwargs}]:")
        await super().prepare(*args, **kwargs)

    async def finalize(self):
        print(f"Say.finalize:")

    async def routine(self):
        what = self.args[0]
        who = self.args[1]
        whom = self.args[2]

        seconds = random.randrange(1, 5)
        await asyncio.sleep(seconds)
        print(f"{who} says '{what}' to '{whom}'")
        return seconds


async def test_task_01():
    ret = await Say()("hello", "me", "world")
    print(f"test_task_01: -> seconds_taken = {ret}: -> success:")


async def test_task_02():

    class SayerLoop(LoopIO):
        async def extract_args(self):
            words = [
                "Hi. How do you do?",
                "Ha-ha. Nice.",
                "Ho ho.",
                "Bye.",
            ]
            whoms = [
                "Jack",
                "John",
                "Sally",
                "Jane",
            ]

            sentence = random.choice(words)
            whom = random.choice(whoms)
            who = random.choice(whoms)
            return (sentence, who, whom), {}

        async def before(self):
            print(f"SayerLoop.before:")

        async def after(self, result):
            print(f"SayerLoop.after: [result={result}]")

    sayer_loop = SayerLoop(body=Say())
    ret = await sayer_loop()


async def test_01():
    class ISay(RoutineIO):
        async def routine(self):
            s = f"I say '{self.args[0]}'"
            print(s)
            return s

    class YouSay(RoutineIO):
        async def routine(self):
            s = f"You say '{self.args[0]}'"
            print(s)
            return s

    class TheySay(RoutineIO):
        async def routine(self):
            s = f"They say '{self.args[0]}'"
            print(s)
            return s

    sayers = SplitIO(ISay(), YouSay(), TheySay())

    sayings = [
        "Hello",
        "Welcome",
        "How do you do?"
    ]
    for saying in sayings:
        results = await sayers(saying)
        print(f"results: -> {results}")


async def test_02():
    class Sum(RoutineIO):
        async def routine(self):
            return sum(*self.args)

    class Double(RoutineIO):
        async def routine(self):
            return 2*self.args[0]

    class Square(RoutineIO):
        async def routine(self):
            return self.args[0] * self.args[0]

    formula = ComposeIO(Square(), Double(), Sum())
    numbers = [
        (1, 2),
        (2, 3),
        (1, 2, 3, 4, 5),
    ]
    for nums in numbers:
        results = await formula(nums)
        print(f"results: -> {results}")


async def test_client():
    method = 'get'

    urls = []
    kwargs = {}
    chunk_size = 256
    output_path = "./output"
    input_path = "./input"
    input_dir = InputDir(input_path)
    pool = SessionPool()
    queue1 = asyncio.Queue()
    queue2 = asyncio.Queue()
    text_handlers = [TextHandler()]
    binary_handlers = [ChunkHandler(chunk_size)]
    client = Client(pool, queue1, queue2, chunk_size, text_handlers, binary_handlers)

    async for filename in input_dir("*.urls"): # or Interpreter! App 99 {{ Of LSD? List; String; Dictionary }}
        input_file = InputFile(filename)
        async for line in input_file.readlines(): # or db { of your? << choice >> }
            print(f"line: {line}")
            urls.append(Url(line))

    for url in urls:
        await queue1.put(url)

    while not queue1.empty():
        url = await queue1.get()
        links, url = await client.request(method, url, **kwargs)
        for link in links:
            await queue1.put(link)

    print()
    print(f"checking queue2")
    while not queue2.empty():
        url = await queue2.get()
        links = '[' + '; '.join(str(link) for link in url.linked_by) + ']'
        print(f"Url: {str(url)} <- {links}")
    try:
        pass
    except asyncio.CancelledError:
        pass


async def test_producers():
    class Generate(RoutineIO):
        async def routine(self):
            number = random.randrange(1, 6, 1)
            return number

    class Formula(RoutineIO):
        async def routine(self):
            return 10 + self.args[0]

    class Print(RoutineIO):
        def __init__(self, queue_loop):
            super().__init__()
            self.queue_loop = queue_loop

        async def routine(self):
            number = self.args[0]
            print(f"number: {number}")
            await self.queue_loop.put(number)

    queue1 = asyncio.Queue()
    queue2 = asyncio.Queue()

    producers_n = 5
    consumers_n = 5
    processors_n = 5

    pool = []
    for _ in range(producers_n):
        pool.append(ProducerIO(queue1, Generate()))

    for _ in range(processors_n):
        pool.append(VectorIO(queue1, queue2, Formula()))

    for _ in range(consumers_n):
        pool.append(ConsumerIO(queue2, Print(queue1)))

    tasks = [Tasker(routine)() for routine in pool]

    try:
        results = await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print(f"CANCELLED")

        print(f"PRINTING: queue1:")
        items = []
        while not queue1.empty():
            items.append(queue1.get_nowait())
        print(f" items: {items}")

        print()
        print(f"PRINTING: queue2:")
        items = []
        while not queue2.empty():
            items.append(queue2.get_nowait())
        print(f" items: {items}")


async def test_crawler():
    method = 'get'

    urls = []
    kwargs = {}
    chunk_size = 256
    output_path = "./output"
    input_path = "./input"
    input_dir = InputDir(input_path)
    pool = SessionPool()
    queue1 = asyncio.Queue()
    queue2 = asyncio.Queue()
    text_handlers = [TextHandler()]
    binary_handlers = [ChunkHandler(chunk_size)]
    client = Client(pool, queue1, queue2, chunk_size, text_handlers, binary_handlers)

    # Start up
    async for filename in input_dir("*.urls"):
        input_file = InputFile(filename)
        async for line in input_file.readlines():
            print(f"line: {line}")
            urls.append(Url(line))

    for url in urls:
        await queue1.put(url)

    class Producer(RoutineIO):
        """ Makes requests """
        async def routine(self):
            url = await queue1.get()
            links, url = await client.request(method, url, **kwargs)
            for link in links:
                await queue1.put(link)

    class Formula(RoutineIO):
        async def routine(self):
            return 10 + self.args[0]

    class Consumer(RoutineIO):
        def __init__(self, queue_loop):
            super().__init__()
            self.queue_loop = queue_loop

        async def routine(self):
            number = self.args[0]
            print(f"number: {number}")
            await self.queue_loop.put(number)

    queue1 = asyncio.Queue()
    queue2 = asyncio.Queue()

    producers_n = 5
    consumers_n = 5
    processors_n = 5

    pool = []
    for _ in range(producers_n):
        pool.append(ProducerIO(queue1, Generate()))

    for _ in range(processors_n):
        pool.append(VectorIO(queue1, queue2, Formula()))

    for _ in range(consumers_n):
        pool.append(ConsumerIO(queue2, Process(queue1)))

    tasks = [Tasker(routine)() for routine in pool]

    try:
        results = await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print(f"CANCELLED")

        print(f"PRINTING: queue1:")
        items = []
        while not queue1.empty():
            items.append(queue1.get_nowait())
        print(f" items: {items}")

        print()
        print(f"PRINTING: queue2:")
        items = []
        while not queue2.empty():
            items.append(queue2.get_nowait())
        print(f" items: {items}")


if __name__ == "__main__":
    from signal import SIGINT, SIGTERM

    async def shutdown():
        print(f"Shutting down...")

    async def main():
        try:
            # await test_callable()
            # await test_task_01()
            # await test_task_02()
            # await test_01()
            # await test_02()
            # await test_producers()
            await test_client()
        except asyncio.CancelledError:
            await shutdown()

    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(main())
    for signal in [SIGINT, SIGTERM]:
        loop.add_signal_handler(signal, main_task.cancel)
    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()
