import asyncio
from typing import Any, Union, Callable, Collection, Dict, Coroutine
from ioctools.base import Tasker, LoopIO, CallableIO, RoutineIO, SplitIO, ComposeIO


# These two are the beginning points and ending points in your application scheme
class ProducerIO(LoopIO):
    """ Feeds a queue with data out of [self.generate] RoutineIO """
    def __init__(self, queue: asyncio.Queue = asyncio.Queue(), generate=RoutineIO()):
        super().__init__(body=generate)
        self.queue = queue
        
    async def after(self, result):
        await self.queue.put(result)
        return True


class ConsumerIO(LoopIO):
    """ Consumes an item from the queue. Check out [ProducerIO] """
    def __init__(self, queue: asyncio.Queue = asyncio.Queue(), consume=RoutineIO()):
        super().__init__(body=consume)
        self.item = None
        self.queue = queue

    async def generate_args(self):
        """ """
        return (self.item,), {}

    async def before(self):
        """ Add try/catch, dear user."""
        self.item = await self.queue.get()
        return True


class VectorIO(LoopIO):
    def __init__(self,
                 queue_in: asyncio.Queue = asyncio.Queue(),
                 queue_out: asyncio.Queue = asyncio.Queue(),
                 process=RoutineIO()):
        super().__init__(body=process)
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.item = None

    async def before(self):
        try:
            self.item = self.queue_in.get_nowait()
        except asyncio.QueueEmpty:
            self.item = 0
        return True

    async def generate_args(self):
        return (self.item,), {}

    async def after(self, result):

        await self.queue_out.put(result)
        self.item = None
        return True


class Worker:
    def __init__(self, count, routine=RoutineIO()):
        self.routine = routine
        self.taskers = []
        self.count = count

        for _ in range(self.count):
            self.taskers = Tasker(routine=routine)


class TaskPool:
    def __init__(self):
        self.tasks = []

    async def append(self, worker, *args, **kwargs):
        for tasker in worker.taskers:
            self.tasks.append(tasker(*args, **kwargs))