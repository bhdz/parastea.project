import asyncio
from typing import Any, Union, Collection, Dict, Coroutine


class Callable:
    def __call__(self, *args, **kwargs):
        pass


class Context:
    """ Context for every RoutineIO call """

    def __init__(self, args=(), kwargs={}, result=None, error=None):
        self.args = args
        self.kwargs = kwargs
        self.result = result
        self.error = error


class Tasker(Callable):
    """ Base class that wraps a coroutine, a call context, and contains a asyncio.Task object """

    def __init__(self, routine, context: Context = Context()):
        self.routine = routine
        self.subject = None
        self.context = context

    def __call__(self, *args, **kwargs):
        self.context.args = args
        self.context.kwargs = kwargs
        self.subject = asyncio.create_task(self.routine(*args, **kwargs))
        if not self.subject:
            raise Exception(f"Error! [asyncio.create_task] returned None")
        return self.subject

    def __await__(self):
        result = self.__call__(
            *self.context.args,
            **self.context.kwargs,
        ).__await__()

        self.context.result = result
        return result

    @classmethod
    async def gather(cls, *aws, loop=None, return_exceptions=False):
        translated = [
            node.subject if isinstance(node, cls) else node for node in aws
        ]
        return await asyncio.gather(*translated, loop, return_exceptions)


class CallableIO:
    """ Base for every RoutineIO, TaskIO, etc. utility in the library."""

    def __init__(self):
        pass

    async def __call__(self, *args, **kwargs):
        raise NotImplementedError(f"Interface! [ioctools / base.py].CallableIO")


class RoutineIO(CallableIO):
    def __init__(self, context=Context(), coro=None, pause=None):
        super().__init__()
        self.coro = coro
        self.context = context
        self.pause = pause

    @property
    def args(self):
        return self.context.args

    @property
    def kwargs(self):
        return self.context.kwargs

    @property
    def result(self):
        return self.context.result

    @property
    def error(self):
        return self.context.error

    async def prepare(self, *args, **kwargs):
        self.context.args = args
        self.context.kwargs = kwargs
        self.context.result = None
        self.context.error = None

    async def finalize(self):
        if self.pause:
            await asyncio.sleep(self.pause)

    async def routine(self):
        raise NotImplementedError(f"Interface! [ioctools / base.py].RoutineIO.routine")

    async def __call__(self, *args, **kwargs):
        await self.prepare(*args, **kwargs)

        if self.coro:
            self.context.returns = await self.coro(*self.context.args, **self.context.kwargs)
        else:
            self.context.returns = await self.routine()
        await self.finalize()

        return self.context.returns

    def __await__(self, *args, **kwargs):
        return self.__call__(*self.context.args, **self.context.kwargs).__await__()


class SplitIO(RoutineIO):
    """ Passes [self.args] and [self.kwargs] to many routines """

    def __init__(self, *sub_routines):
        self.sub_routines = sub_routines
        super().__init__()

    async def routine(self):
        tasks = []
        for sub in self.sub_routines:
            tasker = Task(routine=sub)
            tasks.append(tasker(*self.args, **self.kwargs))
        return await asyncio.gather(*tasks)


class ComposeIO(RoutineIO):
    """ Takes [n] Routines and chains them 
    into a composition call with the last one taking 
    the actual arguments. """

    def __init__(self, *sub_routines):
        self.sub_routines = sub_routines
        super().__init__()

    async def routine(self):
        it = iter(reversed(self.sub_routines))
        f = next(it)

        f_tasker = Task(routine=f)
        result = await asyncio.gather(f_tasker(*self.args,
                                               **self.kwargs))

        for f in it:
            f_tasker = Task(routine=f)
            result = await asyncio.gather(f_tasker(result[0]))
        return result


class LoopIO(RoutineIO):
    """ Abstracts ASyncIO parallel loops."""

    def __init__(self, body: Union[Callable, RoutineIO] = RoutineIO()):
        super().__init__()
        self.to_quit = False
        self.body = body

    async def before(self):
        """ Called before the [self.body] call """
        return True

    async def after(self, result):
        """ Called after the [self.body] call """
        return True

    async def generate_args(self) -> Collection:
        """ Implement this method to extract arguments for the [self.body] routine """
        return (), {}

    async def routine(self):
        while not self.to_quit:
            if await self.before():
                args, kwargs = await self.generate_args()
                tasker = Tasker(routine=self.body)
                task = tasker(*args, **kwargs)
                results = await asyncio.gather(task)
                if not await self.after(results[0]):
                    self.to_quit = True
