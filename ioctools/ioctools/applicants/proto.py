import asyncio
import typing

class CallableIO:
    """ Base class for every 
    applicant, Routine, Task, Future, Worker, Producer, Consumer, etc. in the library.
    Makes an ASyncIO callable object. """

    def prepare(self, *args, **kwargs) -> bool:
        """ Prepare the arguments for a call. """
        raise NotImplemented(f"Interface [ioctools / base.py].CallableIO.__call__")


    async def done(self):
        """ Signals done """
        pass

    async def __call__(self, *args, **kwargs):
        raise NotImplemented(
            f"Interface! [ioctools / base.py].CallableIO.__call__]")

        await self.done()


# This should go into another package
class Pause(CallableIO):
    """ Simple structure that keeps useful things about pauses. """
    def __init__(self, amount, unit="seconds"):
        self.amount = amount
        self.unit = unit

    def __str__(self):
        return f"[{self.amount} [unit={self.unit}]]"

    def __int__(self):
        if self.unit == 'minutes':
            return self.amount * 60
        elif self.unit == 'hours':
            return self.amount * 3600
        elif self.unit == 'days':
            return self.amount * 3600 * 24
        else:
            return self.amount

    async def __call__(self):
        await asyncio.sleep(int(self))

# This should go into another package.
class Context:
    """ Keeps all Applicant's results and arguments of a async call"""
    def __init__(self, 
            args=(), 
            kwargs={}, 
            returns=None,
            error=None,
            pause=None):

        self.args = args
        self.kwargs = kwargs
        self.returns = returns
        self.error = error

        #: [Self] / Consider! A Base class .
        self.pause = pause

# This should go into another package
class Method:
    """This is a base class of all Applicant's methods that deals with
        abstracting details of the call types into a single apply class."""

    def __init__(self, io_f, context=None):
        self.io_f = io_f
        self.context = context


# These should go into a module of their own
class Routine(Method):
    def __init__(self, io_, context, *args, **kwargs):
        super().__init__(io_, context)

    async def __call__(self, *args, **kwargs):
        self.context.returns = await self.io_f(
            *self.context.args, 
            **self.context.kwargs)


class Task(Method):
    def __init__(self, io_f, *args, **kwargs):
        super().__init__(io_f)
        self._task = asyncio.create_task(self.io_f(
            *self.context.args,
            **self.context.kwargs))    

    async def __call__(self, *args, **kwargs):
        self.context.returns = await asyncio.gather(*[self._task])


async def test_task_01():
    pass
#: ~.~

#: [$] / Go To! [:applicants.py]
DEFAULT_PAUSE = None # Pause(1, "seconds")


class Apply:
    """ Single (time) Routine runner """
    def __init__(self, method=None, context=None):
        self.method = method  
        if context:
            self.method.context = context

    def prepare(self, 
                What_: typing.Union[type, Method], 
                io_: typing.Union[typing.Callable],
                *args, 
                **kwargs):
        if 'context' in kwargs:
            context = kwargs.pop('context')
        else:
            context = Context()

        if 'pause' in kwargs:
            context.pause = kwargs.pop('pause')
        else:
            context.pause = DEFAULT_PAUSE

        print(f"Apply -> args: {args}")
        context.args = args
        context.kwargs = kwargs

        if isinstance(What_, type):
            self.method = What_(io_, context=context)
        elif isinstance(What_, Method):
            self.method = What_
        else:
            raise BadArgument(f"Method_ .IS: {Method_}")

    async def complete(self):
        if self.method.context.pause:
            await self.method.context.pause()
 

    async def __call__(self, What_: typing.Union[Method, type], io_f, *args, **kwargs):
        self.prepare(What_, io_f, *args, **kwargs)

        await self.method()  #: [$] / Self! Please; Provide
                        #:   [Implementation [Method.__await__()]] 
                        #:    . /dev/.bhdz/warn

        await self.complete()
        return self.method.context.returns


class ApplyMany(Apply):
    def __init__(self, count=1):
        super().__init__()
        self.count = count
        self.tasks = []

    def prepare(self, What_: Task, 
            ios_: typing.Container[Method], 
            a_args: typing.Container[typing.Container[typing.Any]], 
            a_kwargs: typing.Container[typing.Dict[typing.Any, typing.Any]]) -> bool:

        it_kwargs = iter(a_kwargs)
        it_args = iter(a_args)

        for io_f in ios_:
            try:
                kwargs = next(it_kwargs)
            except StopIteration:
                kwargs = {}
            
            try:
                args = next(it_args)
            except StopIteration:
                args = ()

            task = Task(io_f, *args, **kwargs)

            self.tasks.append(task)
        return True


    async def complete(self) -> typing.Any:
        self.context.returns = ret

        if self.context.pause:
            await self.context.pause()

        return ret

    def forget(self, ret):
        return ret

    def memorise(self, ret):
        self.context.returns = ret
        return ret

    async def __call__(self, What_, ios_: list, args: list, kwargs: list):
        if self.prepare():
            ret = await asyncio.gather(*self.tasks)
            await self.comlete()
            return self.forget(ret)
        else:
            raise Exception(f"Error!?")




        

#
#: [$] / Self! Please; Provide [proper] testing; for this [module:proto.py] 
#:  . /dev/.bhdz/warn
#: proper = + pytest? yes; pytest
#
def test0():
    print(f"Running test0::\n")

    async def main():
        async def pause(number, text):
            print(f"pause: (number={number}) (text={text}) -> begin:")
            await asyncio.sleep(5)
            print(f"pause: -> ends:")
            return 1

        apply = Apply()
        ret = await apply(Routine, pause, "hello", 5)
        print(f"pause -> {ret}")

        
        
        args = (
            (5, "hello",),
            (1, "world",),
            (23, "skidoo",),
        )

        #pause_routine = Routine(pause)
        pause_tasks = (
            Task(Routine(pause)),
            Task(Routine(pause)),
            Task(Routine(pause)),
        )

        ret = many_pauses(Task, Apply(Routine, pause_routine, "hello", 5)
    asyncio.run(main())

    print(f"\n::test0")


def test1():
    print(f"Running test1::")
    async def main():
        pass
    print(f"::test1")
    

if __name__ == "__main__":
    tests = [
        test0,
        test1,
    ]
    print(f"\tTest Case / 00::")
    for test in tests:
        print(f"\tTest!")
        test()
        print()

