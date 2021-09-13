
import asyncio
import typing


# This should go into another package
class Pause:
    """ Simple structure that keeps useful things about pauses. """
    def __init__(self, amount, unit="seconds"):
        self.amount = amount
        self.unit = unit

    def __str__(self):
        return f"[{self.amount} [unit={self.unit}]]"


# This should go into another package
class Method:
    """This is a base class of all Applicant's methods that deals with
        abstracting details of the call types into a single apply class."""

    def __await__(self):
        raise NotImplemented(f"Interface? Yes.")

    def __init__(self, io_f, context=None):
        self.io_f = io_f
        self.context = context

    async def __call__(self, *args, **kwargs):
        #: [$] / Self! Do It! [Errors & Exceptions] -> [into another class] 
        #:  . /dev/stdnull !? Shh... be qwui'yeet \:-)
        self.context.returns = await self.io_f(
                *self.context.args,
                **self.context.kwargs,
            )

# This should go into another package.
class Context:
    """ Keeps all Applicant's results and arguments of a async call"""
    def __init__(self, 
            args, 
            kwargs, 
            returns=None, 
            pause=Pause(1)):

        self.args = args
        self.kwargs = kwargs
        self.returns = returns
        self.error = None

        #: [Self] / Consider! A Base class .
        self.pause = pause


# These should go into a module of their own
class One(Method):
    def __init__(self, io_f, *args, **kwargs):
        super().__init__(io_f)
    async def __call__(self, *args, **kwargs):
        return await self.io_f(*args, **kwargs)


class Many(Method):
    def __init__(self, io_f, *args, **kwargs):
        super().__init__(io_f)

    async def __call__(self, *args, **kwargs):
        pass
#: ~.~

#: [$] / Go To! [:applicants.py]
DEFAULT_PAUSE = Pause(1, "seconds")


class Apply:
    def __init__(self, io_f):
        self.io_f = io_f

    async def __call__(self, Method_: typing.Union[Method, type], *args, **kwargs):
        context = Context(*args, **kwargs)
        context.args = args
        context.kwargs = kwargs

        if isinstance(Method_, type): #: Construct! For? Convenience .
            context.pause = DEFAULT_PAUSE
            method = Method_(self.io_f, context=context)
        elif isinstance(Method_, Method):
            method = Method_
        else:
            raise BadArgument(f"Method_ .IS: {Method_}")

        method.context = context
        
        await method()  #: [$] / Self! Please; Provide
                        #:   [Implementation [Method.__await__()]] 
                        #:    . /dev/.bhdz/warn

        return method.context.returns


#
#: [$] / Self! Please; Provide [proper] testing; for this [module:proto.py] 
#:  . /dev/.bhdz/warn
#: proper = + pytest? yes; pytest
#
def test0():
    print(f"Running test0::")
    async def main():
        pass
    print(f"::test0")


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

