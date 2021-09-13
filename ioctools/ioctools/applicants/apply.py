
import asyncio


class Pause:
    def __init__(self, span=1):
        self.span = span

class Method:
    pass

class One(Method):
    def __init__(self, *args, **kwargs):
        pass

    async def __call__(self, *args, **kwargs):
        pass



class Many(Method):
    pass


class Apply:
    def __init__(self, io_f):
        self.io_f = io_f

    def __call__(self, Method: type, *args, **kwargs):
        method = Method(self.io_f, pause=)
        return method(self.io_f)




