import asyncio
from ioctools.base import CallableIO


class Context:
    """ Holds the miscelanious data about [MethodIO]s """
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
class MethodIO(CallableIO):
    """This is a base class of all Applicant's methods that deals with
        abstracting details of the an ASyncIO call."""

    def __init__(self, io_f, context=None):
        self.io_f = io_f
        self.context = context