import json
import os
from python_json_config import ConfigBuilder


class Builder(object):
    def __init__(self, *args, **kwargs):
        self.path = kwargs.pop('path', './config.json')

    def parse(self, path):
        config = Config()
        self.path = path
        with open(os.path.abspath(path)) as json:
            data = json.load(json)
            config = Config()
        return config


class Config(dict):
    def __init__(self, *args, **kwargs):
        if len(args) > 0:
            self._value = args[0]
        else:
            self._value = None
        super().__init__(**kwargs)

    def __getattr__(self, item):
        print(f"Config.__getattr__: {item}")
        if item.startswith('__'):
            return self.__dict__[item]
        elif item.startswith('_'):
            return self.__dict__[item]
        else:
            return dict.__getitem__(self, item)

    def __setattr__(self, item, value):
        print(f"Config.__setattr__: [{item} = {value}]")
        if item.startswith('__'):
            self.__dict__[item] = value
        elif item.startswith('_'):
            self.__dict__[item] = value
        else:
            dict.__setitem__(self, item, value)

    def __str__(self):
        for key, value in super().items():
            pass
        return f"Config: " + super().__str__()


config = Config()
print(f"config: {config._value}", end="\n\n")

config.test = Config(1)
print(f"config/test: {config.test._value}", end="\n\n")

config.test.one = Config('TEST ME')
print(f"config/test/one: {config.test.one._value}", end="\n\n")

config.test.two = Config('How to test me')
print(f"config/test/two: {config.test.two._value}", end="\n\n")

config.test.two.one = Config('Hello')
print(f"config/test/two/one: {config.test.two.one._value}", end="\n\n")

exit(0)

builder = ConfigBuilder()
config = builder.parse('../config.json')

print(f"config/crawler/chunk_size: {config.data['crawler']['chunk_size']}")
print(f"config/crawler/sleep_time: {config.data['crawler']['chunk_size']}")