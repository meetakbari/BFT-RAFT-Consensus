import os
import json
import time
import collections

class PersistentDict(collections.UserDict):
    """Dictionary data structure that is automatically persisted to disk
    as json."""
    def __init__(self, path=None, data={}):
        if os.path.isfile(path):
            with open(path, 'r') as f:
                data = json.loads(f.read())
        self.path = path
        super().__init__(data)

    def __setitem__(self, key, value):
        self.data[self.__keytransform__(key)] = value
        self.persist()

    def __delitem__(self, key):
        del self.data[self.__keytransform__(key)]
        self.persist()

    def __keytransform__(self, key):
        return key

    def persist(self):
        with open(self.path, 'w+') as f:
            f.write(json.dumps(self.data))