import os
import msgpack
import collections
import asyncio
import logging
import raft.server.config as cfg
from Crypto.Signature import PKCS1_PSS
from Crypto.Hash import SHA256
from raft.server import utils

logger = logging.getLogger(__name__)


class Log(collections.UserList):
    def __init__(self, erase_log=False):
        super().__init__()
        self.path = os.path.join(cfg.config.getMyStorage(), 'log')
        #  load
        # logger.debug('Initializing log')
        if erase_log and os.path.isfile(self.path):
            os.remove(self.path)
            logger.debug('Using parameters')
        elif os.path.isfile(self.path):
            self.data = utils.msgpack_appendable_unpack(self.path)
            logger.debug('Using persisted data')

    def findEntry(self, data):
        for i, entry in enumerate(self.data):
            if entry['data'] == self.data: return i
        return -1

    def getHash(self, index):
        return utils.getLogHash(self.data, index)

    
    def append_entries(self, entries, start):
        """
        Overwrite entries in log, from start to end inclusive
        if only one entry, start = end
        """
        # if (!utils.validateEntries(entries)) return
        entries = list(entries)
        if len(self.data) >= start:
            old_index = len(self.data)
            self.replace(self.data[:start] + entries)
            new_index = len(self.data)
            if new_index > old_index:
                entryVals = [{'data': entry['data'], 'term' : entry['term']} for entry in self.data]
                print("Appending entries to log. New log is :", entryVals)

        else:
            self.data += entries
            utils.msgpack_appendable_pack(entries, self.path)

    def replace(self, new_data):
        if os.path.isfile(self.path):
            os.remove(self.path)
        self.data = new_data
        utils.msgpack_appendable_pack(self.data, self.path)

class Compactor():
    def __init__(self, count=0, term=None, data={}):
        self.count = count
        self.term = term
        self.data = data
        self.path = os.path.join(cfg.config.getMyStorage(), 'compact')
        #  load
        # logger.debug('Initializing compactor')
        if count or term or data:
            self.persist()
            # logger.debug('Using parameters')
        elif os.path.isfile(self.path):
            with open(self.path, 'rb') as f:
                self.__dict__.update(msgpack.unpack(f, encoding='utf-8'))
            logger.debug('Using persisted data')

    @property
    def index(self):
        return self.count - 1

    def persist(self):
        with open(self.path, 'wb+') as f:
            raw = {'count': self.count, 'term': self.term, 'data': self.data}
            msgpack.pack(raw, f, use_bin_type=True)


class DictStateMachine(collections.UserDict):
    def __init__(self, data={}, lastApplied=0):
        super().__init__(data)
        self.lastApplied = lastApplied

    def apply(self, items, end):
        items = items[self.lastApplied + 1:end + 1]
        for item in items:
            self.lastApplied += 1
            item = item['data']
            if item['action'] == 'change':
                self.data[item['key']] = item['value']
            elif item['action'] == 'delete':
                del self.data[item['key']]
