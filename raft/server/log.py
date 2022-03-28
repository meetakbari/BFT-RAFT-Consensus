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

class LogManager:
    """Instantiate and manage the components of the "Log" subsystem.
    That is: the log, the compactor and the state machine."""
    def __init__(self, compact_count=0, compact_term=None, compact_data={},
                 machine=DictStateMachine, address=None):
        erase_log = compact_count or compact_term or compact_data
        self.log = Log(erase_log)
        self.compacted = Compactor(compact_count, compact_term, compact_data)
        self.state_machine = machine(data=self.compacted.data,
                                     lastApplied=-1)
        self.commitIndex = len(self.log) - 1
        # TODO persist prepareIndex
        self.prepareIndex = self.commitIndex # nothing has yet been prepared. prepareIndex is inclusive
        self.state_machine.apply(self, self.commitIndex)
        self.address = address

    def __getitem__(self, index):
        """Get item or slice from the log, based on absolute log indexes.
        Item(s) already compacted cannot be requested."""
        if type(index) is slice:
            start = index.start - self.compacted.count if index.start else None
            stop = index.stop - self.compacted.count if index.stop else None
            return self.log[start:stop:index.step]
        elif type(index) is int:
            return self.log[index - self.compacted.count]

    @property
    def index(self):
        """Log tip index."""
        return len(self.log) - 1

    def term(self, index=None):
        """Return a term given a log index. If no index is passed, return
        log tip term."""
        if index is None:
            return self.term(self.index)
        elif index == -1:
            return 0
        elif not len(self.log) or index <= -1:
            return self.compacted.term
        else:
            return self[index]['term']
    def getHash(self, index):
        return self.log.getHash(index)


    # def pre_prepare_entries(self, entries, prevLogIndex):
    #     self.log.pre_prepare_entries(entries, prevLogIndex + 1)
    #     if entries:
    #         print('Pre-Prepare. New log: %s', self.log.data)
    
    def append_entries(self, entries, start):
        self.log.append_entries(entries, start)


    def commit(self, leaderCommit):
        assert self.commitIndex <= leaderCommit
        if leaderCommit > self.commitIndex:
            print(self.address, 'Advancing commit to %s' % leaderCommit)
        self.commitIndex = leaderCommit  # no overshoots
        
        # above is the actual commit operation, just incrementing the counter!
        # the state machine application could be asynchronous
        self.state_machine.apply(self, self.commitIndex)
        # self.compaction_timer_touch()
    
    def prepare(self, leaderPrepare):
        assert self.prepareIndex <= leaderPrepare
        if leaderPrepare > self.prepareIndex:
            print(self.address, 'Advancing prepare to %s' % leaderPrepare)    
        self.prepareIndex = leaderPrepare    

    def compact(self):
        del self.compaction_timer
        if self.commitIndex - self.compacted.count < 60:
            return
        not_compacted_log = self[self.state_machine.lastApplied + 1:]
        self.compacted.data = self.state_machine.data.copy()
        self.compacted.term = self.term(self.state_machine.lastApplied)
        self.compacted.count = self.state_machine.lastApplied + 1
        self.compacted.persist()
        self.log.replace(not_compacted_log)

    def compaction_timer_touch(self):
        """Periodically initiates compaction."""
        if not hasattr(self, 'compaction_timer'):
            loop = asyncio.get_event_loop()
            self.compaction_timer = loop.call_later(0.01, self.compact)