import copy
import os
import asyncio
import shutil
import random
import string
import logging
from multiprocessing import Process
from raft.server.main import setup
from raft.server.config import Config
from Crypto.PublicKey import RSA


class Pool:
    
    def __init__(self, num_servers, keys, clientKey):
        # if type(server_ids) is int:
        #     server_ids = range(server_ids)
        self._generate_configs(num_servers, keys, clientKey)
        self.servers = {}
        self.server_ids = [i for i in range(num_servers)]
        for c in self.configs:
            print('Generating server', c.nodeID)
            self.servers[c.nodeID] = (Process(target=self._run_server,
                                                       args=(c,)))

    def start(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            print('Starting server', x)
            self.servers[x].start()

    def stop(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            print('Stopping server', x)
            if self.running[x]:
                self.servers[x].terminate()
                self.servers[x] = Process(target=self._run_server,
                                          args=(self.configs[x],))

    def rm(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            shutil.rmtree(self.configs[x].getMyStorage())
            print('Removing files related to server', x)

    @property
    def running(self):
        return {k: v.is_alive() for (k, v) in self.servers.items()}

    @property
    def ids(self):
        return self.server_ids.copy()

    def _generate_configs(self, numIds, keys, clientKey):
        """ keys are the node private keys """
        storage_dir = "%s/persistStorage" % os.path.abspath(os.path.dirname(__file__))
        # keys = [RSA.generate(2048) for i in range(numIds)]

        clusterAddresses = [("127.0.0.1", 9110 + i) for i in range(numIds)] # [(ip_addr, port)]
        clusterMap = {k:keys[i].publickey() for i,k in enumerate(clusterAddresses)} #[(ip_addr, port) -> public key]
        self.configs = [Config(storage_dir, clusterMap, i, keys[i], clusterAddresses[i], clientKey, True) for i in range(numIds)]
        #self.configs = [Config(storage_dir, cluster_vals, server_id, True) for server_id in range(numIds)]

    def _run_server(self, config):
        setup(config)
        raft_logger = logging.getLogger('raft')
        raft_logger.setLevel(logging.CRITICAL)
        loop = asyncio.get_event_loop()
        loop.run_forever()


def get_random_string(lenght=12, allowed_chars=None):
    random_gen = random.SystemRandom()
    if allowed_chars is None:
        allowed_chars = string.ascii_letters + string.digits
    return ''.join([random_gen.choice(allowed_chars) for _ in range(lenght)])

def getPublicAndPrivateKeysForTestNodes(n):
    return [RSA.generate(2048) for i in range(numIds)]