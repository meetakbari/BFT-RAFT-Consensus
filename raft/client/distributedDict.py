import collections
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__),'../../'))
from raft.client.abstractClient import AbstractClient
from raft.client.refresh_policies import RefreshPolicyAlways
from raft.server.utils import importClientPrivateKey
from Crypto.Signature import PKCS1_PSS
from raft.server.config import Config
import random


# private key and clusterMap are verifiers
class DistributedDict(collections.UserDict, AbstractClient):
    """Client for raft instances with dictionary based state machines."""
    def __init__(self, addr, port, clusterMap, privateKey, append_retry_attempts=3,
                 refresh_policy=RefreshPolicyAlways()):
        # collections.UserDict().__init__(self)
        # AbstractClient.__init__(self, privateKey)
        super().__init__()
        if privateKey is not None:
            self.privateKey = privateKey
            
        
        self.data['cluster'] = set([(addr, port)])
        self.append_retry_attempts = append_retry_attempts
        self.publicKeyMap = clusterMap
        addrs = tuple(self.publicKeyMap.keys())
        self.data['cluster'] = set(self.publicKeyMap.keys())
        self.currentLeader = tuple(random.choice(addrs))
        self.refresh_policy = refresh_policy
        self.refresh(force=True)

    def __getitem__(self, key):
        self.refresh()
        return self.data[key]

    def __setitem__(self, key, value):
        self._append_log({'action': 'change', 'key': key, 'value': value})

    def __delitem__(self, key):
        self.refresh(force=True)
        del self.data[self.__keytransform__(key)]
        self._append_log({'action': 'delete', 'key': key})

    def __keytransform__(self, key):
        return key

    def __repr__(self):
        self.refresh()
        return super().__repr__()

    def refresh(self, force=False):
        if force or self.refresh_policy.can_update():
            self.data = self._get_state()

    def _append_log(self, payload):
        for attempt in range(self.append_retry_attempts):
            response = super()._append_log(payload)
            if response['success']:
                break
        # TODO: logging
        return response

def createClientDict(addr, port, config):
    config = Config.CreateConfig(config, -1, False)
    return DistributedDict(addr, port, config.cluster, config.private_key)

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        d = DistributedDict('127.0.0.1', 9111)
        d[sys.argv[1]] = sys.argv[2]
