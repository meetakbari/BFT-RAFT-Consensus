import json
import raft.server.utils as utils
from Crypto.Signature import PKCS1_PSS

config = None # global config variable! set in main.py


class Config:
    """Collect and merge CLI and file based config.
    This class is a singleton based on the Borg pattern.
    """
    __shared_state = {}
    def __init__(self, storageDir, cluster, nodeId, private_key, address, client_key, debug):
        """
        @param storageDir the directory to put consistent storage
        @param cluster a mapping of {(ipaddr, port) -> publicKey}
        @param nodeId the node id of this specific node
        @param private_key the private_key of this specific node
        @param address the address of this specific node (ip_addr, port)
        @param the client's public key
        """
        self.storageDir = storageDir
        clusterVerifiers = {k : PKCS1_PSS.new(pk) for k, pk in cluster.items()}
        self.cluster = clusterVerifiers # an array of ("addr", portNum, publicKey)
        self.nodeID = nodeId
        self.debug = debug
        self.private_key = PKCS1_PSS.new(private_key)
        self.address = address
        self.client_key = PKCS1_PSS.new(client_key)

    """ Return a new config that is based off of the json given in parameter"""
    def CreateConfig(cfg_filename, nodeId, debug=True):
        with open(cfg_filename) as f:    
            d = json.load(f)
        assert "StorageDir" in d
        assert "cluster" in d
        assert "keyDir" in d
        keyDir = d["keyDir"]
        n = d["cluster"]
        # get the public keys g
        public_keys = utils.importPublicKeys(keyDir, len(n))
        assert len(public_keys) == len(n)
        # get the private key
        if nodeId == -1:
            private_key = utils.importClientPrivateKey(keyDir)
        else:
            private_key = utils.importPrivateKey(keyDir, nodeId)
        client_key = utils.importClientPublicKey(keyDir)
        clusterMap = {}
        for i, l in enumerate(d["cluster"]):
            assert len(l) == 2
            clusterMap[(l[0], l[1])] = public_keys[i] # (addr, port) -> public key
        address = tuple(d["cluster"][nodeId])
        return Config(d["StorageDir"], clusterMap, nodeId, private_key, address, client_key, debug)
    
    """ Get the storage information for one node"""
    def getStorageLocation(self,index):
        assert index < len(self.cluster)
        return "%s/%d.storage" % (self.storageDir, index) # storageDir/{index}.storage

    def getMyStorage(self):
        return self.getStorageLocation(self.nodeID)
    
    def getMyClusterInfo(self):
        """ return the tuple (ip addr, port) for this node"""
        return self.address

    def getMyPrivateKey(self):
        return self.private_key