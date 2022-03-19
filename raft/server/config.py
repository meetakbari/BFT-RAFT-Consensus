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