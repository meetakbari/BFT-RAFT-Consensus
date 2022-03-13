import os
import json
import time
import collections
import asyncio
import logging
import msgpack
import math
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
from Crypto.Signature import PKCS1_PSS
import Crypto

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



# -------- KEY UTILS ------------
def createAndWriteKeys(directoryName, n):
    private_dir = "%s/private_keys" % directoryName
    public_dir = "%s/public_keys" % directoryName
    if not os.path.exists(private_dir):
        os.makedirs(private_dir)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)
    for i in range(n):
        key = RSA.generate(2048)
        privateKey = key.exportKey('PEM')
        publicKey = key.publickey().exportKey('PEM')
        privateKeyFileName = "%s/%d.pem" % (private_dir, i)
        publicKeyFileName = "%s/%d.pem" % (public_dir, i)
        f = open(privateKeyFileName, 'wb')
        f.write(privateKey)
        f.close()

        f = open(publicKeyFileName, 'wb')
        f.write(publicKey)
        f.close()
    
    # generate client keys
    key = RSA.generate(2048)
    privateKey = key.exportKey('PEM')
    publicKey = key.publickey().exportKey('PEM')
    privateKeyFileName = "%s/%d.pem" % (private_dir, "client_key")
    publicKeyFileName = "%s/%d.pem" % (public_dir, "client_key")
    f = open(privateKeyFileName, 'wb')
    f.write(privateKey)
    f.close()
    f = open(publicKeyFileName, 'wb')
    f.write(publicKey)
    f.close()
