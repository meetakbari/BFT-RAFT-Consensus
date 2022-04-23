import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__),'../../')) # set python path

import asyncio
import logging
import argparse
import raft.server.config as cfg
from raft.server.config import Config
from raft.server.protocols import Orchestrator, PeerProtocol, ClientProtocol
from raft.server.logger import start_logger

def setup(config):
    """Setup a node."""
    cfg.config = config
    start_logger()
    logger = logging.getLogger(__name__)

    loop = asyncio.get_event_loop()
    orchestrator = Orchestrator()
    addrTuple = cfg.config.getMyClusterInfo() #(addr, port)
    print ("Setting up", addrTuple)
    coro = loop.create_datagram_endpoint(lambda: PeerProtocol(orchestrator),
                                         local_addr=addrTuple)
    transport, _ = loop.run_until_complete(coro)
    orchestrator.peer_transport = transport

    coro = loop.create_server(lambda: ClientProtocol(orchestrator), *addrTuple)
    server = loop.run_until_complete(coro)

    logger.info('Serving on %s', addrTuple)
    return server