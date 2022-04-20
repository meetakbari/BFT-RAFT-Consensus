import asyncio
import os
import msgpack
import logging
from raft.server.states import Voter, Follower, Leader
import raft.server.config as cfg
from raft.server.utils import extended_msgpack_serializer

logger = logging.getLogger(__name__)


class Orchestrator():
    """The orchestrator manages the current node state,
    switching between Follower, Candidate and Leader when necessary.
    Only one Orchestrator """
    def __init__(self):
        os.makedirs(cfg.config.getMyStorage(), exist_ok=True)
        self.state = Voter(orchestrator=self)

    # def change_state(self, new_state):
    #     self.state.teardown()
    #     logger.info('State change:' + new_state.__name__)
    #     self.state = new_state(old_state=self.state)
    
    def change_follower(self):
        self.state.teardown()
        self.state = Follower(old_state = self.state)
    
    def change_voter(self):
        self.state.teardown()
        self.state = Voter(old_state = self.state)

    def change_leader(self, proof):
        self.state.teardown()
        self.state = Leader(old_state = self.state, proof=proof)


    def data_received_peer(self, sender, message):
        self.state.data_received_peer(sender, message)

    def data_received_client(self, transport, message):
        self.state.data_received_client(transport, message)

    def send(self, transport, message):
        transport.sendto(msgpack.packb(message, use_bin_type=True,
                         default=extended_msgpack_serializer))

    def send_peer(self, recipient, message):
        if recipient != self.state.volatile['address']:
            self.peer_transport.sendto(
                msgpack.packb(message, use_bin_type=True), tuple(recipient))

    def broadcast_peers(self, message):
        for recipient in self.state.volatile['cluster']:
            self.send_peer(recipient, message)