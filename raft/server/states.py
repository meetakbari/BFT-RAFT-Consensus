import asyncio
import logging
import statistics
from random import randrange
from os.path import join
from raft.server.utils import PersistentDict, TallyCounter
from raft.server.log import LogManager
from raft.server.utils import get_kth_largest, get_quorum_size, validate_entries, signDict, validateDict, getLogHash, validateIndex
import raft.server.config as cfg
import Crypto
logger = logging.getLogger(__name__)


class State:
    """Abstract state for subclassing."""
    def __init__(self, old_state=None, orchestrator=None):
        """State is initialized passing an orchestrator instance when first
        deployed. Subsequent state changes use the old_state parameter to
        preserve the environment.
        """
        Crypto.Random.atfork()
        if old_state:
            self.orchestrator = old_state.orchestrator
            self.persist = old_state.persist
            self.volatile = old_state.volatile
            self.log = old_state.log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(cfg.config.getMyStorage(), 'state'),
                                          {'votedFor': None, 'currentTerm': -1})
            self.volatile = {'leaderId': None, 'cluster': cfg.config.cluster.keys(),
                             'publicKeyMap': cfg.config.cluster.copy(), # map from addresses to the verifiers
                             'address': cfg.config.getMyClusterInfo(),
                             'privateKey': cfg.config.getMyPrivateKey(), # an actual signer
                             'clientKey': cfg.config.client_key}
            self.election_timer = None
            self.log = LogManager(address=self.volatile['address'])
            self._update_cluster()
        self.term_change_messages = {} # Map of term -> term change messages for that term (for terms for which you are a valid leader)
        self.stats = TallyCounter(['read', 'write', 'append'])

    def getLeaderForTerm(self, term):
        index = term % len(self.volatile['cluster'])
        return list(self.volatile['cluster'])[index]

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        logger.debug('Received %s from %s', msg['type'], peer)

        method = getattr(self, 'on_peer_' + msg['type'], None)
        if method:
            method(peer, msg)
        else:
            logger.info('Unrecognized message from %s: %s', peer, msg)

    def data_received_client(self, protocol, msg):
        """Receive client messages from orchestrator and pass them to the
        appropriate method."""
        method = getattr(self, 'on_client_' + msg['type'], None)
        if method:
            method(protocol, msg)
        else:
            logger.info('Unrecognized message from %s: %s',
                        protocol.transport.get_extra_info('peername'), msg)