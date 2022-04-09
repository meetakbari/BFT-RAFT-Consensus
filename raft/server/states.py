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

    def on_client_append(self, protocol, msg):
        """Redirect client to leader upon receiving a client_append message."""
        entryIndex = self.log.log.findEntry(msg['data'])
        if entryIndex != -1:
            protocol.send({'type': 'result', 'success': True, 'proof':self.proofOfCommit, 'log':tuple(self.log.log.data), 'index': entryIndex})
            print("Found entry immediately so just sending it back")
            return

        if hasattr(self, "election_timer") and self.election_timer is None:
            self.start_election_timer()

        msg = {'type': 'redirect',
            'leader': self.volatile['leaderId']}
        protocol.send(msg)
        logger.debug('Redirect client %s:%s to leader',
                    *protocol.transport.get_extra_info('peername'))

    def on_client_config(self, protocol, msg):
        """Redirect client to leader upon receiving a client_config message."""
        return self.on_client_append(protocol, msg)

    def on_client_get(self, protocol, msg):
        """Return state machine to client."""
        state_machine = self.log.state_machine.data.copy()
        self.stats.increment('read')
        protocol.send(state_machine)

    def on_client_diagnostic(self, protocol, msg):
        """Return internal state to client."""
        msg = {'status': self.__class__.__name__,
               'persist': {'votedFor': self.persist['votedFor'],
                           'currentTerm': self.persist['currentTerm']},
               'volatile': self.volatile,
               'log': {'commitIndex': self.log.commitIndex},
               'stats': self.stats.data}
        msg['volatile']['cluster'] = list(msg['volatile']['cluster'])

        if type(self) is Leader:
            msg.update({'leaderStatus':
                        {'netIndex': tuple(self.nextIndex.items()),
                         'prePrepareIndex': tuple(self.prePrepareIndexMap.items()),
                         'waiting_clients': {k: len(v) for (k, v) in
                                             self.waiting_clients.items()}}})
        protocol.send(msg)

    def _update_cluster(self, entries=None):
        """Scans compacted log and log, looking for the latest cluster
        configuration."""
        return

    def on_peer_term_change(self, peer, msg):

        # validate that the peer actually send this message
        proposedTerm = msg['term']

        if (not validateDict(msg, self.volatile['publicKeyMap'][peer])):
            return

        # should this message be for us
        if self.getLeaderForTerm(proposedTerm) != self.volatile['address']:
            return
        
        # this person has committed entries we don't even know about - we can't be leader for this term
        if msg['commitIndex'] > self.log.index:
            return

        # validate commit point and prepare point on incoming message
        hypothetical_new_log = self.log.log.data[:(msg['commitIndex'] + 1)] + list(msg['logAfterCommit'])
        commitIndex = msg['commitIndex']
        prepareIndex = commitIndex + len(msg['logAfterCommit'])

        calcPrepare = prepareIndex
        calcCommit = commitIndex
        if (msg['proof'] is not None):
            calcPrepare, calcCommit = validateIndex(hypothetical_new_log, msg['proof'], self.volatile['publicKeyMap'], len(self.volatile['cluster']))
        if calcPrepare == -2 or not calcPrepare == prepareIndex or not calcCommit == commitIndex:
            return
        
        if commitIndex > self.log.commitIndex:
            self.log.commit(commitIndex)
            
        if proposedTerm not in self.term_change_messages:
            self.term_change_messages[proposedTerm] = {}

        self.term_change_messages[proposedTerm][peer] = msg
            
        quorum_size = get_quorum_size(len(self.volatile['cluster'])) - 1
        if len(self.term_change_messages[proposedTerm]) == quorum_size:
            self.send_new_term(proposedTerm)

    def send_new_term(self, proposedTerm):
        peer_messages = [tuple(i) for i in self.term_change_messages[proposedTerm].items()]
        messages = [pm[1] for pm in peer_messages]
        extra_log_entries = []
        latest_term = -1
        longest = -1
        latestProof = None
        for msg in messages:
            logAfterCommit = list(msg['logAfterCommit'])
            if len(logAfterCommit) > 0:
                lastTerm = logAfterCommit[len(logAfterCommit) - 1].term
                if lastTerm > latest_term:
                    latest_term = lastTerm
                    longest = len(logAfterCommit)
                    extra_log_entries = logAfterCommit
                    latestProof = msg['proof']
                elif lastTerm == latest_term and len(logAfterCommit) > longest:
                    longest = len(logAfterCommit)
                    extra_log_entries = logAfterCommit
                    latestProof = msg['proof']
        self.log.append_entries(extra_log_entries, self.log.commitIndex)

        new_term_msg = {
            'type' : 'new_term',
            'term' : proposedTerm,
            'proof' : self.term_change_messages[proposedTerm]
        }
        # change ourselves to be a leader
        print(self.volatile['address'], " has received enough term change messages and is now the leader")
        self.persist['currentTerm'] = proposedTerm
        self.orchestrator.change_leader(latestProof)

        new_term_msg_signed = signDict(new_term_msg, self.volatile['privateKey'])
        for addr in self.volatile['cluster']:
            if addr != self.volatile['address']:
                self.orchestrator.send_peer(addr, new_term_msg_signed)