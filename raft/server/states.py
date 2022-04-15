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
    
    def get_latest_element_from_log(logSlice, commitIndex):
        if len(logSlice) == 0:
            return 

    def on_peer_new_term(self, peer, msg):
        # validate from peer
        if (not validateDict(msg, self.volatile['publicKeyMap'][peer])):
            return
        if msg['term'] <= self.persist['currentTerm']:
            return

        quorum_size = get_quorum_size(len(self.volatile['cluster'])) - 1 #2f
        if len(msg['proof']) < quorum_size:
            return # not enough messages to convince that new term
        
        for proofPeer, proofMsg in msg['proof'].items():
            if (not validateDict(proofMsg, self.volatile['publicKeyMap'][proofPeer])):
                return
            if proofMsg['term'] != msg['term']:
                return # not the right term in proof
        self.persist['currentTerm'] = msg['term']
        print(self.volatile['address'], " has received and validated a new_term message and is now reverting to follower state in term:", msg['term'])
        self.orchestrator.change_follower()

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

class Follower(State):
    """Follower state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        self.persist['votedFor'] = None
        self.proofOfCommit = None
        self.currTimeout = 5
        self.timerStartTime = None

    def teardown(self):
        """Stop timers before changing state."""
        self.election_timer.cancel()
        self.election_timer = None

    def start_election_timer(self):
        """Delays transition to the Candidate state by timer."""

        timeout = self.currTimeout
        loop = asyncio.get_event_loop()
        self.election_timer = loop.\
            call_later(timeout, self.orchestrator.change_voter)
        logger.debug('Election timer started: %s s', timeout)

    def cancel_election_timer(self):
        if hasattr(self, "election_timer"):  
            self.election_timer.cancel()
            self.election_timer = None
        self.currTimeout = 5

    def on_peer_update(self, peer, msg):
        """Manages incoming log entries from the Leader.
        Data from log compaction is always accepted.
        In the end, the log is scanned for a new cluster config.
        """

        # validate that the leader actually sent this message
        # TODO validate that this leader is a valid leader
        if (not validateDict(msg, self.volatile['publicKeyMap'][peer])):
            return

        entries = list(msg['entries'])
        # validate that the entries proposed are all signed by the client
        if not validate_entries(entries, self.volatile['clientKey']):
            return

        status_code = 0
        # check for prev log index match
        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = msg['prevLogTerm'] is None or\
            self.log.term(msg['prevLogIndex']) == msg['prevLogTerm']

        if not term_is_current or not prev_log_term_match:
            # there was a mismatch in prev log index
            status_code = 1
            logger.warning('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')
        else:
            hypothetical_new_log = self.log.log.data[:(msg['prevLogIndex'] + 1)] + entries
            # validate prepare and commit
            calcPrepare, calcCommit = validateIndex(hypothetical_new_log, msg['proof'], self.volatile['publicKeyMap'], len(self.volatile['cluster']))
            if calcPrepare == -2 or not calcPrepare == msg['leaderPrepare'] or not calcCommit == msg['leaderCommit']:
                return

            # either we don't need to overwrite a prepare or the leader
            # has a valid prepare index greater than our own
            should_copy_leader_log = (msg['leaderPrepare'] >= self.log.prepareIndex)
            if not should_copy_leader_log:
                status_code = 2 # you are trying to overwrite a prepare index without a greater leader prepare
            else:
                # we are successful so overwrite with leader's log
                self.log.append_entries(msg['entries'], msg['prevLogIndex']+1)
                self.log.prepare(msg['leaderPrepare'])
                oldCommitIndex = self.log.commitIndex
                self.log.commit(msg['leaderCommit'])
                newCommitIndex = self.log.commitIndex
                if newCommitIndex > oldCommitIndex:
                    self.cancel_election_timer()
                logger.debug('Log index is now %s', self.log.index)
                self.stats.increment('append', len(msg['entries']))
                self.proofOfCommit = msg['proof']
            self.volatile['leaderId'] = msg['leaderId']

        self._update_cluster()

        # status_code: {0: Good, 1: prev-log index mismatch, 2: tried to overwrite prepare without a greater leader prepare}
        resp = {'type': 'response_update', "status_code": status_code,
                'term': self.persist['currentTerm'],
                'prePrepareIndex': (self.log.index, self.log.getHash(self.log.index)),
                'prepareIndex' : (self.log.prepareIndex, self.log.getHash(self.log.prepareIndex))
        }
        signedResp = signDict(resp, self.volatile['privateKey'])
        self.orchestrator.send_peer(peer, signedResp)

class Voter(Follower):
    def __init__(self, old_state=None, orchestrator=None):
        super().__init__(old_state, orchestrator)
        self.election_timer = None
        self.currTimeout *= 2
        self.proposedTerm = self.persist['currentTerm'] + 1

        timeout = 1
        loop = asyncio.get_event_loop()
        self.term_change_timer = loop.call_later(timeout, self.send_term_change)

    def teardown(self):
        if self.election_timer is not None: 
            self.election_timer.cancel()
            self.election_timer = None

        self.term_change_timer.cancel()
        self.term_change_timer = None