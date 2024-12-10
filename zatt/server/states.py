import asyncio
import logging
import statistics
import random
import time
import pdb
import dill
from random import randrange
from os.path import join
from .utils import PersistentDict, TallyCounter
from .log import LogManager
from .config import config
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import utils
from cryptography.hazmat.backends import default_backend


logger = logging.getLogger(__name__)


class State:
    """Abstract state for subclassing."""
    def __init__(self, old_state=None, orchestrator=None):
        """State is initialized passing an orchestator instance when first
        deployed. Subsequent state changes use the old_state parameter to
        preserve the environment.
        """
        # pdb.set_trace()
        
        if old_state:
            self.orchestrator = old_state.orchestrator
            self.persist = old_state.persist
            self.volatile = old_state.volatile
            self.log = old_state.log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(config.storage, 'state'),
                                          {'votedFor': None, 'currentTerm': 0, 'protocol': "pbft"}) # In the persistent storage so that it cannot be messed with by faulty nodes AND remembers after it recovers
            self.volatile = {'leaderId': None, 'cluster': config.cluster,
                             'address': config.address}
            self.log = LogManager()
            self._update_cluster()
            
        self.stats = TallyCounter(['read', 'write', 'append'])
        # add the blackList field in persistant storage, if it does not exist
        if 'blackList' not in self.persist:
            self.persist['blackList'] = []
        # pdb.set_trace()
        # Check for PBFT protocol
        # if 'protocol' in self.persist and self.persist['protocol'] == 'pbft':
        # self.pbft = PBFTNode() # TODO: Check if we need a new set of storage files
        
        # To set the self.orchestrator.state based on the protocol... But doesn't work :(
        # if 'protocol' in self.persist and self.persist['protocol'] == 'pbft':
        #     self.pbft = PBFTNode(self.orchestrator, self.persist, self.volatile, self.log) # TODO: Check if we need a new set of storage files
        #     self.orchestrator.state = self.pbft
        # else:
        #     self.state = Follower(orchestrator=self)

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        # pdb.set_trace()
        logger.debug(f"\nProtocol currently in place: {self.persist['protocol']}\n")
        if self.persist['protocol'] == 'pbft':
            # self.pbft.data_received_peer(peer, msg)
            self.orchestrator.state.data_received_peer(peer, msg)
        
        else:
            if peer in self.persist['blackList']:
                logger.debug('Peer %s is blacklisted', peer)
                # TODO: Or something else
                return
            if len(self.persist['blackList']) >= len(self.volatile['cluster']) / 4:
                logger.info('TIME TO TRANFTER YOOOOO')
                self.persist['protocol'] = "PBFT"
                self.orchestrator.change_state(PBFTNode) ## Right Now, every node goes to the PBFTNode state and then dies...
                # TODO: more functionality TBD
                return
            # logger.debug('Received %s from %s', msg['type'], peer)
            if self.persist['currentTerm'] < msg['term']:
                self.persist['currentTerm'] = msg['term']
                if not type(self) is Follower:
                    logger.info('Remote term is higher, converting to Follower')
                    self.orchestrator.change_state(Follower)
                    self.orchestrator.state.data_received_peer(peer, msg)
                    return
            method = getattr(self, 'on_peer_' + msg['type'], None)
            if method:
                method(peer, msg)
            else:
                logger.info('Unrecognized message from %s: %s', peer, msg)

    def data_received_client(self, protocol, msg):
        """Receive client messages from orchestrator and pass them to the
        appropriate method."""
        # pdb.set_trace()
            
        if self.persist['protocol'] == 'pbft':
            if 'type' in msg and msg['type'] == 'get':
                self.on_client_get(protocol, msg)
                return
            self.orchestrator.state._data_received_client(protocol, msg)
            
        else:
            if 'type' in msg and msg['type'] == 'get':
                self.on_client_get(protocol, msg)
                return
            
            logger.debug(f"Current protocol is: {self.persist['protocol']}, So accordingly:")
            logger.debug("my role is: %s, I have received request from the client. My address is: %s", self.__class__.__name__, self.volatile['address'])
            if 'public_key' not in msg:
                logger.error('public_key not in msg')
                protocol.send({'type': 'result', 'success': False, 'additional_msg': 'public_key not in msg'})
                return
            # store the public key in the volatile state
            method = getattr(self, 'on_client_' + msg['type'], None)
            if method:
                method(protocol, msg)
            else:
                logger.info('Unrecognized message from %s: %s',
                            protocol.transport.get_extra_info('peername'), msg)
        return

    def on_client_append(self, protocol, msg):
        # """Redirect client to leader upon receiving a client_append message."""
        """ When client talks to follower, it only contains the public key.
            Thus we store this public key in the volatile state.
        """
        pdb.set_trace()
        
        self.volatile['public_key'] = msg['public_key']
        logger.debug('stored public_key to volatile. public_key is: %s', self.volatile['public_key'])
        protocol.send({'type': 'result', 'success': True, 'additional_msg': 'public_key stored to volatile'})
        return
        msg = {'type': 'redirect',
               'leader': self.volatile['leaderId']}
        protocol.send(msg)


    def on_client_config(self, protocol, msg):
        """Redirect client to leader upon receiving a client_config message."""
        pdb.set_trace()
        return self.on_client_append(protocol, msg)

    def on_client_get(self, protocol, msg):
        """Return state machine as wel as the cluster information to the client."""
        state_machine = self.log.state_machine.data.copy()
        # logger.debug('on_client_get gets called')
        # logger.debug('state_machine:', state_machine)
        self.stats.increment('read')
        if self.persist['protocol'] == 'raft':
            res = {"state_machine": state_machine, 'cluster': self.volatile['cluster'], 'leader': self.volatile['leaderId'], 'protocol': self.persist['protocol']}
        else:
            res = {"state_machine": state_machine, 'cluster': self.volatile['cluster'], 'primary': self.volatile['primary'], 'protocol': self.persist['protocol']}
        protocol.send(res)

    def on_client_diagnostic(self, protocol, msg):
        """Return internal state to client."""
        if self.persist['protocol'] == 'raft':
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
                            'matchIndex': tuple(self.matchIndex.items()),
                            'waiting_clients': {k: len(v) for (k, v) in
                                                self.waiting_clients.items()}}})

        else:
            msg = {
                'status': self.__class__.__name__,
                'persist': {
                    'currentView': self.current_view,
                },
                'volatile': self.volatile,
                'log': {
                    'primary': self.volatile['primary'],
                    'preparedMessages': list(self.prepared_messages.values()),
                    'commitIndex': self.commit_index
                },
                'stats': self.stats.data
            }
            msg['volatile']['cluster'] = list(msg['volatile']['cluster'])

            # If this node is the primary, include additional information
            # if self.persist['primary'] == self.volatile['address']:
            #     msg.update({
            #         'primaryStatus': {
            #             'pendingRequests': list(self.pending_requests),
            #             'broadcasts': self.broadcast_log,
            #             'clientResponses': {k: v for k, v in self.client_responses.items()}
            #         }
            #     })

            # # If this node is a replica, include additional state information
            # else:
            #     msg.update({
            #         'replicaStatus': {
            #             'viewChangeVotes': self.view_change_votes,
            #             'preparedCertificates': {k: v for k, v in self.prepared_certificates.items()}
            #         }
            #     })

            # Send the diagnostic information back to the client
            protocol.send(msg)

    def _update_cluster(self, entries=None):
        """Scans compacted log and log, looking for the latest cluster
        configuration."""
        if 'cluster' in self.log.compacted.data:
            self.volatile['cluster'] = self.log.compacted.data['cluster']
        for entry in (self.log if entries is None else entries):
            if entry['data']['key'] == 'cluster':
                self.volatile['cluster'] = entry['data']['value']
        self.volatile['cluster'] = tuple(map(tuple, self.volatile['cluster']))
    
    def _verify_signature(self, message, signature, public_key):
        """_summary_

        Args:
            message (_type_): the message to be verified
            signature (_type_): the signature to be verified
            public_key (_type_): the public key to be used for verification

        Returns:
            _type_: False if the signature is invalid, true otherwise
        """
        try:
            # logger.debug("yooo message is: %s", message)
            # logger.debug("yooo signature is: %s", signature)
            # logger.debug("yooo public_key is: %s", public_key)
            # currently message is of type dict. I need to convert it to bytes
            msgBytes = dill.dumps(message['data'])
            public_key = serialization.load_pem_public_key(public_key, backend=default_backend())
            public_key.verify(
                signature,
                msgBytes,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            logger.debug('Signature verification passed!')
            return True
        except Exception as e:
            logger.error('Error verifying signature: %s', e)
            return False
       

class Follower(State):
    """Follower state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        # pdb.set_trace()
        self.persist['votedFor'] = None
        self.restart_election_timer()

    def teardown(self):
        """Stop timers before changing state."""
        self.election_timer.cancel()

    def restart_election_timer(self):
        """Delays transition to the Candidate state by timer."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        # timeout = randrange(1, 4) * 10 ** (0 if config.debug else -1)
        timeout = randrange(5, 20) * 10 ** (0 if config.debug else -1)
        
        loop = asyncio.get_event_loop()
        self.election_timer = loop.\
            call_later(timeout, self.orchestrator.change_state, Candidate)
        logger.debug('Election timer restarted: %s s', timeout)

    def on_peer_request_vote(self, peer, msg):
        """Grant this node's vote to Candidates."""
        term_is_current = msg['term'] >= self.persist['currentTerm']
        can_vote = self.persist['votedFor'] in [tuple(msg['candidateId']),
                                                None]
        index_is_current = (msg['lastLogTerm'] > self.log.term() or
                            (msg['lastLogTerm'] == self.log.term() and
                             msg['lastLogIndex'] >= self.log.index))
        granted = term_is_current and can_vote and index_is_current

        if granted:
            self.persist['votedFor'] = msg['candidateId']
            self.restart_election_timer()

        logger.debug('Voting for %s. Term:%s Vote:%s Index:%s',
                     peer, term_is_current, can_vote, index_is_current)

        response = {'type': 'response_vote', 'voteGranted': granted,
                    'term': self.persist['currentTerm']}
        self.orchestrator.send_peer(peer, response)

    def on_peer_append_entries(self, peer, msg):
        """Manages incoming log entries from the Leader.
        Data from log compaction is always accepted.
        In the end, the log is scanned for a new cluster config.
        """
        # if leader is in the blackList, do not accept the message
        if peer in self.persist['blackList']:
            logger.debug('Peer %s is blacklisted', peer)
            resp = {'type': 'response_append', 'success': False, 'term': self.persist['currentTerm'], 'matchIndex': self.log.index}
            return

        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = msg['prevLogTerm'] is None or\
            self.log.term(msg['prevLogIndex']) == msg['prevLogTerm']
        success = term_is_current and prev_log_term_match
        # if signature or public_key is in the message, pop them
        if "entries" in msg:
            for entry in msg["entries"]:
                signature, public_key = entry.get("signature", None), self.volatile.get("public_key", None)
                entry.pop("signature", None)
                entry.pop("public_key", None)
                if signature and public_key and not self._verify_signature(entry, signature, public_key):
                    # blackLit the peer
                    self.persist['blackList'].append(peer)
                    # TODO: Look at this
                    logger.debug('Signature verification failed, current blackList: %s', self.persist['blackList'])
                    if len(self.persist['blackList']) >= len(self.volatile['cluster']) / 4: # TODO: Wouldn't it be better if we throw off the current process if this is the case? instead of directly converting to pbft
                        self.persist['protocol'] = "PBFT"
                        logger.info('RAFT threshold reached, switching to PBFT')
                    resp = {'type': 'response_append', 'success': False, 'term': self.persist['currentTerm'], 'matchIndex': self.log.index}
                    self.orchestrator.send_peer(peer, resp)
                    return
        if term_is_current:
            self.restart_election_timer()
        if 'compact_data' in msg:
            self.log = LogManager(compact_count=msg['compact_count'],
                                  compact_term=msg['compact_term'],
                                  compact_data=msg['compact_data'])
            self.volatile['leaderId'] = msg['leaderId']
            # logger.debug('Initialized Log with compact data from Leader')
        elif success:
            self.log.append_entries(msg['entries'], msg['prevLogIndex'])
            self.log.commit(msg['leaderCommit'])
            self.volatile['leaderId'] = msg['leaderId']
            logger.debug('Log index is now %s', self.log.index)
            self.stats.increment('append', len(msg['entries']))
        else:
            logger.warning('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')
        self._update_cluster()
        resp = {'type': 'response_append', 'success': success, 'term': self.persist['currentTerm'], 'matchIndex': self.log.index}
        self.orchestrator.send_peer(peer, resp)


class Candidate(Follower):
    """Candidate state. Notice that this state subclasses Follower."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, increase term, vote for self, ask for votes."""
        super().__init__(old_state, orchestrator)
        self.persist['currentTerm'] += 1
        self.votes_count = 0
        logger.info('New Election. Term: %s', self.persist['currentTerm'])
        self.send_vote_requests()

        def vote_self():
            self.persist['votedFor'] = self.volatile['address']
            self.on_peer_response_vote(
                self.volatile['address'], {'voteGranted': True})
        loop = asyncio.get_event_loop()
        loop.call_soon(vote_self)

    def send_vote_requests(self):
        """Ask peers for votes."""
        logger.info('Broadcasting request_vote')
        msg = {'type': 'request_vote', 'term': self.persist['currentTerm'],
               'candidateId': self.volatile['address'],
               'lastLogIndex': self.log.index,
               'lastLogTerm': self.log.term()}
        self.orchestrator.broadcast_peers(msg)

    def on_peer_append_entries(self, peer, msg):
        """Transition back to Follower upon receiving an append_entries."""
        logger.debug('===========Converting to Follower===========')
        self.orchestrator.change_state(Follower)
        self.orchestrator.state.on_peer_append_entries(peer, msg)

    def on_peer_response_vote(self, peer, msg):
        """Register peers votes, transition to Leader upon majority vote."""
        self.votes_count += msg['voteGranted']
        logger.info('Vote count: %s', self.votes_count)
        if self.votes_count > len(self.volatile['cluster']) / 2:
            logger.debug('===========Converting to Leader, majority reached===========')
            self.orchestrator.change_state(Leader)


class Leader(State):
    """Leader state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, sets leader variables, start periodic
        append_entries"""
        super().__init__(old_state, orchestrator)
        logger.info('Leader of term: %s', self.persist['currentTerm'])
        self.volatile['leaderId'] = self.volatile['address']
        self.matchIndex = {p: 0 for p in self.volatile['cluster']}
        self.nextIndex = {p: self.log.commitIndex + 1 for p in self.matchIndex}
        self.waiting_clients = {}
        self.send_append_entries()

        if 'cluster' not in self.log.state_machine:
            self.log.append_entries([
                {'term': self.persist['currentTerm'],
                 'data':{'key': 'cluster',
                         'value': tuple(self.volatile['cluster']),
                         'action': 'change'}}],
                self.log.index)
            self.log.commit(self.log.index)

    # override the method in the parent class
    def data_received_client(self, protocol, msg):
        """Receive client messages from orchestrator and pass them to the
        appropriate method."""
        # for testing purpose
        pdb.set_trace()
        logger.debug('I am a leader, and I have received request from the client.')
        if 'type' in msg and msg['type'] == 'get':
            logger.debug("calling on_client_get")
            self.on_client_get(protocol, msg)
            return
        method = getattr(self, 'on_client_' + msg['message']['type'], None)
        # protocol.send({'type': 'result', 'success': False, 'additional_msg': 'Signature verification passed'})
        # return 
        # TODO: do I have to fix the msg content?? Not sure. Too hungry to think about it...
        if method:
            logger.debug('I am calling the method: %s', method)
            method(protocol, msg)
        else:
            logger.info('Unrecognized message from %s: %s',
                        protocol.transport.get_extra_info('peername'), msg)

    def teardown(self):
        """Stop timers before changing state."""
        self.append_timer.cancel()
        if hasattr(self, 'config_timer'):
            self.config_timer.cancel()
        for clients in self.waiting_clients.values():
            for client in clients:
                client.send({'type': 'result', 'success': False})
                logger.error('Sent unsuccessful response to client')

    def send_append_entries(self):
        """Send append_entries to the cluster, containing:
        - nothing: if remote node is up to date.
        - compacted log: if remote node has to catch up.
        - log entries: if available.
        Finally schedules itself for later esecution."""
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            
            msg = {'type': 'append_entries',
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderId': self.volatile['address'],
                   'prevLogIndex': self.nextIndex[peer] - 1,
                   'entries': self.log[self.nextIndex[peer]:
                                       self.nextIndex[peer] + 100]} 
            msg.update({'prevLogTerm': self.log.term(msg['prevLogIndex'])})

            # There are some issue with how the entreis are sliced.. TODO: fix this

            if self.nextIndex[peer] <= self.log.compacted.index:
                msg.update({'compact_data': self.log.compacted.data,
                            'compact_term': self.log.compacted.term,
                            'compact_count': self.log.compacted.count})
            logger.debug('Sending %s entries to %s. Start index %s',
                         len(msg['entries']), peer, self.nextIndex[peer])
            self.orchestrator.send_peer(peer, msg)

        timeout = randrange(1, 4) * 10 ** (-1 if config.debug else -2)
        loop = asyncio.get_event_loop()
        self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def on_peer_response_append(self, peer, msg):
        # peer = self.volatile['address'], msg = {'success': True, 'matchIndex': self.log.commitIndex}
        """Handle peer response to append_entries.
        If successful RPC, try to commit new entries.
        If RPC unsuccessful, backtrack."""
        if msg['success']:
            self.matchIndex[peer] = msg['matchIndex']
            self.nextIndex[peer] = msg['matchIndex'] + 1

            self.matchIndex[self.volatile['address']] = self.log.index
            self.nextIndex[self.volatile['address']] = self.log.index + 1
            # index = statistics.median_low(self.matchIndex.values()) # I feel like this is the problem...
            # what if I change to mediam_high?
            index = statistics.median_low(self.matchIndex.values())
            self.log.commit(index) 
            self.send_client_append_response()
        else:
            self.nextIndex[peer] = max(0, self.nextIndex[peer] - 1)

    def on_client_append(self, protocol, msg):
        """Append new entries to Leader log. When client taks to leader, it contains the public key + the actual message."""
        msgData = msg['message']['data']
        # TODO: think about what to do with the signatrue...
        logger.debug('Leader has received append request from client')
        # Here I attach the signature and the public key to the log entry. So that later followers can verify the signature
        signature = msg['signature']
       
        # Introduce faulty behavior by incorrectly signing the entry
        faulty_signature = b'faulty_signature'  # Replace with incorrect data to simulate a faulty leader
        
        # TODO: 5 percent chance of faulty signature
        if random.uniform(0, 1) < 0.05:
            entry = {'term': self.persist['currentTerm'], 'data': msgData, "signature": faulty_signature}
        else:
            entry = {'term': self.persist['currentTerm'], 'data': msgData, "signature": signature}
        if msgData['key'] == 'cluster':
            protocol.send({'type': 'result', 'success': False})
        self.log.append_entries([entry], self.log.index)
        
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(protocol)
        else:
            logger.debug("Appending to waiting_clients")
            self.waiting_clients[self.log.index] = [protocol]

        
        # protocol.send({'type': 'result', 'success': True, "log_index": logIndex,
        #             "term": term, "volatile_addr": volatile_addr, "commit_index": commit_index,
        #             'msg': "test things out"})
    
        # peer = self.volatile['address'], msg = {'success': True, 'matchIndex': self.log.commitIndex}
        self.on_peer_response_append(
            self.volatile['address'], {'success': True,
                                       'matchIndex': self.log.commitIndex})

    def send_client_append_response(self):
        """Respond to client upon commitment of log entries."""
        to_delete = []
        # print("I am at send_client_append_response, here is the log.commitIndex: ", self.log.commitIndex) # 0
        # print("Here is the waiting_clients: ", self.waiting_clients) # 1
        
        # commit index is not set correctly..
        for client_index, clients in self.waiting_clients.items():
            if client_index <= self.log.commitIndex:
                for client in clients:
                    client.send({'type': 'result', 'success': True}) 
                    logger.debug('Sent successful response to client')
                    self.stats.increment('write')
                to_delete.append(client_index)
        for index in to_delete:
            del self.waiting_clients[index]

    def on_client_config(self, protocol, msg):
        """Push new cluster config. When uncommitted cluster changes
        are already present, retries until they are committed
        before proceding."""
        pending_configs = tuple(filter(lambda x: x['data']['key'] == 'cluster',
                                self.log[self.log.commitIndex + 1:]))
        if pending_configs:
            timeout = randrange(1, 4) * 10 ** (0 if config.debug else -1)
            loop = asyncio.get_event_loop()
            self.config_timer = loop.\
                call_later(timeout, self.on_client_config, protocol, msg)
            return

        success = True
        cluster = set(self.volatile['cluster'])
        peer = (msg['address'], int(msg['port']))
        if msg['action'] == 'add' and peer not in cluster:
            logger.info('Adding node %s', peer)
            cluster.add(peer)
            self.nextIndex[peer] = 0
            self.matchIndex[peer] = 0
        elif msg['action'] == 'delete' and peer in cluster:
            logger.info('Removing node %s', peer)
            cluster.remove(peer)
            del self.nextIndex[peer]
            del self.matchIndex[peer]
        else:
            success = False
        if success:
            self.log.append_entries([
                {'term': self.persist['currentTerm'],
                 'data':{'key': 'cluster', 'value': tuple(cluster),
                         'action': 'change'}}],
                self.log.index)
            self.volatile['cluster'] = cluster
        protocol.send({'type': 'result', 'success': success})

class PBFTNode(State):
    def __init__(self, old_state=None, orchestrator=None):
        super().__init__(old_state, orchestrator)
## Somehow if we are inheriting the old_state and orchestrator, it is not working... We get an error saying `Orchestrator` doesn't have `orchestrator`?! TODO: Correct it!!!!
        # super().__init__()
        
        # pdb.set_trace()
        logger.debug(self.persist)
        self.prepared = set()
        self.committed = set()
        self.acknowledged = set()
        self.cluster = self.volatile['cluster']
        self.volatile['current_view'] = 0 # Current view number
        self.volatile['primary'] = None
        self.view_change_votes = {}  # Track view change votes from other nodes
        self.last_pre_prepare_time = 0
        
        # Assign an index to this node in the cluster
        self.node_index = self.cluster.index(self.volatile['address'])
        
        # To help this server get to speed with the rest of the cluster
        # self.request_current_state()
        
        # Start monitoring for primary inactivity
        self.monitor_primary_inactivity()
        
        # Check if this node should initiate view change
        if self.volatile['primary'] is None:
            self.start_view_change_timer()
    
    def request_current_state(self):
        """Request the current state from the cluster when the server joins."""
        pdb.set_trace()
        msg = {
            'type': 'join-request',
            'address': self.volatile['address']
        }
        for peer in self.cluster:
            if peer != self.volatile['address']:
                self.orchestrator.send_peer(peer, msg)

    def handle_join_request(self, peer, msg):
        """Handle a join request from a new server joining the cluster."""
        logger.info("Received join request from new server: %s", msg['address'])

        # Send back the current state (view, primary, etc.)
        current_state = {
            'type': 'join-response',
            'view': self.volatile['current_view'],
            'primary': self.volatile['primary'],
            'log': self.get_log()  # Or any other state details needed
        }
        
        self.orchestrator.send_peer(msg['address'], current_state)

    def handle_join_response(self, peer, msg):
        """Handle the response to the join request."""
        logger.info("Received join response from cluster: %s", msg)

        # Update the primary and view based on the current state from the cluster
        self.volatile['primary'] = msg['primary']
        self.volatile['current_view'] = msg['view']
        
        # If the new server is the primary, switch to Primary state
        if self.volatile['primary'] == self.volatile['address']:
            logger.info("This node is the new primary. Switching to Primary state.")
            self.orchestrator.change_state(Primary)
        else:
            logger.debug("This node is not the primary.")
        
    def monitor_primary_inactivity(self):
        """Monitor the primary node for inactivity and trigger view change."""
        timeout = random.uniform(5, 10)  # Timeout duration for detecting inactivity
        loop = asyncio.get_event_loop()
        self.inactivity_timer = loop.call_later(timeout, self.check_primary_activity)

    def check_primary_activity(self):
        """Check primary activity and trigger view change if necessary."""
        if not self.has_received_pre_prepare():
            logger.warning("No activity from primary. Initiating view change.")
            synthetic_peer = "self"  # Indicating it's the local node triggering
            synthetic_msg = {'type': 'view-change', 'view': self.volatile['current_view'] + 1}
            self.handle_view_change(synthetic_peer, synthetic_msg)
        else:
            new_primary_index = (self.volatile['current_view'] + 1) % len(self.cluster)
            self.volatile['primary'] = self.cluster[new_primary_index]
            logger.debug("Primary is active. Resetting inactivity timer.")
            self.monitor_primary_inactivity()  # Reset the timer
        
    def has_received_pre_prepare(self):
        """Check if the node has received a pre-prepare message recently."""
        current_time = time.time()
        # Check if the last pre-prepare was received within an acceptable threshold
        return (current_time - self.last_pre_prepare_time) <= 10  # 10-second threshold
    
    def start_view_change_timer(self):
        """Start a timer to initiate a view change if no primary exists."""
        timeout = random.uniform(2, 5)  # Randomized timeout to avoid collisions
        loop = asyncio.get_event_loop()
        self.view_change_timer = loop.call_later(timeout, self.trigger_view_change)

    def trigger_view_change(self):
        """Trigger a view change if no primary is active."""
        if self.volatile['primary'] is None:
            logger.info("No primary detected. Initiating view change.")
            # synthetic_peer = "self"  # Indicating it's the local node triggering
            synthetic_msg = {'type': 'view-change', 'view': self.volatile['current_view'] + 1}
            # self.send_view_change_requests(synthetic_peer, synthetic_msg)
            self.orchestrator.broadcast_peers(synthetic_msg)
        else:
            logger.debug("====Primary exists. View change not required====")

    def handle_view_change(self, peer, msg):
        """Handle view change message from another node"""
        logger.debug("Node %s received VIEW-CHANGE from %s", self.volatile['address'], peer)

        # Validate the view change message (check if the view number is valid)
        if msg['view'] > self.volatile['current_view']:
            self.view_change_votes[peer] = msg['view']  # Store vote for the new view

            # Check if sufficient votes have been collected for the view-change
            votes_needed = len(self.cluster) - self.get_fault_tolerance()
            logger.debug(f"Votes needed: {votes_needed}, Votes collected: {len(self.view_change_votes)}")
            
            # If enough votes are collected, initiate the view change
            if len(self.view_change_votes) >= votes_needed:
                logger.info("Sufficient votes collected, initiating view change.")
                self.initiate_view_change(msg['view'])

    def initiate_view_change(self, new_view):
        """Initiate the view change process by updating the view and selecting a new primary"""
        logger.debug("Node %s initiating view change to view %d", self.volatile['address'], new_view)
        
        self.volatile['current_view'] = new_view
        self.view_change_votes = {}  # Reset the votes for the new view

        # Select a new primary node based on (view + 1) % cluster_size
        new_primary_index = (new_view + 1) % len(self.cluster)
        self.volatile['primary'] = self.cluster[new_primary_index]
        logger.info("New primary selected: %s", self.volatile['primary'])
        
        # Change state of the new primary to 'Primary'
        if self.volatile['primary'] == self.volatile['address']:
            logger.info("\n\n==========This node is the new primary.==========\n\n")
            self.orchestrator.change_state(Primary)
        # else:
        #     logger.info("This node is not the new primary.")

        # Broadcast the new view to the cluster
        # self.broadcast_view_change()

    # def broadcast_view_change(self):
    #     """Broadcast view change message to all nodes"""
    #     logger.debug("Node %s broadcasting VIEW-CHANGE for view %d", self.volatile['address'], self.volatile['current_view'] + 1)
    
    #     new_view = self.volatile['current_view'] 
    #     self.volatile['current_view'] = new_view
    #     # self.view_change_votes[self.volatile['address']] = new_view  # Self-vote
        
    #     msg = {
    #         'type': 'new-view',
    #         'view': new_view,
    #         'primary': self.volatile['primary']
    #     }
        
    #     self.orchestrator.broadcast_peers(msg)
    #     # for peer in self.cluster:
    #     #     if peer != self.volatile['address']:
    #     #         self.orchestrator.send_peer(peer, msg)
        
    def set_changed_view(self, peer, msg):
        """Recieve the first message from the new primary"""
        logger.info(f"Recieved the first message from the Primary")
        self.volatile['current_view'] = msg['view']
        self.volatile['primary'] = msg['primary']

    def _data_received_client(self, protocol, msg):
        """Receive client messages from orchestrator and process them."""
        logger.debug(
            "My role is: %s, I have received a request from the client. My address is: %s",
            self.__class__.__name__, self.volatile['address']
        )
        
        # Validate the incoming message
        if 'public_key' not in msg:
            logger.error('public_key not in msg')
            protocol.send({'type': 'result', 'success': False, 'additional_msg': 'public_key not in msg'})
            return
        
        # Store the public key in the volatile state
        self.volatile['client_public_key'] = msg['public_key']
        
        # If this node is the primary, invoke the pre-prepare phase
        if self.volatile['address'] == self.volatile['primary']:
            logger.debug("This node is the primary. Initiating pre-prepare phase.")
            self.initiate_pre_prepare(protocol, msg)
        else:
            # For replicas, pass the message to the appropriate handler
            logger.debug("This node is not the primary. Forwarding message to appropriate handler.")
            method = getattr(self, 'on_client_' + msg['type'], None)
            if method:
                method(protocol, msg)
            else:
                logger.info(
                    'Unrecognized message from %s: %s',
                    protocol.transport.get_extra_info('peername'), msg
                )
            return    
    
    def data_received_peer(self, peer, msg):
        """Handle messages from peers in PBFT protocol"""
        logger.debug("PBFTNode received %s from peer %s", msg['type'], peer)
        
        if msg['type'] == 'new-view':
            self.set_changed_view(peer, msg)
        if msg['type'] == 'heartbeat':
            self.last_pre_prepare_time = time.time()  # Update primary activity
        if msg['type'] == 'pre-prepare':
            self.handle_pre_prepare(peer, msg)
        elif msg['type'] == 'prepare':
            self.handle_prepare(peer, msg)
        elif msg['type'] == 'commit':
            self.handle_commit(peer, msg)
        elif msg['type'] == 'ack':
            self.handle_ack(peer, msg)
        elif msg['type'] == 'view-change':
            self.handle_view_change(peer, msg)
        else:
            logger.info('Unrecognized message from %s: %s', peer, msg)

    def handle_pre_prepare(self, peer, msg):
        """Handle the pre-prepare message sent by the primary node"""
        logger.debug("Node %s received PRE-PREPARE from %s with value: %s", self.volatile['address'], peer, msg['value'])
        # Validate the pre-prepare message (signature, value, etc.)
        # After validation, prepare the message
        # self.prepared.add(msg['value'])
        self.send_prepare(peer, msg['value'])

    def send_prepare(self, peer, value):
        """Send prepare message to the primary node"""
        logger.debug("Node %s sending PREPARE for value: %s", self.volatile['address'], value)
        self.orchestrator.send_peer(peer, {
            'type': 'prepare',
            'value': value,
            'primary': self.volatile['primary'],
            'view': self.volatile['current_view']
        })

    def handle_prepare(self, peer, msg):
        """Handle the prepare message from another node"""
        logger.debug("Node %s received PREPARE from %s with value: %s", self.volatile['address'], peer, msg['value'])
        # If the message is valid, add to prepared set and send commit message if needed
        self.prepared.add(msg['value'])
        if len(self.prepared) >= (len(self.cluster) - self.get_fault_tolerance()):
            self.send_commit(peer, msg['value'])

    def send_commit(self, peer, value):
        """Send commit message to the primary node"""
        logger.debug("Node %s sending COMMIT for value: %s", self.volatile['address'], value)
        self.orchestrator.send_peer(peer, {
            'type': 'commit',
            'value': value,
            'node_id': self.volatile['address'],
        })

    def handle_commit(self, peer, msg):
        """Handle commit message"""
        logger.debug("Node %s received COMMIT from %s with value: %s", self.volatile['address'], peer, msg['value'])
        self.committed.add(msg['value'])
        if len(self.committed) >= (len(self.cluster) - self.get_fault_tolerance()):
            self.send_ack(peer, msg['value'])

    def send_ack(self, peer, value):
        """Send ACK message to the primary node"""
        logger.debug("Node %s sending ACK for value: %s", self.volatile['address'], value)
        self.orchestrator.send_peer(peer, {
            'type': 'ack',
            'value': value,
            'node_id': self.volatile['address'],
            'term': self.persist['currentTerm']
        })

    def handle_ack(self, peer, msg):
        """Handle ACK message"""
        logger.debug("Node %s received ACK from %s with value: %s", self.volatile['address'], peer, msg['value'])
        self.acknowledged.add(msg['value'])
        if len(self.acknowledged) >= (len(self.cluster) - self.get_fault_tolerance()):
            # Commit the value locally (on the ledger)
            self.commit_value(msg['value'])
            logger.info("Node %s has successfully committed value: %s", self.volatile['address'], msg['value'])

    def commit_value(self, value):
        """Commit the value to the ledger"""
        logger.debug("Committing value: %s", value)
        # Logic for updating the local ledger
        self.persist['ledger'] = self.persist.get('ledger', [])
        self.persist['ledger'].append(value)
        
    def get_fault_tolerance(self):
        """Return the fault tolerance level (f) for the PBFT protocol"""
        return (len(self.cluster) - 1) // 3  # PBFT can tolerate up to (n-1)/3 faulty nodes

    def handle_propose(self, protocol, msg):
        """Handle a client proposal in PBFT protocol"""
        logger.debug("Node %s received client proposal: %s", self.volatile['address'], msg)
        if self.primary is None:
            self.primary = self.volatile['address']  # Set the primary node if it's not set

        # Send pre-prepare message to all nodes
        self.send_pre_prepare(msg['value'])
        protocol.send({'type': 'result', 'success': True, 'additional_msg': 'Value proposed to PBFT network'})

    def send_pre_prepare(self, value):
        """Send pre-prepare message to all nodes"""
        logger.debug("Node %s sending PRE-PREPARE with value: %s", self.volatile['address'], value)
        for peer in self.cluster:
            if peer != self.volatile['address']:  # Send to all other nodes
                self.orchestrator.send_peer(peer, {
                    'type': 'pre-prepare',
                    'value': value,
                    'node_id': self.volatile['address'],
                    'term': self.persist['currentTerm']
                })

class Primary(State):
    def __init__(self, old_state=None, orchestrator=None):
        super().__init__(old_state, orchestrator)

        self.heartbeat_interval = 5  # Time interval for sending heartbeats
        self.start_heartbeat()
        
        self.broadcast_view_change()
        
    def broadcast_view_change(self):
        """Broadcast view change message to all nodes"""
        logger.debug("Node %s broadcasting VIEW-CHANGE for view %d", self.volatile['address'], self.volatile['current_view'] + 1)
    
        new_view = self.volatile['current_view'] 
        self.volatile['current_view'] = new_view
        # self.view_change_votes[self.volatile['address']] = new_view  # Self-vote
        
        msg = {
            'type': 'new-view',
            'view': new_view,
            'primary': self.volatile['primary']
        }
        
        self.orchestrator.broadcast_peers(msg)
        # for peer in self.cluster:
        #     if peer != self.volatile['address']:
        #         self.orchestrator.send_peer(peer, msg)    
    
    def start_heartbeat(self):
        """Start sending periodic heartbeats to replicas."""
        loop = asyncio.get_event_loop()
        loop.call_later(self.heartbeat_interval, self.send_heartbeat)

    def send_heartbeat(self):
        """Send a heartbeat message to all replicas."""
        logger.debug("Primary %s sending heartbeat.", self.volatile['address'])
        for peer in self.cluster:
            if peer != self.volatile['address']:  # Do not send to self
                msg = {'type': 'heartbeat', 'view': self.volatile['current_view']}
                self.orchestrator.send_peer(peer, msg)
        self.start_heartbeat()  # Schedule the next heartbeat
        
    # def _data_received_client(self, protocol, msg):
    #     """Receive client messages from orchestrator and process them."""
    #     logger.debug(
    #         "My role is: %s, I have received a request from the client. My address is: %s",
    #         self.__class__.__name__, self.volatile['address']
    #     )
        
    #     # Validate the incoming message
    #     if 'public_key' not in msg:
    #         logger.error('public_key not in msg')
    #         protocol.send({'type': 'result', 'success': False, 'additional_msg': 'public_key not in msg'})
    #         return
        
    #     # Store the public key in the volatile state
    #     self.volatile['client_public_key'] = msg['public_key']
        
    #     # If this node is the primary, invoke the pre-prepare phase
    #     if self.volatile['address'] == self.volatile['primary']:
    #         logger.debug("This node is the primary. Initiating pre-prepare phase.")
    #         self.initiate_pre_prepare(protocol, msg)
    #     else:
    #         # For replicas, pass the message to the appropriate handler
    #         logger.debug("This node is not the primary. Forwarding message to appropriate handler.")
    #         method = getattr(self, 'on_client_' + msg['type'], None)
    #         if method:
    #             method(protocol, msg)
    #         else:
    #             logger.info(
    #                 'Unrecognized message from %s: %s',
    #                 protocol.transport.get_extra_info('peername'), msg
    #             )
    #         return    
    
