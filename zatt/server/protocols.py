import asyncio
import os
import msgpack
import logging
from .states import Follower, PBFTNode
from .config import config
from .utils import extended_msgpack_serializer
import pickle
import dill

logger = logging.getLogger(__name__)


class Orchestrator():
    """The orchestrator manages the current node state,
    switching between Follower, Candidate and Leader when necessary.
    Only one Orchestrator """
    def __init__(self):
        os.makedirs(config.storage, exist_ok=True)
        # self.state = None  # Initialize the state here to avoid the AttributeError
        # import pdb; pdb.set_trace()
        # self.state = Follower(orchestrator=self)  # ALWAYS start with raft by default, hope that's fine
        self.state = PBFTNode(orchestrator=self)
        logger.debug(f"The State is set to: {self.state}")
        
    def change_state(self, new_state):
        self.state.teardown()
        logger.info('State change:' + new_state.__name__)
        self.state = new_state(old_state=self.state)
        if new_state.__name__ == 'PBFTNode':
            logger.info("CHANGED TO PBFT!!! I DON'T HAVE TO WORK ANYMORE")
            exit()

    def data_received_peer(self, sender, message):
        self.state.data_received_peer(sender, message)

    def data_received_client(self, transport, message):
        self.state.data_received_client(transport, message)

    def send(self, transport, message):
        # transport.sendto(msgpack.packb(message, use_bin_type=True,
        #                  default=extended_msgpack_serializer))
        # transport.sendto(pickle.dumps(message))
        transport.sendto(dill.dumps(message))
        
    # I added this lol
    def prepare_message_for_serialization(self, message):
        # print("entries type is: ", type(message['entries']))
        if "entries" in message: 
            message['entries'] = message['entries'].to_list()
        return message

    def send_peer(self, recipient, message):
        # message = self.prepare_message_for_serialization(message)
        # try:
        # import pdb; pdb.set_trace()
        if recipient != self.state.volatile['address']:
            # print("type of the message: ", type(message))
            # self.peer_transport.sendto(
            #     msgpack.packb(message, use_bin_type=True), tuple(recipient))
            # self.peer_transport.sendto(
            #     pickle.dumps(message), tuple(recipient))
            self.peer_transport.sendto(
                dill.dumps(message), tuple(recipient))
        # except Exception as e:
        #     print(e)
        #     print("This is the message: ", message)
        #     print("msg entry: ", message['entries'])


    def broadcast_peers(self, message):
        for recipient in self.state.volatile['cluster']:
            self.send_peer(recipient, message)


class PeerProtocol(asyncio.Protocol):
    """UDP protocol for communicating with peers."""
    def __init__(self, orchestrator, first_message=None):

        self.orchestrator = orchestrator
        self.first_message = first_message

    def connection_made(self, transport):
        self.transport = transport
        if self.first_message:
            # transport.sendto(
            #     msgpack.packb(self.first_message, use_bin_type=True))
            # transport.sendto(pickle.dumps(self.first_message))
            transport.sendto(dill.dumps(self.first_message))

    def datagram_received(self, data, sender):
        # message = msgpack.unpackb(data, encoding='utf-8')
        # message = msgpack.unpackb(data, raw=False)
        # message = pickle.loads(data)
        message = dill.loads(data)
        
        self.orchestrator.data_received_peer(sender, message)

    def error_received(self, ex):
        print('Error:', ex)


class ClientProtocol(asyncio.Protocol):
    """TCP protocol for communicating with clients."""
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator

    def connection_made(self, transport):
        logger.debug('Established connection with client %s:%s',
                     *transport.get_extra_info('peername'))
        self.transport = transport

    def data_received(self, data):
        # message = msgpack.unpackb(data, encoding='utf-8')
        # message = pickle.loads(data)
        message = dill.loads(data)
        self.orchestrator.data_received_client(self, message)

    def connection_lost(self, exc):
        logger.debug('Closed connection with client %s:%s',
                     *self.transport.get_extra_info('peername'))

    def send(self, message):
        # self.transport.write(msgpack.packb(
        #     message, use_bin_type=True, default=extended_msgpack_serializer))
        # self.transport.write(pickle.dumps(message))
        self.transport.write(dill.dumps(message))
        self.transport.close()
