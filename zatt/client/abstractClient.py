import socket
import random
import msgpack
import pickle
import dill
import pdb
# import encryption stuff
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import utils


class AbstractClient:
    """Abstract client. Contains primitives for implementing functioning
    clients."""

    def _request(self, message):
        try:
            if message['type'] == 'get':
                return self._handle_get_request(message)
            else:
                return self._handle_set_request(message)
        except socket.timeout:
            print('Timeout')
        except Exception as e:
            print(f"Exception: {e}")
    
    def _handle_get_request(self, message):
        response = self._send_to_server(self.server_address, message)
        if 'type' in response and response['type'] == 'redirect':
            self.server_address = tuple(response['leader'])
            print("current leader:", self.server_address)
            return self._handle_get_request(message)
        return response

    def _handle_set_request(self, message):
        followers_response = [self._send_to_follower(follower, message) for follower in self.followers]
        leader_response = self._send_to_leader(message, self.currLeader)  
        return {"leaderResp": leader_response, "followersResp": followers_response, 'success': leader_response['success']}

    def _send_to_server(self, address, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        try:
            sock.connect(address)
            sock.send(dill.dumps(message))
            return self._receive_response(sock)
        finally:
            sock.close()

    def _send_to_leader(self, message, leaderAddr):
        print("sending to leader function got called")
        # can we only sign the data part of the message?
        # signedData = self._sign_message(dill.dumps(message['data']))
        signed_message = {
            'message': message,
            'signature': self._sign_message(dill.dumps(message['data'])),
            'public_key': self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
        }
        return self._send_to_server(leaderAddr, signed_message)
    
    def _send_to_follower(self, followerAddr, message):
        # to the follower, only send the public key, no message
        print(f'sending follower {followerAddr} the public key')
        msgType = message['type']
        public_key_message = {
            'type': msgType,
            'public_key': self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
        }
        return self._send_to_server(followerAddr, public_key_message)
    
    def _receive_response(self, sock):
        buff = bytes()
        while True:
            block = sock.recv(128)
            if not block:
                break
            buff += block
        if not buff:
            raise ValueError("No data received from server")
        return dill.loads(buff)

    def _get_state(self):
        """Retrive remote state machine."""
        if "cluster" in self.data:
            if not hasattr(self, 'server_address'):
                self.server_address = tuple(random.choice(self.data['cluster']))
        return self._request({'type': 'get'})

    def _append_log(self, payload):
        """Append to remote log."""
        return self._request({'type': 'append', 'data': payload})

    def _set_encryption_keys(self):
        """Get encryption keys."""
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()
        return self.private_key, self.public_key
    
    def _sign_message(self, message):
        """Sign the message."""
        return self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

    @property
    def diagnostic(self):
        return self._request({'type': 'diagnostic'})

    def config_cluster(self, action, address, port):
        return self._request({'type': 'config', 'action': action,
                              'address': address, 'port': port})
