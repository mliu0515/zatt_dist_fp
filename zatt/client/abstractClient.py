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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)  # Set a timeout of 5 seconds
        try:
            # pdb.set_trace()
            sock.connect(self.server_address)
            print("connected to server_address:", self.server_address)
            # sock.send(msgpack.packb(message, use_bin_type=True))
            # Sign the message here
            message = {'message': message, 
                       'signature': self._sign_message(dill.dumps(message)), 
                       'public_key': self.public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo)}
            sock.send(dill.dumps(message))

            buff = bytes()
            while True:
                block = sock.recv(128)
                if not block:
                    break
                buff += block

            if not buff:
                raise ValueError("No data received from server")
            
            # resp = msgpack.unpackb(buff, raw=False)
            resp = dill.loads(buff)
        except socket.timeout:
            print('Timeout')
        finally:
            sock.close()
        if 'type' in resp and resp['type'] == 'redirect':
            self.server_address = tuple(resp['leader'])
            print("current leader:", self.server_address)
            resp = self._request(message)
        return resp

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
