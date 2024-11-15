import socket
import random
import msgpack
import pickle
import pdb

class AbstractClient:
    """Abstract client. Contains primitives for implementing functioning
    clients."""

    def _request(self, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)  # Set a timeout of 5 seconds
        try:
            # print("server_address:", self.server_address)
            sock.connect(self.server_address)
            print("connected to server_address:", self.server_address)
            # sock.send(msgpack.packb(message, use_bin_type=True))
            pdb.set_trace()
            sock.send(pickle.dumps(message))
            pdb.set_trace()

            buff = bytes()
            while True:
                block = sock.recv(128)
                if not block:
                    break
                buff += block
            # resp = msgpack.unpackb(buff, encoding='utf-8')
            # print("Received buffer length:", len(buff))
            # print("Received buffer content:", buff)

            if not buff:
                raise ValueError("No data received from server")
            
            # resp = msgpack.unpackb(buff, raw=False)
            resp = pickle.loads(buff)
            # resp = msgpack.unpackb(buff, encoding='utf-8')
        except socket.timeout:
            print('Timeout')
        finally:
            sock.close()
        if 'type' in resp and resp['type'] == 'redirect':
            self.server_address = tuple(resp['leader'])
            print("current leader:", self.server_address)
            # something is wrong with the server. It keeps redirecting for some reason
            # It keeps redirecting to itself. I found the bug:
            # debug message: My role is Follower. My address is: 127.0.0.1:5254. I am redirecting client 127.0.0.1:57603 to leader, who has the id ('127.0.0.1', 5254)
            # which means leader's type is not changed to Leader. It is still Follower. I need to fix this.
            resp = self._request(message)
        return resp

    def _get_state(self):
        """Retrive remote state machine."""
        self.server_address = tuple(random.choice(self.data['cluster']))
        # print("self.server_address:", self.server_address)
        # print("what the data looks like: ", self.data)
        return self._request({'type': 'get'})

    def _append_log(self, payload):
        """Append to remote log."""
        print("payload:", payload)
        return self._request({'type': 'append', 'data': payload})

    @property
    def diagnostic(self):
        return self._request({'type': 'diagnostic'})

    def config_cluster(self, action, address, port):
        return self._request({'type': 'config', 'action': action,
                              'address': address, 'port': port})
