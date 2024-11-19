import collections
import pdb
from zatt.client.abstractClient import AbstractClient
from zatt.client.refresh_policies import RefreshPolicyAlways
# import encryption stuff
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import utils



class DistributedDict(collections.UserDict, AbstractClient):
    """Client for zatt instances with dictionary based state machines."""
    def __init__(self, addr, port, append_retry_attempts=3,
                 refresh_policy=RefreshPolicyAlways()):
        super().__init__()
        self.data['cluster'] = [(addr, port)]
        print("self.data['cluster']:", self.data['cluster'])
        self.append_retry_attempts = append_retry_attempts
        self.refresh_policy = refresh_policy
        pdb.set_trace()
        self.refresh(force=True)
        pdb.set_trace()

    def __getitem__(self, key):
        self.refresh()
        return self.data[key]

    def __setitem__(self, key, value):
        self._append_log({'action': 'change', 'key': key, 'value': value})

    def __delitem__(self, key):
        self.refresh(force=True)
        del self.data[self.__keytransform__(key)]
        self._append_log({'action': 'delete', 'key': key})

    def __keytransform__(self, key):
        return key

    def __repr__(self):
        self.refresh()
        return super().__repr__()

    def refresh(self, force=False):
        """_summary_
            This refresh function does a bunch of things:
            1. If no server_address, set the server_address to a random server in the cluster
            2. Additional for FP: set up the public and private keys of the client here, if not already set
            3. Client should know who is the current leader, and what are the other servers in the cluster
        Args:
            force (bool, optional): _description_. Defaults to False.
        """
        if force or not (self.private_key and self.public_key):
            self.private_key, self.public_key = self._set_encryption_keys()
            # publicKeyStrForTesting = self.public_key.public_bytes(
            #     encoding=serialization.Encoding.PEM,
            #     format=serialization.PublicFormat.SubjectPublicKeyInfo
            # ).decode('utf-8')
            # privateKeyStrForTesting = self.private_key.private_bytes(
            #     encoding=serialization.Encoding.PEM,
            #     format=serialization.PrivateFormat.PKCS8,
            #     encryption_algorithm=serialization.NoEncryption()
            # ).decode('utf-8')
            # print(f"successfully set up keys, the public key is: {publicKeyStrForTesting}, and the private key is: {privateKeyStrForTesting}")
        if force or self.refresh_policy.can_update():
            self.data = self._get_state()
        if "leader" in self.data:
            self.currLeader = tuple(self.data['leader'])
        if "cluster" in self.data: 
            self.followers = [server for server in self.data['cluster'] if server != self.currLeader]
        
    
    def _append_log(self, payload):
        for attempt in range(self.append_retry_attempts):
            response = super()._append_log(payload)
            if response['success']:
                break
        # TODO: logging
        return response

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        d = DistributedDict('127.0.0.1', 9111)
        d[sys.argv[1]] = sys.argv[2]
