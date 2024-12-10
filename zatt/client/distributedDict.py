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
                 refresh_policy=RefreshPolicyAlways(), mode="raft"):
        super().__init__()
        # pdb.set_trace()
        self.data['cluster'] = [(addr, port)]
        self.data['mode'] = mode
        print("self.data['cluster']:", self.data['cluster'])
        self.append_retry_attempts = append_retry_attempts
        self.refresh_policy = refresh_policy
        self.mode = mode  # Add mode parameter to control behavior
        if self.mode != "raft":
            print("WARNING: Raft functionality is disabled. Mode is not 'raft'.")
        self.refresh(force=True)

    def __getitem__(self, key):
        """Retrieve an item. Only works in raft mode."""
        self._check_mode()  # Ensure raft mode is enabled
        self.refresh()  # Refresh state if in raft mode
        return self.data[key]

    def __setitem__(self, key, value):
        """Set an item. Only works in raft mode."""
        self._check_mode()  # Ensure raft mode is enabled
        # Only proceed if the mode is "raft"
        if self.mode == "raft":
            self._append_log({'action': 'change', 'key': key, 'value': value})
        else:
            print("Not in raft mode. Skipping __setitem__.")

    def __delitem__(self, key):
        """Delete an item. Only works in raft mode."""
        self._check_mode()  # Ensure raft mode is enabled
        self.refresh(force=True)
        del self.data[self.__keytransform__(key)]
        if self.mode == "raft":
            self._append_log({'action': 'delete', 'key': key})
        else:
            print("Not in raft mode. Skipping __delitem__.")

    def __keytransform__(self, key):
        """Transform key. Works in all modes."""
        self._check_mode()  # Ensure raft mode is enabled
        return key

    def __repr__(self):
        """Represent the dictionary. Only works in raft mode."""
        self._check_mode()  # Ensure raft mode is enabled
        self.refresh()  # Refresh state if in raft mode
        return super().__repr__()

    def refresh(self, force=False):
        """_summary_
            This refresh function does a bunch of things:
            1. If no server_address, set the server_address to a random server in the cluster
            2. Additional for FP: set up the public and private keys of the client here, if not already set
            3. Client should know who is the current leader, and what are the other servers in the cluster
        Args:
            force (bool, optional): _description_. Defaults to False.
        
        Refreshes the client state based on the mode."""
        self._check_mode()  # Ensure raft mode is enabled
        if self.mode != "raft":
            raise RuntimeError("Raft functionality is disabled. Cannot refresh state.")

        # pdb.set_trace()
        if force or not (self.private_key and self.public_key):
            self.private_key, self.public_key = self._set_encryption_keys()
        if force or self.refresh_policy.can_update():
            self.data = self._get_state()
        if "leader" in self.data:
            self.currLeader = tuple(self.data['leader'])
        if "cluster" in self.data:
            self.followers = [server for server in self.data['cluster'] if server != self.currLeader]
    
        
    def _append_log(self, payload):
        """Only append log if mode is "raft"."""
        self._check_mode()  # Ensure raft mode is enabled
        if self.mode == "raft":
            for attempt in range(self.append_retry_attempts):
                response = super()._append_log(payload)
                if response['success']:
                    break
            return response
        else:
            print("Not in raft mode. Skipping log append.")
            
    def _request(self, message):
        """Override _request to check mode before processing requests."""
        self._check_mode()  # Ensure raft mode is enabled
        if self.mode == "raft":
            return super()._request(message)
        else:
            print("Not in raft mode. Skipping request.")
            return {"success": False, "error": "Not in raft mode"}
        
    def _check_mode(self):
        """Checks if the mode is 'raft'. Raises an exception if not."""
        if self.mode != "raft":
            raise RuntimeError("Raft functionality is disabled. Operation not allowed in this mode.")


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        d = DistributedDict('127.0.0.1', 9111)
        d[sys.argv[1]] = sys.argv[2]
