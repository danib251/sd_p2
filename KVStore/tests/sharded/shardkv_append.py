from KVStore.clients.clients import SimpleClient
from KVStore.tests.utils import test_get, test_append

"""
Tests on simple storage requests on a single storage server.
"""

DATA = "MUDA "


class ShardkvAppendTests:

    def __init__(self, client: SimpleClient):
        self.client = client

    def test(self, num_iter: int):
        assert (test_append(self.client, 81, DATA))
        assert (test_get(self.client, 81, DATA * (num_iter + 1)))
