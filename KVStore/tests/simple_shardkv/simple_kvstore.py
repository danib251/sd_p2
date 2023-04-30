from KVStore.clients.clients import SimpleClient
from KVStore.tests.utils import test_get, test_put, test_append, test_l_pop, test_r_pop

"""
Tests on simple storage requests on a single storage server.
"""


class SimpleKVStoreTests:
    def __init__(self, client: SimpleClient):
        self.client = client

    def test(self):
        assert (test_get(self.client, 10, None))

        assert (test_put(self.client, 33, "?!?!?"))
        assert (test_get(self.client, 33, "?!?!?"))

        assert (test_append(self.client, 45, "huh?"))
        assert (test_get(self.client, 45, "huh?"))
        assert (test_put(self.client, 45, "huh!"))
        assert (test_get(self.client, 45, "huh!"))
        assert (test_append(self.client, 45, "?"))
        assert (test_get(self.client, 45, "huh!?"))

        assert (test_l_pop(self.client, 3, None))
        assert (test_l_pop(self.client, 45, "h"))
        assert (test_r_pop(self.client, 45, "?"))
        assert (test_l_pop(self.client, 45, "u"))
        assert (test_r_pop(self.client, 45, "!"))
        assert (test_r_pop(self.client, 45, "h"))
        assert (test_l_pop(self.client, 45, ""))

        assert (test_get(self.client, 86, None))
        assert (test_get(self.client, 34, None))
        assert (test_append(self.client, 86, "URV_ROCKS"))
        assert (test_append(self.client, 34, "paxos_enjoyer"))
        assert (test_get(self.client, 86, "URV_ROCKS"))
        assert (test_get(self.client, 34, "paxos_enjoyer"))
