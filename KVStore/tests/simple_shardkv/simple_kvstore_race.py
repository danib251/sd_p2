import concurrent
from typing import List

from KVStore.clients.clients import SimpleClient
from KVStore.tests.utils import test_get, test_append

"""
Tests on parallel storage requests in a race condition on a single storage server.
"""

DATA = "ORA "
NUM_REQUESTS = 5


class SimpleKVStoreRaceTests:
    def __init__(self, clients: List[SimpleClient]):
        if len(clients) > 5:
            Exception("Max 5 clients")
        self.clients = {client_id: clients[client_id] for client_id in range(len(clients))}

    def _test(self, process_id: int):
        client = self.clients[process_id]
        for _ in range(NUM_REQUESTS):
            test_append(client, 15, DATA)

    def test(self):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(self._test, range(len(self.clients)))
        expected_result = DATA * NUM_REQUESTS * len(self.clients)
        assert (test_get(self.clients[0], 15, expected_result))
