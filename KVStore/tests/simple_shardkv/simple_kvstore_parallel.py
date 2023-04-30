import concurrent
import time
from typing import List

from KVStore.clients.clients import SimpleClient
from KVStore.tests.utils import test_get, test_put, test_append

"""
Tests on parallel storage requests on a single storage server.
"""

DATA = {
    0: "1.965.000.000",
    1: "75.000.000",
    2: "32.000.000",
    3: "24.000.000",
    4: "3.200.000"
}


class SimpleKVStoreParallelTests:
    def __init__(self, clients: List[SimpleClient]):
        if len(clients) > 5:
            Exception("Max 5 clients")
        self.clients = {client_id: clients[client_id] for client_id in range(len(clients))}

    def _test(self, process_id: int):
        client = self.clients[process_id]
        time.sleep(1)
        assert (test_put(client, process_id * 20, DATA[process_id]))
        assert (test_get(client, process_id * 20, DATA[process_id]))
        assert (test_append(client, process_id * 20, DATA[process_id]))
        assert (test_get(client, process_id * 20, DATA[process_id] + DATA[process_id]))
        assert (test_put(client, process_id * 20, ""))

    def test(self):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(self._test, range(len(self.clients)))
