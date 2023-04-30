import multiprocessing
import random
import string
import time
from typing import List

from KVStore.clients.clients import SimpleClient
from KVStore.tests.utils import test_get, test_put

"""
Tests on simple storage requests on a single storage server.
"""

EXEC_TIME = 10


class ShardKVReplicationPerformanceTest:

    def __init__(self, clients: List[SimpleClient]):
        if len(clients) > 3:
            Exception("Max 3 clients")
        self.clients = {client_id: clients[client_id] for client_id in range(len(clients))}

    def _test(self, process_id: int, return_dict) -> (float, float):

        client = self.clients[process_id]

        num_ops = 0
        num_errors = 0

        start_time = time.time()

        while time.time() - start_time < EXEC_TIME:

            key = random.randint(0, 100)
            value = ''.join(random.choices(string.ascii_uppercase +
                                           string.digits, k=4))

            test_put(client, key, value)
            res = test_get(client, key, value)

            num_ops += 1
            if res is False:
                num_errors += 1

        return_dict[process_id] = (num_ops / EXEC_TIME, num_errors / EXEC_TIME)

    def test(self):

        manager = multiprocessing.Manager()
        return_dict = manager.dict()

        procs = [multiprocessing.Process(target=self._test, args=[client_id, return_dict])
                 for client_id in range(len(self.clients))]
        [p.start() for p in procs]
        for proc in procs:
            proc.join()

        throughputs = [res[0] for res in return_dict.values()]
        error_rates = [res[1] for res in return_dict.values()]

        return sum(throughputs), sum(error_rates)
