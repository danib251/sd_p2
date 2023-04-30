import time

from KVStore.clients.clients import ShardReplicaClient
from KVStore.kvstorage import start_storage_server_replicas
from KVStore.shardmaster import start_shardmaster_replicas
from KVStore.tests.replication.replication_performance import ShardKVReplicationPerformanceTest
from tabulate import tabulate

SHARDMASTER_PORT = 52003
STORAGE_PORTS = [52004 + i for i in range(20)]

NUM_CLIENTS = 2
NUM_SHARDS = 2
NUM_STORAGE_SERVERS = 6
CONSISTENCY_LEVELS = [0, 1, 2]

print("Testing throughput (OP/s) and number of errors for different consistency levels")
print("Configuration:")
print("\tNumber of clients: %d" % NUM_CLIENTS)
print("\tNumber of shards: %d" % NUM_SHARDS)
print("\tStorage servers: %d" % NUM_STORAGE_SERVERS)

results = []

for consistency_level in CONSISTENCY_LEVELS:
    print(f"Running with consistency level {consistency_level}.")

    server_proc = start_shardmaster_replicas.run(SHARDMASTER_PORT, NUM_SHARDS)
    storage_proc_end_queues = [start_storage_server_replicas.run(STORAGE_PORTS[i], SHARDMASTER_PORT, consistency_level)
                               for i in range(NUM_STORAGE_SERVERS)]

    clients = [ShardReplicaClient(f"localhost:{SHARDMASTER_PORT}") for _ in range(NUM_CLIENTS)]

    test = ShardKVReplicationPerformanceTest(clients)
    throughput, error_rate = test.test()
    results.append([consistency_level, throughput, error_rate])

    server_proc.terminate()
    [queue.put(0) for queue in storage_proc_end_queues]

    time.sleep(1)

print("Final results:")
# Show results in table
print(tabulate(results, headers=['Consistency_level', 'Throughput (OP/s)', 'Error rate (Errors/s)']))
