from KVStore.shardmaster import start_shardmaster
from KVStore.kvstorage import start_storage_server_sharded
from KVStore.clients.clients import ShardClient
from KVStore.tests.simple_shardkv import *
from KVStore.tests.sharded.shardkv_append import ShardkvAppendTests

SHARDMASTER_PORT = 52003
STORAGE_PORTS = [52004 + i for i in range(20)]
NUM_CLIENTS = 2
NUM_STORAGE_SERVERS = [2, 4, 8]

print("Tests with different shardmasters")
for num_servers in NUM_STORAGE_SERVERS:
    print(f"{num_servers} storage servers.")
    server_proc = start_shardmaster.run(SHARDMASTER_PORT)

    storage_proc_end_queues = [start_storage_server_sharded.run(STORAGE_PORTS[i], SHARDMASTER_PORT) for i in
                               range(num_servers)]

    client = ShardClient(f"localhost:{SHARDMASTER_PORT}")
    test1 = SimpleKVStoreTests(client)
    test1.test()
    client.stop()

    clients = [ShardClient(f"localhost:{SHARDMASTER_PORT}") for _ in range(NUM_CLIENTS)]
    test2 = SimpleKVStoreParallelTests(clients)
    test2.test()
    [client.stop() for client in clients]

    [queue.put(0) for queue in storage_proc_end_queues]
    server_proc.terminate()

print("Tests redistributions 1")
#  Test if the system supports dynamic removal of shards
num_servers = 5
server_proc = start_shardmaster.run(SHARDMASTER_PORT)
storage_proc_end_queues = [start_storage_server_sharded.run(STORAGE_PORTS[i], SHARDMASTER_PORT)
                           for i in range(num_servers)]
clients = [ShardClient(f"localhost:{SHARDMASTER_PORT}") for _ in range(NUM_CLIENTS)]

for i in range(num_servers - 1):
    print(f"{num_servers} storage servers.")
    test2 = SimpleKVStoreParallelTests(clients)
    test2.test()

    queue = storage_proc_end_queues[i]
    queue.put(0)

server_proc.terminate()
storage_proc_end_queues[num_servers - 1].put(0)

print("Test redistribution 2 (keep data after redistribution)")
# Test if data is redistributes across shards when the number of nodes changes
num_servers = 5
server_proc = start_shardmaster.run(SHARDMASTER_PORT)
storage_proc_end_queues = [start_storage_server_sharded.run(STORAGE_PORTS[i], SHARDMASTER_PORT) for i in
                           range(num_servers)]
client = ShardClient(f"localhost:{SHARDMASTER_PORT}")

for i in range(num_servers - 1):
    print(f"{num_servers} storage servers.")

    test2 = ShardkvAppendTests(client)
    test2.test(i)

    queue = storage_proc_end_queues[0]
    queue.put(0)
    storage_proc_end_queues = storage_proc_end_queues[1:]

for i in range(num_servers):
    print(f"{num_servers} storage servers.")

    test2 = ShardkvAppendTests(client)
    test2.test(i + 4)

    storage_proc_end_queues.append(start_storage_server_sharded.run(STORAGE_PORTS[i], SHARDMASTER_PORT))

[queue.put(0) for queue in storage_proc_end_queues]
server_proc.terminate()
