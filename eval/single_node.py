from KVStore.kvstorage import start_storage_server
from KVStore.clients.clients import SimpleClient
from KVStore.tests.simple_shardkv import *

SERVER_PORT = 52003
NUM_CLIENTS = 3

server_proc = start_storage_server.run(SERVER_PORT)

client = SimpleClient(f"localhost:{SERVER_PORT}")
test1 = SimpleKVStoreTests(client)
test1.test()
client.stop()

clients = [SimpleClient(f"localhost:{SERVER_PORT}") for _ in range(NUM_CLIENTS)]
test2 = SimpleKVStoreParallelTests(clients)
test2.test()
[ client.stop() for client in clients]

clients = [SimpleClient(f"localhost:{SERVER_PORT}") for _ in range(NUM_CLIENTS)]
test2 = SimpleKVStoreRaceTests(clients)
test2.test()
[ client.stop() for client in clients]

server_proc.terminate()