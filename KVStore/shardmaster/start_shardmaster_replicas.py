import time
from concurrent import futures
from multiprocessing import Process

import grpc

from KVStore.protos import kv_store_shardmaster_pb2_grpc
from KVStore.shardmaster.shardmaster import ShardMasterServicer, ShardMasterReplicasService

HOSTNAME: str = "localhost"


def _run(port: int, number_of_shards: int):
    address: str = "%s:%d" % (HOSTNAME, port)

    master_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ShardMasterServicer(ShardMasterReplicasService(number_of_shards))
    kv_store_shardmaster_pb2_grpc.add_KVStoreServicer_to_server(servicer, master_server)

    # listen on port 50051
    print("KV Storage server listening on: %s" % address)
    master_server.add_insecure_port(address)
    master_server.start()

    try:
        time.sleep(3000)
    except KeyboardInterrupt:
        master_server.stop(0)
    except EOFError:
        master_server.stop(0)


def run(port: int, number_of_shards: int) -> Process:
    server_proc = Process(target=_run, args=[port, number_of_shards])
    server_proc.start()
    return server_proc
