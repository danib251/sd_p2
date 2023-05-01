from concurrent import futures
from multiprocessing import Process, Queue

import grpc

from KVStore.kvstorage.kvstorage import KVStorageServicer, KVStorageSimpleService
from KVStore.protos import kv_store_pb2_grpc, kv_store_shardmaster_pb2_grpc

HOSTNAME: str = "localhost"


def _run(end_queue: Queue, storage_server_port: int, shardmaster_port: int):
    address: str = "%s:%d" % (HOSTNAME, storage_server_port)

    storage_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = KVStorageServicer(KVStorageSimpleService())
    kv_store_pb2_grpc.add_KVStoreServicer_to_server(servicer, storage_server)

    # listen on port 50051
    print("KV Storage server listening on: %s" % address)
    storage_server.add_insecure_port(address)
    storage_server.start()

    # open a gRPC channel
    channel = grpc.insecure_channel(f'localhost:{shardmaster_port}')
    # create a stub (client)
    stub = kv_store_shardmaster_pb2_grpc.ShardMasterStub(channel)
    stub.Join(storage_server_port)

    try:
        end_queue.get(block=True, timeout=240)
        stub.Leave()
    except KeyboardInterrupt:
        storage_server.stop(0)
    except EOFError:
        storage_server.stop(0)


def run(port: int, shardmaster_port: int) -> Queue:
    end_queue = Queue()
    server_proc = Process(target=_run, args=[end_queue, port, shardmaster_port])
    server_proc.start()
    return end_queue