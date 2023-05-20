import logging
from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from KVStore.kvstorage.kvstorage import KVStorageSimpleService
import grpc
import threading

logger = logging.getLogger(__name__)


class ShardMasterService:
    def join(self, server: str):
        pass

    def leave(self, server: str):
        pass

    def query(self, key: int) -> str:
        pass

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self.servers = []  # list of servers' addresses
        self.lock = threading.Lock()

    def join(self, server: str):
        with self.lock:
            if server not in self.servers:
                self.servers.append(server)
                num_servers = len(self.servers)
                if num_servers != 1:
                    # Calculate lower and upper value for the new server's shard
                    shard_size = KEYS_UPPER_THRESHOLD // num_servers
                    last_shard_size = KEYS_UPPER_THRESHOLD // (num_servers - 1)
                    # Redistribute the keys of all servers
                    for i, s in enumerate(self.servers[:-2]):
                        lower_val = i * shard_size
                        upper_val = (i + 1) * shard_size - 1 if i != num_servers - 1 else KEYS_UPPER_THRESHOLD
                        max_upper_val = (i + 1) * last_shard_size
                        request = RedistributeRequest(
                            destination_server=self.servers[i + 1],
                            lower_val=upper_val,
                            upper_val=max_upper_val
                        )
                        self.channel = grpc.insecure_channel(s)
                        self.stub = KVStoreStub(self.channel)
                        response = self.stub.Redistribute(request)

                    # create a RedistributeRequest protobuf message with the necessary parameters
                    request = RedistributeRequest(
                        destination_server=server,
                        lower_val=(KEYS_UPPER_THRESHOLD) - shard_size,
                        upper_val=KEYS_UPPER_THRESHOLD
                    )
                    self.channel = grpc.insecure_channel(self.servers[-2])
                    self.stub = KVStoreStub(self.channel)
                    response = self.stub.Redistribute(request)
            print("servers created: ", server)
            print("self.servers created: ", self.servers)

    def leave(self, server: str):
        with self.lock:
            if server not in self.servers:
                raise ValueError("The server is not present in the system")
            posicion = self.servers.index(server)
            num_servers = len(self.servers)

            last_shard_size = KEYS_UPPER_THRESHOLD // num_servers
            # print("self.servers: ", len(self.servers))
            if num_servers != 1:
                shard_size = KEYS_UPPER_THRESHOLD // (num_servers - 1)
                # Redistribute the keys of all servers in reverse order
                sublist = self.servers[:-2]
                for i, s in enumerate(sublist):
                    upper_val = (i + 1) * last_shard_size - 1
                    max_upper_val = (i + 1) * last_shard_size + (i + 1) * shard_size - (i + 1) * last_shard_size - 1
                    destination_server = self.servers[i + 1]
                    request = RedistributeRequest(
                        destination_server=s,
                        lower_val=upper_val,
                        upper_val=max_upper_val
                    )
                    self.channel = grpc.insecure_channel(self.servers[i])
                    self.stub = KVStoreStub(self.channel)
                    response = self.stub.Redistribute(request)

                # Perform redistribution for the last server
                request = RedistributeRequest(
                    destination_server=self.servers[-2],
                    lower_val=KEYS_UPPER_THRESHOLD - last_shard_size,
                    upper_val=KEYS_UPPER_THRESHOLD
                )

                self.channel = grpc.insecure_channel(server)
                self.stub = KVStoreStub(self.channel)
                response = self.stub.Redistribute(request)

                self.servers.remove(server)
            else:
                self.servers.remove(server)
            print("servers deleted: ", server)
            print("self.servers deleted : ", self.servers)

    def query(self, key: int) -> str:
        num_servers = len(self.servers)
        if num_servers == 0:
            raise ValueError("There are no servers in the system")

        shard_size = KEYS_UPPER_THRESHOLD // num_servers  # size of each shard (considerando un máximo de 100 keys)
        shard_index = key // shard_size  # index of the shard that contains the key

        # return the address of the server that owns the shard
        ''' print ("num_servers: ", num_servers)
            print("key: ", key)
            print("shard_index: ", shard_index)'''

        return self.servers[shard_index % num_servers]


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        self.number_of_shards = number_of_shards
        self.replicas = []  # List to store the replica servers

    def leave(self, replica: str):
        with self.lock:
            if replica not in self.replicas:
                raise ValueError("The replica is not present in the system")

            num_replicas = len(self.replicas)

            if num_replicas == 1:
                raise ValueError("Cannot remove the last replica")

            shard_size = KEYS_UPPER_THRESHOLD // (num_replicas - 1)
            shard_index = self.replicas.index(replica)

            # Calculate the range of keys to redistribute
            lower_val = shard_index * shard_size
            upper_val = (shard_index + 1) * shard_size - 1 if shard_index != num_replicas - 1 else KEYS_UPPER_THRESHOLD

            # Redistribute the keys from the leaving replica to the remaining replicas
            for i, destination_replica in enumerate(self.replicas):
                if i != shard_index:
                    max_upper_val = (i + 1) * shard_size
                    request = RedistributeRequest(
                        destination_server=destination_replica,
                        lower_val=upper_val,
                        upper_val=max_upper_val
                    )
                    self.channel = grpc.insecure_channel(destination_replica)
                    self.stub = KVStoreStub(self.channel)
                    response = self.stub.Redistribute(request)

            self.replicas.remove(replica)
            print("Replica removed:", replica)
            print("Remaining replicas:", self.replicas)

    def join_replica(self, replica: str):
        with self.lock:
            if replica not in self.replicas:
                self.replicas.append(replica)
                num_replicas = len(self.replicas)
                if num_replicas != 1:
                    # Calculate shard size for the new replica
                    shard_size = KEYS_UPPER_THRESHOLD // num_replicas

                    # Redistribute the keys of all replicas
                    for i, destination_replica in enumerate(self.replicas[:-1]):
                        lower_val = i * shard_size
                        upper_val = (i + 1) * shard_size - 1 if i != num_replicas - 2 else KEYS_UPPER_THRESHOLD
                        max_upper_val = (i + 1) * shard_size
                        request = RedistributeRequest(
                            destination_server=destination_replica,
                            lower_val=upper_val,
                            upper_val=max_upper_val
                        )
                        self.channel = grpc.insecure_channel(destination_replica)
                        self.stub = KVStoreStub(self.channel)
                        response = self.stub.Redistribute(request)

                print("Replica server created:", replica)
                print("Current replicas:", self.replicas)


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_master_service.join(request.server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_master_service.leave(request.server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        server = self.shard_master_service.query(request.key)
        return QueryResponse(server=server)

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        server = request.server
        self.shard_master_service.join_replica(server)
        return JoinReplicaResponse()

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        key = request.key
        server = self.shard_master_service.query_replica(key)
        return QueryResponse(server=server)
