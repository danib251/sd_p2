import logging
#from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

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

        def join(self, server: str):
            if server not in self.servers:
                self.servers.append(server)
                # TODO: redistribute keys based on the new number of servers

        def leave(self, server: str):
            if server in self.servers:
                self.servers.remove(server)
                # TODO: redistribute keys based on the new number of servers

        def query(self, key: int) -> str:
            num_servers = len(self.servers)
            if num_servers == 0:
                raise ValueError("There are no servers in the system")

            shard_size = (1 << 32) // num_servers  # size of each shard
            shard_index = key // shard_size  # index of the shard that contains the key

            # return the address of the server that owns the shard
            return self.servers[shard_index % num_servers]


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service
        """
        To fill with your code
        """

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
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        """
        To fill with your code
        """
