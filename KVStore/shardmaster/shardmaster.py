import google

from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, LeaveRequest, QueryResponse, JoinRequest, Operation, \
    JoinReplicaResponse, Role
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer


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
        """
        To fill with your code
        """

    def join(self, server: str):
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def query(self, key: int) -> str:
        """
        To fill with your code
        """


class ShardMasterReplicasService(ShardMasterService):
    def __init__(self, number_of_shards: int):
        self.number_of_shards = number_of_shards
        """
        To fill with your code
        """

    def join(self, server: str):
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def query(self, key: int) -> str:
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

    def Join(self, request: JoinRequest, context) -> google.protobuf.Empty:
        """
        To fill with your code
        """

    def Leave(self, request: LeaveRequest, context) -> google.protobuf.Empty:
        """
        To fill with your code
        """

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        """
        To fill with your code
        """

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryRequest, context) -> QueryResponse:
        """
        To fill with your code
        """
