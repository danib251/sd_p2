import logging
from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from KVStore.kvstorage.kvstorage import KVStorageSimpleService 
import grpc
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
                num_servers = len(self.servers)

                # Calculate lower and upper value for the new server's shard
                shard_size = KEYS_UPPER_THRESHOLD // num_servers
                lower_val = (num_servers - 1) * shard_size
                upper_val = KEYS_UPPER_THRESHOLD if num_servers == 1 else lower_val + shard_size - 1
                
                # create a RedistributeRequest protobuf message with the necessary parameters
                request = RedistributeRequest(
                    destination_server=server,
                    lower_val=lower_val,
                    upper_val=upper_val
                )
                self.channel = grpc.insecure_channel(server)
                self.stub = KVStoreStub(self.channel)
                response = self.stub.Redistribute(request)
                # make the RPC call to the Redistribute function using the gRPC client
                

                

        def leave(self, server: str):
            if server in self.servers:
                # Remove server from the list
                self.servers.remove(server)
                
                '''# Update shard ranges
                shard_size = KEYS_UPPER_THRESHOLD // len(self.servers)
                lower_val = 0
                
                for idx, s in enumerate(self.servers):
                    upper_val = lower_val + shard_size
                    if idx == len(self.servers) - 1:
                        upper_val = KEYS_UPPER_THRESHOLD
                        
                    # Redistribute shards
                    KVStorageSimpleService.redistribute(s, lower_val, upper_val)
                    lower_val = upper_val

                

        def query(self, key: int) -> str:
            num_servers = len(self.servers)
            if num_servers == 0:
                raise ValueError("There are no servers in the system")

            shard_size = (1 << 32) // num_servers  # size of each shard
            shard_index = key // shard_size  # index of the shard that contains the key

            # return the address of the server that owns the shard
            return self.servers[shard_index % num_servers]'''


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
