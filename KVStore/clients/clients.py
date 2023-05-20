from typing import Union, Dict
import grpc
import logging
from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse, AppendRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None


class SimpleClient:
    def __init__(self, kvstore_address: str):
        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)

    def get(self, key: int) -> Union[str, None]:
        request = GetRequest(key=key)
        response = self.stub.Get(request)
        return _get_return(response)

    def l_pop(self, key: int) -> Union[str, None]:
        request = GetRequest(key=key)
        response = self.stub.LPop(request)
        return _get_return(response)

    def r_pop(self, key: int) -> Union[str, None]:
        request = GetRequest(key=key)
        response = self.stub.RPop(request)
        return _get_return(response)

    def put(self, key: int, value: str):
        request = PutRequest(key=key, value=value)
        self.stub.Put(request)

    def append(self, key: int, value: str):
        request = AppendRequest(key=key, value=value)
        self.stub.Append(request)

    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        """
        To fill with your code
        """

    def get(self, key: int) -> Union[str, None]:
        # Query shard master for destination server
        request = QueryRequest(key=key)
        response = self.stub.Query(request)

        # Direct storage request to destination server
        if response.server:
            destination_server = response.server
            with grpc.insecure_channel(destination_server) as channel:
                storage_stub = KVStoreStub(channel)
                get_request = GetRequest(key=key)
                get_response = storage_stub.Get(get_request)
                return _get_return(get_response)

    def l_pop(self, key: int) -> Union[str, None]:
        # Query shard master for destination server
        request = QueryRequest(key=key)
        response = self.stub.Query(request)

        # Direct storage request to destination server
        if response.server:
            destination_server = response.server
            with grpc.insecure_channel(destination_server) as channel:
                storage_stub = KVStoreStub(channel)
                l_pop_request = GetRequest(key=key)
                l_pop_response = storage_stub.LPop(l_pop_request)
                return _get_return(l_pop_response)

    def r_pop(self, key: int) -> Union[str, None]:
        # Query shard master for destination server
        request = QueryRequest(key=key)
        response = self.stub.Query(request)

        # Direct storage request to destination server
        if response.server:
            destination_server = response.server
            with grpc.insecure_channel(destination_server) as channel:
                storage_stub = KVStoreStub(channel)
                r_pop_request = GetRequest(key=key)
                r_pop_response = storage_stub.RPop(r_pop_request)
                return _get_return(r_pop_response)

    def put(self, key: int, value: str):
        # Query shard master for destination server
        request = QueryRequest(key=key)
        response = self.stub.Query(request)

        # Direct storage request to destination server
        if response.server:
            destination_server = response.server
            with grpc.insecure_channel(destination_server) as channel:
                storage_stub = KVStoreStub(channel)
                put_request = PutRequest(key=key, value=value)
                storage_stub.Put(put_request)

    def append(self, key: int, value: str):
        request = QueryRequest(key=key)
        response = self.stub.Query(request)

        # Direct storage request to destination server
        if response.server:
            destination_server = response.server
            with grpc.insecure_channel(destination_server) as channel:
                storage_stub = KVStoreStub(channel)
                appened_request = AppendRequest(key=key, value=value)
                storage_stub.Append(appened_request)


class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        request = QueryReplicaRequest(key=key)
        response = self.stub.QueryReplica(request)
        server = response.server

        if server:
            with grpc.insecure_channel(server) as channel:
                storage_stub = KVStoreStub(channel)
                get_request = GetRequest(key=key)
                get_response = storage_stub.Get(get_request)
                return _get_return(get_response)
        else:
            return None

    def l_pop(self, key: int) -> Union[str, None]:
        request = QueryReplicaRequest(key=key)
        response = self.stub.QueryReplica(request)
        server = response.server

        if server:
            with grpc.insecure_channel(server) as channel:
                storage_stub = KVStoreStub(channel)
                l_pop_request = GetRequest(key=key)
                l_pop_response = storage_stub.LPop(l_pop_request)
                return _get_return(l_pop_response)
        else:
            return None

    def r_pop(self, key: int) -> Union[str, None]:
        request = QueryReplicaRequest(key=key)
        response = self.stub.QueryReplica(request)
        server = response.server

        if server:
            with grpc.insecure_channel(server) as channel:
                storage_stub = KVStoreStub(channel)
                r_pop_request = GetRequest(key=key)
                r_pop_response = storage_stub.RPop(r_pop_request)
                return _get_return(r_pop_response)
        else:
            return None

    def put(self, key: int, value: str):
        request = QueryReplicaRequest(key=key)
        response = self.stub.QueryReplica(request)
        server = response.server

        if server:
            with grpc.insecure_channel(server) as channel:
                storage_stub = KVStoreStub(channel)
                put_request = PutRequest(key=key, value=value)
                storage_stub.Put(put_request)

    def append(self, key: int, value: str):
        request = QueryReplicaRequest(key=key)
        response = self.stub.QueryReplica(request)
        server = response.server

        if server:
            with grpc.insecure_channel(server) as channel:
                storage_stub = KVStoreStub(channel)
                append_request = AppendRequest(key=key, value=value)
                storage_stub.Append(append_request)
