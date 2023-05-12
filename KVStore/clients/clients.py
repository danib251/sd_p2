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
        result = _get_return(response)
        if result is not None:
            return response.value
        else:
            return None


    def l_pop(self, key: int) -> Union[str, None]:
        request = GetRequest(key=key)
        response = self.stub.LPop(request)
        result = _get_return(response)
        if result is not None:
            return response.value
        else:
            return None


    def r_pop(self, key: int) -> Union[str, None]:
        request = GetRequest(key=key)
        response = self.stub.RPop(request)
        result = _get_return(response)
        if result is not None:
            return response.value
        else:
            return None

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
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def put(self, key: int, value: str):
        """
        To fill with your code
        """


    def append(self, key: int, value: str):
        """
        To fill with your code
        """


class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def put(self, key: int, value: str):
        """
        To fill with your code
        """


    def append(self, key: int, value: str):
        """
        To fill with your code
        """

