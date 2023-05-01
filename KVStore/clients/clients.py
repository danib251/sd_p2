import grpc

from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub

class SimpleClient:
    def __init__(self, shard_master_address: str):
        
        self.channel = grpc.insecure_channel('localhost:50051')
        self.stub = ShardMasterStub(self.channel)
        
        self.channel2 = grpc.insecure_channel(shard_master_address)
        self.stub2 = KVStoreStub(self.channel2)

    def get(self, key: int) -> str:
        request = KVStoreStub()
        request.key = key
        response = self.stub2.get(request)
        if response.result == "":
            return None
        return response.result

    def l_pop(self, key: int) -> str:
        response = self.stub2.LPop(KVStoreStub.GetRequest(key=key))
        return response.value if response.value != '' else None

    def r_pop(self, key: int) -> str:
        response = self.stub.RPop(KVStoreStub.GetRequest(key=key))
        return response.value if response.value != '' else None

    def put(self, key: int, value: str):
        request = KVStoreStub()
        request.key = key
        request.value = value
        response = self.stub.put(request)
        return response.result

    def append(self, key: int, value: str):
        self.stub.Append(KVStoreStub.AppendRequest(key=key, value=value))


    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address):
        super().__init__(shard_master_address)

    def get(self, key: int) -> str:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
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


class ShardReplicaClient(SimpleClient):
    def __init__(self, shard_master_address):
        super().__init__(shard_master_address)

    def get(self, key: int) -> str:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
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
