import grpc

from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub


class SimpleClient:
    def __init__(self, shard_master_address: str):
        """
        To fill with your code
        """
        self.channel = grpc.insecure_channel('localhost:50051')
        self.stub = ShardMasterStub(self.channel)

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
