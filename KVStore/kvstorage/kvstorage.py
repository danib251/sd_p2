from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer

from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 5


class KVStorageService:

    def __init__(self):
        pass

    def get(self, key: int) -> str:
        pass

    def l_pop(self, key: int) -> str:
        pass

    def r_pop(self, key: int) -> str:
        pass

    def put(self, key: int, value: str):
        pass

    def append(self, key: int, value: str):
        pass

    def redistribute(self, destinationkeys_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        """
        To fill with your code
        """

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

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        """
        To fill with your code
        """

    def transfer(self, keys_values: list):
        """
        To fill with your code
        """


class KVStorageReplicasService(KVStorageService):
    role: Role

    def __init__(self, consistency_level: int):
        self.consistency_level = consistency_level
        """
        To fill with your code
        """

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

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        """
        To fill with your code
        """

    def transfer(self, keys_values: list):
        """
        To fill with your code
        """

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context):
        """
        To fill with your code
        """

    def LPop(self, request: GetRequest, context):
        """
        To fill with your code
        """

    def RPop(self, request: GetRequest, context):
        """
        To fill with your code
        """

    def Put(self, request: PutRequest, context):
        """
        To fill with your code
        """

    def Append(self, request: AppendRequest, context):
        """
        To fill with your code
        """

    def Redistribute(self, request: RedistributeRequest, context):
        """
        To fill with your code
        """

    def Transfer(self, request: TransferRequest, context):
        """
        To fill with your code
        """

    def AddReplica(self, request: ServerRequest, context):
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context):
        """
        To fill with your code
        """
