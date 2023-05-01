from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer
import threading
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
        self.storage = {}
        self.lock = threading.Lock()

    def get(self, key: int) -> str:
        with self.lock:
            if key in self.storage:
                return self.storage[key]
            else:
                return None

    def l_pop(self, key: int) -> str:
        with self.lock:
            if key not in self.storage:
                return None
            elif len(self.storage[key]) == 0:
                return ""
            else:
                return self.storage[key].pop(0)

    def r_pop(self, key: int) -> str:
        with self.lock:
            if key not in self.storage:
                return None
            elif len(self.storage[key]) == 0:
                return ""
            else:
                return self.storage[key].pop()

    def put(self, key: int, value: str):
        with self.lock:
            self.storage[key] = value

    def append(self, key: int, value: str):
        with self.lock:
            if key in self.data:
                self.data[key] = value + self.data[key]
            else:
                self.data[key] = value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        keys_to_remove = []
        keys_to_transfer = {}
        for key, value in self.storage.items():
            if lower_val <= key <= upper_val:
                keys_to_transfer[key] = value
                keys_to_remove.append(key)
        for key in keys_to_remove:
            del self.storage[key]
        try:
            with SimpleClient(destination_server) as client:
                client.transfer(keys_to_transfer)
        except Exception as e:
            # Re-insert transferred keys in case of error
            for key, value in keys_to_transfer.items():
                self.storage[key] = value
            raise e

    def transfer(self, keys_values: list):
        for key, value in keys_values:
            self.storage[key] = value

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
