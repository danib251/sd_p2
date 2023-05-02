import threading
import time
import random
from typing import Dict, Union, List
import logging
import grpc
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub

from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


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

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):
    def __init__(self):
        super().__init__()
        self.data = {}
        self.lock = threading.Lock()

    def get(self, key: int) -> Union[str, None]:
        with self.lock:
            if key in self.data:
                return self.data[key]
            else:
                return None

    def l_pop(self, key: int) -> Union[str, None]:
        with self.lock:
            if key not in self.data:
                return None

            value = self.data[key]

            if len(value) == 0:
                return ""

            popped_char = value[0]
            if len(value) == 1:
                self.data[key] = ""
            else:
                self.data[key] = value[1:]

            return popped_char

    def r_pop(self, key: int) -> Union[str, None]:
        with self.lock:
            if key not in self.data:
                return None

            value = self.data[key]

            if len(value) == 0:
                return ""

            popped_char = value[-1]
            if len(value) == 1:
                self.data[key] = ""
            else:
                self.data[key] = value[:-1]

            return popped_char

    def put(self, key: int, value: str):
        with self.lock:
            self.data[key] = value

    def append(self, key: int, value: str):
        with self.lock:
            existing_value = self.data.get(key)
            if existing_value:
                self.data[key] = value + existing_value
            else:
                self.data[key] = value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        """
        To fill with your code
        """

    def transfer(self, keys_values: List[KeyValue]):
        """
        To fill with your code
        """


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
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

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:
        """
        To fill with your code
        """

    def LPop(self, request: GetRequest, context) -> GetResponse:
        """
        To fill with your code
        """

    def RPop(self, request: GetRequest, context) -> GetResponse:
        """
        To fill with your code
        """

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """
