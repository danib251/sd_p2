import time
import random
from typing import Dict, Union, List
import logging
import grpc

from KVStore.protos import kv_store_pb2
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub
from google.protobuf import empty_pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from KVStore.protos.kv_store_shardmaster_pb2 import Role
import threading

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
        self.storage = {}
        self.lock = threading.Lock()

    def get(self, key: int) -> Union[str, None]:
        with self.lock:
            if key in self.storage:
                return self.storage[key]
            else:
                return None

    def l_pop(self, key: int) -> Union[str, None]:
        with self.lock:
            if key not in self.storage:
                return None
            elif len(self.storage[key]) == 0:
                return ""
            else:
                return self.storage[key].pop(0)

    def r_pop(self, key: int) -> Union[str, None]:
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
            if key in self.storage:
                self.storage[key] = value + self.storage[key]
            else:
                self.storage[key] = value
                

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

    def transfer(self, keys_values: List[KeyValue]):
        for key, value in keys_values:
            self.storage[key] = value


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
        key = request.key
        value = self.storage_service.get(key)
        if value is None:
            return GetResponse()
        return GetResponse(value=value)


    def LPop(self, request: kv_store_pb2.GetRequest, context) -> kv_store_pb2.GetRequest:
        key = request.key
        value = self.storage_service.l_pop(key)
        if value is not None:
            return kv_store_pb2.GetRequest(value=value)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Key not found.")
            return kv_store_pb2.GetRequest()

    def RPop(self, request: kv_store_pb2.GetRequest, context) -> kv_store_pb2.GetRequest:
        key = request.key
        value = self.storage_service.r_pop(key)
        if value is not None:
            return kv_store_pb2.GetRequest(value=value)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Key not found.")
            return kv_store_pb2.GetRequest()

    def Put(self, request: kv_store_pb2.PutRequest, context) -> empty_pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.put(key, value)
        return empty_pb2.Empty()

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.append(key, value)
        return google_dot_protobuf_dot_empty__pb2.Empty()
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
