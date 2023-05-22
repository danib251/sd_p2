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
                value = self.storage[key][0]
                self.storage[key] = self.storage[key][1:]
                return value

    def r_pop(self, key: int) -> Union[str, None]:
        with self.lock:
            if key not in self.storage:
                return None
            elif len(self.storage[key]) == 0:
                return ""
            else:
                value = self.storage[key][-1]
                self.storage[key] = self.storage[key][:-1]
                return value

    def put(self, key: int, value: str):
        with self.lock:
            self.storage[key] = value

    def append(self, key: int, value: str):
        with self.lock:
            if key in self.storage:
                self.storage[key] = self.storage[key] + value  
            else:
                self.storage[key] = value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        with self.lock:
            print("Redistributing keys to server: %s" % destination_server)
            # Get the keys that should be moved to the destination server
            keys_to_move = []
            for key, value in self.storage.items():
                if lower_val <= key <= upper_val:
                    keys_to_move.append(key)

            # Create a list with the key-value pairs to be moved
            kv_pairs_to_move = []
            for key, value in self.storage.items():
                if lower_val <= key <= upper_val:
                    kv = KeyValue()
                    kv.key = key
                    kv.value = value
                    kv_pairs_to_move.append(kv)

            if kv_pairs_to_move:
                # Delete the keys from the local storage
                for key in keys_to_move:
                    del self.storage[key]

                # Connect to the destination server and transfer the keys
                with grpc.insecure_channel(destination_server) as channel:
                    storage_stub = KVStoreStub(channel)
                    transfer_request = TransferRequest(keys_values=kv_pairs_to_move)
                    storage_stub.Transfer(transfer_request)


    def transfer(self, keys_values: List[KeyValue]):
            for kv in keys_values:
                self.storage[kv.key] = kv.value
                

class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        self.storage = {}
        self.replicas = {}
        self.lock = threading.Lock()

    def l_pop(self, key: int) -> str:
        with self.lock:
            if key not in self.storage:
                raise KeyError("Key does not exist")

            value = self.storage[key]
            del self.storage[key]

            return value

    def r_pop(self, key: int) -> str:
        with self.lock:
            if key not in self.storage:
                raise KeyError("Key does not exist")

            value = self.storage[key]
            del self.storage[key]

            return value

    def put(self, key: int, value: str):
        with self.lock:
            self.storage[key] = value

    def append(self, key: int, value: str):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = value
            else:
                self.storage[key] += value

    def add_replica(self, server: str):
        with self.lock:
            if server in self.replicas:
                raise ValueError("The replica is already present")

            self.replicas[server] = True

    def remove_replica(self, server: str):
        with self.lock:
            if server not in self.replicas:
                raise ValueError("The replica is not present")

            del self.replicas[server]

    def set_role(self, role: Role):
        with self.lock:
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


    def LPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        value = self.storage_service.l_pop(key)
        if value is not None:
            return kv_store_pb2.GetResponse(value=value)
        else:
            return kv_store_pb2.GetResponse()

    def RPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        value = self.storage_service.r_pop(key)
        if value is not None:
            return kv_store_pb2.GetResponse(value=value)
        else:
            return kv_store_pb2.GetResponse()

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
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
        destination_server = request.destination_server
        lower_val = request.lower_val
        upper_val = request.upper_val
        self.storage_service.redistribute(destination_server, lower_val, upper_val)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        keys_values = request.keys_values
        self.storage_service.transfer(keys_values)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """
