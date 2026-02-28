from web3 import Web3
import json

from sdk.sdk_ipc import IPC
from sdk.config import SDKConfig

class MockStorage:
    def get_file_index_by_id(self, opts, bucket, file_id):
        # web3 wrapper behavior
        return 2

class MockAuth:
    address = "0x00"

class MockIPC:
    storage = MockStorage()
    auth = MockAuth()

ipc = IPC(None, None, MockIPC(), SDKConfig(address=""))
try:
    print(ipc.ipc.storage.get_file_index_by_id({}, "bucket", "file"))
except Exception as e:
    print(e)
