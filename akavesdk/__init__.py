import os
import sys

# Add private directory to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRIVATE_PATH = os.path.join(PROJECT_ROOT, "private")
if PRIVATE_PATH not in sys.path:
    sys.path.append(PRIVATE_PATH)

from private.cids import CIDError, verify, verify_raw

# Import and expose main SDK classes
from sdk.sdk import SDK, Bucket, BucketCreateResult, SDKConfig, SDKError
from sdk.sdk_ipc import IPC

# Make SDKError appear under akavesdk in tracebacks
SDKError.__module__ = "akavesdk"

# Define what gets imported with "from akavesdk import *"
__all__ = ["SDK", "SDKError", "SDKConfig", "IPC", "BucketCreateResult", "Bucket", "verify_raw", "verify", "CIDError"]
