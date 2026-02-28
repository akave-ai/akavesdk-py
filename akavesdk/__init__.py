import sys
import os

# Add private directory to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRIVATE_PATH = os.path.join(PROJECT_ROOT, "private")
if PRIVATE_PATH not in sys.path:
    sys.path.append(PRIVATE_PATH)

# Import and expose main SDK classes
from sdk.sdk import SDK, BucketCreateResult, Bucket, SDKError, SDKConfig
from sdk.sdk_ipc import IPC
from private.cids import verify_raw, verify, CIDError


# Make SDKError appear under akavesdk in tracebacks
SDKError.__module__ = "akavesdk"

# Define what gets imported with "from akavesdk import *"
__all__ = ["SDK", "SDKError", "SDKConfig", "IPC", 
           "BucketCreateResult", "Bucket", "verify_raw", "verify", "CIDError"]