"""
Protocol buffer definitions for Akave SDK.
"""

# Import protobuf modules to make them available
try:
    from . import ipcnodeapi_pb2
    from . import ipcnodeapi_pb2_grpc
    
    __all__ = [
        'ipcnodeapi_pb2',
        'ipcnodeapi_pb2_grpc'
    ]
except ImportError as e:
    # Handle protobuf import errors gracefully
    print(f"Warning: Could not import protobuf modules: {e}")
    __all__ = []
