import grpc
import time
import threading
from typing import Dict, Optional, Tuple, Callable
from private.pb import nodeapi_pb2_grpc, ipcnodeapi_pb2_grpc


class ConnectionPool:
    # by default , retry 3 times with exponential backoff.
    # The init function sets the retries and delay parameters.
    @staticmethod
    def _retry(retries=3, delay=1):
        def decorator(func):
            def wrapper(self, *args, **kwargs):
                current_retry = 0
                current_delay = delay
                while current_retry < retries:
                    try:
                        print(f"Function {func.__name__} succeeded on attempt {current_retry + 1}.")
                        return func(self, *args, **kwargs)
                    except Exception as e:
                        current_retry += 1
                        if current_retry >= retries:
                            raise e
                        print(f"Attempt {current_retry} failed: {e}. Retrying in {current_delay} seconds...")
                        time.sleep(current_delay)
                        current_delay *= 2
                return None
            return wrapper
        return decorator
    
    def __init__(self, retries=3, delay=1):
        self._lock = threading.RLock()
        self._connections = {}
        self.use_connection_pool = False
        # Apply retry logic to the methods.
        retry_decorator = self._retry(retries, delay)
        self.create_client = retry_decorator(self.create_client)
        self.create_ipc_client = retry_decorator(self.create_ipc_client)
        self.get = retry_decorator(self.get)
        self._new_connection = retry_decorator(self._new_connection)
        self.close = retry_decorator(self.close) 

    def create_client(self, addr: str, pooled: bool) -> Tuple[Optional[nodeapi_pb2_grpc.NodeAPIStub], Optional[Callable[[], None]], Optional[Exception]]:
        if pooled:
            conn = self.get(addr)
            if conn is None:
                return None, None, Exception("Failed to get connection")
            return nodeapi_pb2_grpc.NodeAPIStub(conn), None, None

        conn = self._new_connection(addr)
        if conn is None:
            return None, None, Exception("Failed to create connection")
        return nodeapi_pb2_grpc.NodeAPIStub(conn), (lambda: conn.close() if conn is not None else None), None

    def create_ipc_client(self, addr: str, pooled: bool) -> Tuple[Optional[ipcnodeapi_pb2_grpc.IPCNodeAPIStub], Optional[Callable[[], None]], Optional[Exception]]:
        if pooled:
            conn = self.get(addr)
            if conn is None:
                return None, None, Exception("Failed to get connection")
            return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), None, None

        conn = self._new_connection(addr)
        if conn is None:
            return None, None, Exception("Failed to create connection")
        return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), (lambda: conn.close() if conn is not None else None), None

    def get(self, addr: str) -> Optional[grpc.Channel]:
        """Retrieves an existing gRPC connection from the pool."""
        if not addr:
            return None
        # Use a lock to ensure thread-safe access to the connections dictionary
        with self._lock:
            return self._connections.get(addr)

    def _new_connection(self, addr: str) -> Optional[grpc.Channel]:
        """Creates a new gRPC connection to the specified address."""
        if not addr:
            return None
        try:
            conn = grpc.insecure_channel(addr)
            with self._lock:
                self._connections[addr] = conn
            return conn
        except Exception as e:
            return None

    def close(self) -> Optional[Exception]:
        """Closes all connections in the pool."""
        with self._lock:
            errors = []
            for addr, conn in self._connections.items():
                try:
                    conn.close()
                except Exception as e:
                    errors.append(f"Failed to close connection to {addr}: {e}")
            self._connections.clear()
            if errors:
                return Exception("Encountered errors while closing connections: " + ", ".join(errors))
            return None
