import grpc
import threading
from ..private.pb import nodeapi_pb2_grpc, ipcnodeapi_pb2_grpc


class ConnectionPool:
    def __init__(self):
        self._lock = threading.RLock()
        self._connections = {}
        self.use_connection_pool = False

    def create_client(self, addr: str, pooled: bool):
        if pooled:
            conn = self.get(addr)
            if conn is None:
                return None, None, Exception("Failed to get connection")
            return nodeapi_pb2_grpc.NodeAPIStub(conn), None, None

        conn = self._new_connection(addr)
        if conn is None:
            return None, None, Exception("Failed to create connection")
        return nodeapi_pb2_grpc.NodeAPIStub(conn), lambda: conn.close(), None

    def create_ipc_client(self, addr: str, pooled: bool):
        if pooled:
            conn = self.get(addr)
            if conn is None:
                return None, None, Exception("Failed to get connection")
            return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), None, None

        conn = self._new_connection(addr)
        if conn is None:
            return None, None, Exception("Failed to create connection")
        return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), lambda: conn.close(), None

    def get(self, addr: str):
        with self._lock:
            return self._connections.get(addr)

    def _new_connection(self, addr: str):
        try:
            conn = grpc.insecure_channel(addr)
            with self._lock:
                self._connections[addr] = conn
            return conn
        except Exception as e:
            return None

    def close(self):
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
