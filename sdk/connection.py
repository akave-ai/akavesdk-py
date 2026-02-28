import grpc
import time
import threading
import logging
from typing import Dict, Optional, Tuple, Callable
from private.pb import ipcnodeapi_pb2_grpc
from .config import SDKError


class ConnectionPool:
    
    def __init__(self):
        self._lock = threading.RLock()
        self._connections: Dict[str, grpc.Channel] = {}

    def create_ipc_client(self, addr: str, pooled: bool) -> Tuple[ipcnodeapi_pb2_grpc.IPCNodeAPIStub, Optional[Callable[[], None]], Optional[Exception]]:
        try:
            if pooled:
                conn, err = self._get(addr)
                if err:
                    return None, None, err
                return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), None, None

            options = [
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),  
                ('grpc.max_send_message_length', 100 * 1024 * 1024),
                ('grpc.keepalive_time_ms', 30000),  
                ('grpc.keepalive_timeout_ms', 10000),  
                ('grpc.keepalive_permit_without_calls', 1), 
                ('grpc.http2.max_pings_without_data', 0),  
                ('grpc.http2.min_time_between_pings_ms', 10000), 
            ]
            conn = grpc.insecure_channel(addr, options=options)
            return ipcnodeapi_pb2_grpc.IPCNodeAPIStub(conn), conn.close, None
            
        except Exception as e:
            return None, None, SDKError(f"Failed to create IPC client: {str(e)}")

    def create_archival_client(self, addr: str, pooled: bool) -> Tuple[ipcnodeapi_pb2_grpc.IPCArchivalAPIStub, Optional[Callable[[], None]], Optional[Exception]]:
        try:
            if pooled:
                conn, err = self._get(addr)
                if err:
                    return None, None, err
                return ipcnodeapi_pb2_grpc.IPCArchivalAPIStub(conn), None, None

            options = [
                ('grpc.max_receive_message_length', 100 * 1024 * 1024), 
                ('grpc.max_send_message_length', 100 * 1024 * 1024),
                ('grpc.keepalive_time_ms', 30000), 
                ('grpc.keepalive_timeout_ms', 10000), 
                ('grpc.keepalive_permit_without_calls', 1),  
                ('grpc.http2.max_pings_without_data', 0), 
                ('grpc.http2.min_time_between_pings_ms', 10000),  
            ]
            conn = grpc.insecure_channel(addr, options=options)
            return ipcnodeapi_pb2_grpc.IPCArchivalAPIStub(conn), conn.close, None
            
        except Exception as e:
            return None, None, SDKError(f"Failed to create archival client: {str(e)}")

    def _get(self, addr: str) -> Tuple[Optional[grpc.Channel], Optional[Exception]]:
        with self._lock:
            if addr in self._connections:
                return self._connections[addr], None

        with self._lock:
            if addr in self._connections:
                return self._connections[addr], None

            try:
                options = [
                    ('grpc.max_receive_message_length', 100 * 1024 * 1024),  
                    ('grpc.max_send_message_length', 100 * 1024 * 1024),
                    ('grpc.keepalive_time_ms', 30000),  
                    ('grpc.keepalive_timeout_ms', 10000),  
                    ('grpc.keepalive_permit_without_calls', 1),  
                    ('grpc.http2.max_pings_without_data', 0), 
                    ('grpc.http2.min_time_between_pings_ms', 10000),  
                ]
                conn = grpc.insecure_channel(addr, options=options)
                
                try:
                    grpc.channel_ready_future(conn).result(timeout=5)
                except grpc.FutureTimeoutError:
                    logging.warning(f"Connection to {addr} not ready within timeout, proceeding anyway")
                
                self._connections[addr] = conn
                return conn, None
                
            except Exception as e:
                return None, SDKError(f"Failed to connect to {addr}: {str(e)}")

    def close(self) -> Optional[Exception]:
        with self._lock:
            errors = []
            
            for addr, conn in self._connections.items():
                try:
                    conn.close()
                except Exception as e:
                    errors.append(f"failed to close connection to {addr}: {str(e)}")
            
            self._connections.clear()
            
            if errors:
                return SDKError(f"encountered errors while closing connections: {errors}")
            
            return None


def new_connection_pool() -> ConnectionPool:
    return ConnectionPool()