import grpc
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, Callable, Any
from ..private.pb import nodeapi_pb2_grpc, ipcnodeapi_pb2_grpc

class ConnectionError(Exception):
    """Custom exception for connection-related errors"""
    pass

class ConnectionPool:
    def __init__(self, ssl_credentials: Optional[grpc.ChannelCredentials] = None, 
                 max_retries: int = 3,
                 max_concurrent: int = 100,
                 max_idle: int = 5,
                 max_age: int = 300):  
        self._lock = threading.RLock()
        self._connections = {}
        self._connection_stats = {}
        self._ssl_credentials = ssl_credentials
        self._max_retries = max_retries
        self._max_concurrent = max_concurrent
        self._max_idle = max_idle
        self._max_age = max_age
        self.use_connection_pool = True

    def _get_channel_options(self) -> list:
        """Returns gRPC channel options for better connection management"""
        return [
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),  
            ('grpc.max_send_message_length', 100 * 1024 * 1024),     
            ('grpc.keepalive_time_ms', 30000),                       
            ('grpc.keepalive_timeout_ms', 10000),                    
            ('grpc.keepalive_permit_without_calls', 1),              
            ('grpc.http2.max_pings_without_data', 0),               
            ('grpc.http2.min_time_between_pings_ms', 10000),         
        ]

    def create_client(self, addr: str, pooled: bool) -> Tuple[Any, Optional[Callable], Optional[Exception]]:
        """Creates a new client with proper error handling and connection management"""
        if pooled:
            return self._get_pooled_client(addr, nodeapi_pb2_grpc.NodeAPIStub)
        return self._create_new_client(addr, nodeapi_pb2_grpc.NodeAPIStub)

    def create_ipc_client(self, addr: str, pooled: bool) -> Tuple[Any, Optional[Callable], Optional[Exception]]:
        """Creates a new IPC client with proper error handling and connection management"""
        if pooled:
            return self._get_pooled_client(addr, ipcnodeapi_pb2_grpc.IPCNodeAPIStub)
        return self._create_new_client(addr, ipcnodeapi_pb2_grpc.IPCNodeAPIStub)

    def _get_pooled_client(self, addr: str, client_class) -> Tuple[Any, Optional[Callable], Optional[Exception]]:
        """Gets or creates a pooled client"""
        with self._lock:
            conn_info = self._connections.get(addr)
            if conn_info and self._is_connection_healthy(conn_info['channel']):
                conn_info['last_used'] = time.time()
                return client_class(conn_info['channel']), None, None
            
            # Remove unhealthy connection if it exists
            if addr in self._connections:
                self._remove_connection(addr)
            
            # Create new connection
            channel = self._create_secure_channel(addr)
            if not channel:
                return None, None, ConnectionError(f"Failed to create connection to {addr}")
            
            self._connections[addr] = {
                'channel': channel,
                'created_at': time.time(),
                'last_used': time.time()
            }
            return client_class(channel), None, None

    def _create_new_client(self, addr: str, client_class) -> Tuple[Any, Optional[Callable], Optional[Exception]]:
        """Creates a new non-pooled client"""
        channel = self._create_secure_channel(addr)
        if not channel:
            return None, None, ConnectionError(f"Failed to create connection to {addr}")
        return client_class(channel), lambda: channel.close(), None

    def _create_secure_channel(self, addr: str) -> Optional[grpc.Channel]:
        """Creates a secure gRPC channel with retry logic"""
        for attempt in range(self._max_retries):
            try:
                if self._ssl_credentials:
                    return grpc.secure_channel(addr, self._ssl_credentials, options=self._get_channel_options())
                else:
                    # Fallback to insecure channel with warning
                    import warnings
                    warnings.warn("Using insecure channel. This is not recommended for production use.", RuntimeWarning)
                    return grpc.insecure_channel(addr, options=self._get_channel_options())
            except Exception as e:
                if attempt == self._max_retries - 1:
                    return None
                time.sleep(min(1 * (attempt + 1), 5))  # Exponential backoff up to 5 seconds

    def _is_connection_healthy(self, channel: grpc.Channel) -> bool:
        """Checks if a connection is healthy"""
        try:
            state = channel.get_state(try_to_connect=False)
            return state not in [grpc.ChannelConnectivity.SHUTDOWN, 
                               grpc.ChannelConnectivity.TRANSIENT_FAILURE]
        except Exception:
            return False

    def _remove_connection(self, addr: str) -> None:
        """Safely removes a connection from the pool"""
        if addr in self._connections:
            try:
                self._connections[addr]['channel'].close()
            except Exception:
                pass
            del self._connections[addr]

    def _cleanup_old_connections(self) -> None:
        """Removes old and idle connections"""
        now = time.time()
        with self._lock:
            addrs_to_remove = []
            for addr, conn_info in self._connections.items():
                if (now - conn_info['created_at'] > self._max_age or 
                    now - conn_info['last_used'] > self._max_idle):
                    addrs_to_remove.append(addr)
            
            for addr in addrs_to_remove:
                self._remove_connection(addr)

    def close(self) -> Optional[Exception]:
        """Closes all connections in the pool"""
        with self._lock:
            errors = []
            for addr, conn_info in self._connections.items():
                try:
                    conn_info['channel'].close()
                except Exception as e:
                    errors.append(f"Failed to close connection to {addr}: {e}")
            self._connections.clear()
            if errors:
                return Exception("Encountered errors while closing connections: " + ", ".join(errors))
            return None

    def __del__(self):
        """Ensures connections are closed when the pool is destroyed"""
        self.close()
