import pytest
from unittest.mock import Mock, patch, MagicMock

from sdk.connection import ConnectionPool, new_connection_pool
from sdk.config import SDKError


class TestConnectionPool:

    def test_init(self):
        pool = ConnectionPool()
        assert pool._connections == {}
        assert pool._lock is not None

    def test_new_connection_pool(self):
        pool = new_connection_pool()
        assert isinstance(pool, ConnectionPool)
        assert pool._connections == {}

    @patch("sdk.connection.grpc.insecure_channel")
    @patch("sdk.connection.grpc.channel_ready_future")
    def test_create_ipc_client_non_pooled(self, mock_ready_future, mock_channel):
        mock_conn = Mock()
        mock_channel.return_value = mock_conn
        mock_future = Mock()
        mock_ready_future.return_value = mock_future

        pool = ConnectionPool()
        stub, close_func, err = pool.create_ipc_client("localhost:5500", pooled=False)

        assert stub is not None
        assert close_func is not None
        assert err is None
        mock_channel.assert_called_once()

    @patch("sdk.connection.grpc.insecure_channel")
    @patch("sdk.connection.grpc.channel_ready_future")
    def test_create_ipc_client_pooled_reuse(self, mock_ready_future, mock_channel):
        mock_conn = Mock()
        mock_channel.return_value = mock_conn
        mock_future = Mock()
        mock_ready_future.return_value = mock_future

        pool = ConnectionPool()
        
        stub1, close_func1, err1 = pool.create_ipc_client("localhost:5500", pooled=True)
        assert err1 is None
        assert stub1 is not None

        stub2, close_func2, err2 = pool.create_ipc_client("localhost:5500", pooled=True)
        assert err2 is None
        assert stub2 is not None
        
        assert mock_channel.call_count == 1

    @patch("sdk.connection.grpc.insecure_channel")
    def test_close_connections(self, mock_channel):
        mock_conn = Mock()
        mock_channel.return_value = mock_conn
        
        pool = ConnectionPool()
        pool._connections["addr1"] = mock_conn
        pool._connections["addr2"] = mock_conn
        
        err = pool.close()
        
        assert err is None
        assert len(pool._connections) == 0
        assert mock_conn.close.call_count == 2

    def test_close_with_errors(self):
        pool = ConnectionPool()
        
        mock_conn_bad = Mock()
        mock_conn_bad.close.side_effect = Exception("Connection error")
        
        pool._connections["addr1"] = mock_conn_bad
        
        err = pool.close()
        
        assert isinstance(err, SDKError)
        assert "encountered errors" in str(err)
        assert len(pool._connections) == 0
