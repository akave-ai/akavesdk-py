import threading
from unittest.mock import ANY, Mock, patch

import grpc
import pytest

from sdk.config import SDKError
from sdk.connection import ConnectionPool, new_connection_pool


class TestConnectionPoolInit:

    def test_init(self):
        pool = ConnectionPool()
        assert pool._connections == {}
        assert isinstance(pool._lock, type(threading.RLock()))

    def test_new_connection_pool(self):
        pool = new_connection_pool()
        assert isinstance(pool, ConnectionPool)
        assert pool._connections == {}


class TestCreateIPCClient:

    def test_create_ipc_client_pooled(self):
        pool = ConnectionPool()
        mock_conn = Mock()

        with patch.object(pool, "_get", return_value=(mock_conn, None)):
            stub, closer, err = pool.create_ipc_client("test:5500", pooled=True)

            assert stub is not None
            assert closer is None
            assert err is None

    def test_create_ipc_client_pooled_with_error(self):
        pool = ConnectionPool()
        error = SDKError("Connection failed")

        with patch.object(pool, "_get", return_value=(None, error)):
            stub, closer, err = pool.create_ipc_client("test:5500", pooled=True)

            assert stub is None
            assert closer is None
            assert err == error

    @patch("grpc.insecure_channel")
    def test_create_ipc_client_not_pooled(self, mock_channel):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_conn.close = Mock()
        mock_channel.return_value = mock_conn

        stub, closer, err = pool.create_ipc_client("test:5500", pooled=False)

        assert stub is not None
        assert closer is not None
        assert err is None
        mock_channel.assert_called_once_with("test:5500", options=ANY)

    @patch("grpc.insecure_channel")
    def test_create_ipc_client_exception(self, mock_channel):
        pool = ConnectionPool()
        mock_channel.side_effect = Exception("Connection error")

        stub, closer, err = pool.create_ipc_client("test:5500", pooled=False)

        assert stub is None
        assert closer is None
        assert isinstance(err, SDKError)
        assert "Failed to create IPC client" in str(err)


class TestCreateStreamingClient:

    def test_create_streaming_client_pooled(self):
        pool = ConnectionPool()
        mock_conn = Mock()

        with patch.object(pool, "_get", return_value=(mock_conn, None)):
            stub, closer, err = pool.create_streaming_client("test:5500", pooled=True)

            assert stub is not None
            assert closer is None
            assert err is None

    def test_create_streaming_client_pooled_with_error(self):
        pool = ConnectionPool()
        error = SDKError("Get connection failed")

        with patch.object(pool, "_get", return_value=(None, error)):
            stub, closer, err = pool.create_streaming_client("test:5500", pooled=True)

            assert stub is None
            assert closer is None
            assert err == error

    @patch("grpc.insecure_channel")
    def test_create_streaming_client_not_pooled(self, mock_channel):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_conn.close = Mock()
        mock_channel.return_value = mock_conn

        stub, closer, err = pool.create_streaming_client("test:5500", pooled=False)

        assert stub is not None
        assert closer is not None
        assert err is None
        mock_channel.assert_called_once_with("test:5500", options=ANY)

    @patch("grpc.insecure_channel")
    def test_create_streaming_client_exception(self, mock_channel):
        pool = ConnectionPool()
        mock_channel.side_effect = Exception("Streaming connection error")

        stub, closer, err = pool.create_streaming_client("test:5500", pooled=False)

        assert stub is None
        assert closer is None
        assert isinstance(err, SDKError)
        assert "Failed to create streaming client" in str(err)


class TestGet:

    @patch("grpc.insecure_channel")
    @patch("grpc.channel_ready_future")
    def test_get_new_connection(self, mock_ready_future, mock_channel):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_channel.return_value = mock_conn

        mock_future = Mock()
        mock_future.result = Mock()
        mock_ready_future.return_value = mock_future

        conn, err = pool._get("test:5500")

        assert conn == mock_conn
        assert err is None
        assert "test:5500" in pool._connections
        mock_channel.assert_called_once_with("test:5500", options=ANY)

    def test_get_existing_connection(self):
        pool = ConnectionPool()
        mock_conn = Mock()
        pool._connections["test:5500"] = mock_conn

        conn, err = pool._get("test:5500")

        assert conn == mock_conn
        assert err is None

    @patch("grpc.insecure_channel")
    @patch("grpc.channel_ready_future")
    def test_get_connection_timeout(self, mock_ready_future, mock_channel):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_channel.return_value = mock_conn

        mock_future = Mock()
        mock_future.result = Mock(side_effect=grpc.FutureTimeoutError())
        mock_ready_future.return_value = mock_future

        conn, err = pool._get("test:5500")

        assert conn == mock_conn
        assert err is None
        assert "test:5500" in pool._connections

    @patch("grpc.insecure_channel")
    def test_get_connection_error(self, mock_channel):
        pool = ConnectionPool()
        mock_channel.side_effect = Exception("Connection failed")

        conn, err = pool._get("test:5500")

        assert conn is None
        assert isinstance(err, SDKError)
        assert "Failed to connect" in str(err)

    @patch("grpc.insecure_channel")
    @patch("grpc.channel_ready_future")
    def test_get_concurrent_access(self, mock_ready_future, mock_channel):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_channel.return_value = mock_conn

        mock_future = Mock()
        mock_future.result = Mock()
        mock_ready_future.return_value = mock_future

        results = []

        def get_connection():
            conn, err = pool._get("test:5500")
            results.append((conn, err))

        threads = [threading.Thread(target=get_connection) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(pool._connections) == 1
        assert all(conn == mock_conn for conn, _ in results)


class TestClose:

    def test_close_empty_pool(self):
        pool = ConnectionPool()

        err = pool.close()

        assert err is None
        assert len(pool._connections) == 0

    def test_close_single_connection(self):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_conn.close = Mock()
        pool._connections["addr1"] = mock_conn

        err = pool.close()

        assert err is None
        assert len(pool._connections) == 0
        mock_conn.close.assert_called_once()

    def test_close_multiple_connections(self):
        pool = ConnectionPool()
        mock_conn1 = Mock()
        mock_conn2 = Mock()
        mock_conn3 = Mock()

        pool._connections["addr1"] = mock_conn1
        pool._connections["addr2"] = mock_conn2
        pool._connections["addr3"] = mock_conn3

        err = pool.close()

        assert err is None
        assert len(pool._connections) == 0
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        mock_conn3.close.assert_called_once()

    def test_close_with_error(self):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_conn.close = Mock(side_effect=Exception("Close failed"))
        pool._connections["addr1"] = mock_conn

        err = pool.close()

        assert err is not None
        assert isinstance(err, SDKError)
        assert "encountered errors" in str(err)
        assert len(pool._connections) == 0

    def test_close_with_partial_errors(self):
        pool = ConnectionPool()

        mock_conn1 = Mock()
        mock_conn1.close = Mock()

        mock_conn2 = Mock()
        mock_conn2.close = Mock(side_effect=Exception("Close failed"))

        mock_conn3 = Mock()
        mock_conn3.close = Mock()

        pool._connections["addr1"] = mock_conn1
        pool._connections["addr2"] = mock_conn2
        pool._connections["addr3"] = mock_conn3

        err = pool.close()

        assert err is not None
        assert isinstance(err, SDKError)
        assert len(pool._connections) == 0
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        mock_conn3.close.assert_called_once()


class TestConnectionPoolThreadSafety:

    @patch("grpc.insecure_channel")
    @patch("grpc.channel_ready_future")
    def test_concurrent_get_and_close(self, mock_ready_future, mock_channel):
        pool = ConnectionPool()
        mock_conn = Mock()
        mock_conn.close = Mock()
        mock_channel.return_value = mock_conn

        mock_future = Mock()
        mock_future.result = Mock()
        mock_ready_future.return_value = mock_future

        def get_connections():
            for i in range(10):
                pool._get(f"addr{i % 3}")

        def close_pool():
            import time

            time.sleep(0.001)
            pool.close()

        get_thread = threading.Thread(target=get_connections)
        close_thread = threading.Thread(target=close_pool)

        get_thread.start()
        close_thread.start()

        get_thread.join()
        close_thread.join()

        pool.close()

        assert len(pool._connections) == 0


@pytest.mark.integration
class TestConnectionPoolIntegration:

    @patch("grpc.insecure_channel")
    def test_full_lifecycle(self, mock_channel):
        mock_conn = Mock()
        mock_conn.close = Mock()
        mock_channel.return_value = mock_conn

        pool = new_connection_pool()

        stub1, closer1, err1 = pool.create_ipc_client("addr1:5500", pooled=False)
        assert err1 is None
        assert stub1 is not None
        assert closer1 is not None

        stub2, closer2, err2 = pool.create_streaming_client("addr2:5500", pooled=False)
        assert err2 is None
        assert stub2 is not None
        assert closer2 is not None

        if closer1:
            closer1()
        if closer2:
            closer2()

        err = pool.close()
        assert err is None

    @patch("grpc.insecure_channel")
    @patch("grpc.channel_ready_future")
    def test_pooled_connections_reuse(self, mock_ready_future, mock_channel):
        mock_conn = Mock()
        mock_channel.return_value = mock_conn

        mock_future = Mock()
        mock_future.result = Mock()
        mock_ready_future.return_value = mock_future

        pool = new_connection_pool()

        stub1, closer1, err1 = pool.create_ipc_client("addr:5500", pooled=True)
        stub2, closer2, err2 = pool.create_ipc_client("addr:5500", pooled=True)

        assert err1 is None
        assert err2 is None
        assert closer1 is None
        assert closer2 is None

        assert mock_channel.call_count == 1

        pool.close()
