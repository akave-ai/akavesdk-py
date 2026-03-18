"""
Unit tests for gRPC base module (sdk/shared/grpc_base.py).

Issue: https://github.com/akave-ai/akavesdk-py/issues/104
"""

from unittest.mock import patch

import grpc
import pytest

from sdk.config import SDKError
from sdk.shared.grpc_base import GrpcClientBase


class _FakeGrpcError(grpc.RpcError):
    """Fake gRPC error that is a real exception (needed for `raise ... from error`)."""

    def __init__(self, status_code, details):
        self._code = status_code
        self._details = details
        super().__init__(details)

    def code(self):
        return self._code

    def details(self):
        return self._details


def _make_grpc_error(status_code, details="Error details"):
    return _FakeGrpcError(status_code, details)


class TestGrpcClientBaseInit:
    """Tests for GrpcClientBase.__init__()."""

    def test_default_timeout(self):
        client = GrpcClientBase()
        assert client.connection_timeout is None

    def test_with_timeout(self):
        client = GrpcClientBase(connection_timeout=30)
        assert client.connection_timeout == 30

    def test_zero_timeout(self):
        client = GrpcClientBase(connection_timeout=0)
        assert client.connection_timeout == 0


class TestHandleDeadlineExceeded:
    """Tests for DEADLINE_EXCEEDED status code handling."""

    @patch("sdk.shared.grpc_base.logging.warning")
    def test_logs_warning(self, mock_warning):
        client = GrpcClientBase(connection_timeout=5)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError):
            client._handle_grpc_error("ListFiles", error)

        mock_warning.assert_called_once()
        assert "ListFiles" in str(mock_warning.call_args)
        assert "timed out" in str(mock_warning.call_args)

    def test_includes_method_name(self):
        client = GrpcClientBase(connection_timeout=5)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError, match="CreateBucket"):
            client._handle_grpc_error("CreateBucket", error)

    def test_error_with_timeout_message(self):
        client = GrpcClientBase(connection_timeout=10)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED, "Timeout")

        with pytest.raises(SDKError, match="request timed out after 10s"):
            client._handle_grpc_error("CreateBucket", error)


class TestHandleGrpcErrorStatusCodes:
    """Tests for various gRPC status codes."""

    def setup_method(self):
        self.client = GrpcClientBase()

    def test_unavailable(self):
        error = _make_grpc_error(grpc.StatusCode.UNAVAILABLE, "Service unavailable")
        with pytest.raises(SDKError, match="UNAVAILABLE.*Service unavailable"):
            self.client._handle_grpc_error("Connect", error)

    def test_not_found(self):
        error = _make_grpc_error(grpc.StatusCode.NOT_FOUND, "Resource not found")
        with pytest.raises(SDKError, match="NOT_FOUND.*Resource not found"):
            self.client._handle_grpc_error("GetBucket", error)

    def test_permission_denied(self):
        error = _make_grpc_error(grpc.StatusCode.PERMISSION_DENIED, "Access denied")
        with pytest.raises(SDKError, match="PERMISSION_DENIED.*Access denied"):
            self.client._handle_grpc_error("DeleteFile", error)


class TestErrorDetails:
    """Tests for error details handling."""

    def setup_method(self):
        self.client = GrpcClientBase()

    def test_none_details_fallback(self):
        error = _make_grpc_error(grpc.StatusCode.UNKNOWN, None)
        with pytest.raises(SDKError, match="No details provided"):
            self.client._handle_grpc_error("Test", error)

    def test_details_included_in_message(self):
        error = _make_grpc_error(grpc.StatusCode.INTERNAL, "specific error info")
        with pytest.raises(SDKError, match="specific error info"):
            self.client._handle_grpc_error("Method", error)


class TestErrorChaining:
    """Tests for exception chaining."""

    def test_non_deadline_chains_original(self):
        client = GrpcClientBase()
        error = _make_grpc_error(grpc.StatusCode.UNAVAILABLE, "down")

        with pytest.raises(SDKError) as exc_info:
            client._handle_grpc_error("Test", error)

        assert exc_info.value.__cause__ is error
