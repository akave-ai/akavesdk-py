"""
Unit tests for gRPC base module (sdk/shared/grpc_base.py).

Covers: GrpcClientBase.__init__() with various timeout configurations,
_handle_grpc_error() with different gRPC status codes, error chaining,
logging behavior, and edge cases.

Issue: https://github.com/akave-ai/akavesdk-py/issues/104
"""

import logging
from unittest.mock import Mock, patch

import grpc
import pytest

from sdk.config import SDKError
from sdk.shared.grpc_base import GrpcClientBase


# ---------------------------------------------------------------------------
# Helper to create a mock gRPC error with .code() and .details()
# ---------------------------------------------------------------------------


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
    """Create a fake gRPC RpcError with code() and details() methods."""
    return _FakeGrpcError(status_code, details)


# ---------------------------------------------------------------------------
# GrpcClientBase.__init__()
# ---------------------------------------------------------------------------


class TestGrpcClientBaseInit:
    """Tests for GrpcClientBase initialization."""

    def test_default_timeout_is_none(self):
        client = GrpcClientBase()
        assert client.connection_timeout is None

    def test_with_timeout(self):
        client = GrpcClientBase(connection_timeout=30)
        assert client.connection_timeout == 30

    def test_zero_timeout(self):
        client = GrpcClientBase(connection_timeout=0)
        assert client.connection_timeout == 0

    def test_large_timeout(self):
        client = GrpcClientBase(connection_timeout=3600)
        assert client.connection_timeout == 3600

    def test_negative_timeout(self):
        client = GrpcClientBase(connection_timeout=-1)
        assert client.connection_timeout == -1

    def test_float_timeout(self):
        client = GrpcClientBase(connection_timeout=1.5)
        assert client.connection_timeout == 1.5


# ---------------------------------------------------------------------------
# _handle_grpc_error() — DEADLINE_EXCEEDED
# ---------------------------------------------------------------------------


class TestHandleDeadlineExceeded:
    """Tests for DEADLINE_EXCEEDED status code handling."""

    def test_raises_sdk_error_with_timeout_message(self):
        client = GrpcClientBase(connection_timeout=10)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED, "Timeout")

        with pytest.raises(SDKError, match="request timed out after 10s"):
            client._handle_grpc_error("CreateBucket", error)

    def test_includes_method_name(self):
        client = GrpcClientBase(connection_timeout=5)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError, match="CreateBucket"):
            client._handle_grpc_error("CreateBucket", error)

    def test_no_timeout_set_shows_none(self):
        client = GrpcClientBase()
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError, match="timed out after Nones"):
            client._handle_grpc_error("TestMethod", error)

    @patch("sdk.shared.grpc_base.logging.warning")
    def test_logs_warning(self, mock_warning):
        client = GrpcClientBase(connection_timeout=5)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError):
            client._handle_grpc_error("ListFiles", error)

        mock_warning.assert_called_once()
        assert "ListFiles" in str(mock_warning.call_args)
        assert "timed out" in str(mock_warning.call_args)

    def test_chains_original_error(self):
        client = GrpcClientBase(connection_timeout=10)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError) as exc_info:
            client._handle_grpc_error("TestMethod", error)

        assert exc_info.value.__cause__ is error


# ---------------------------------------------------------------------------
# _handle_grpc_error() — Other status codes
# ---------------------------------------------------------------------------


class TestHandleGrpcErrorStatusCodes:
    """Tests for various gRPC status codes (non-DEADLINE_EXCEEDED)."""

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

    def test_invalid_argument(self):
        error = _make_grpc_error(grpc.StatusCode.INVALID_ARGUMENT, "Bad input")
        with pytest.raises(SDKError, match="INVALID_ARGUMENT.*Bad input"):
            self.client._handle_grpc_error("Upload", error)

    def test_internal(self):
        error = _make_grpc_error(grpc.StatusCode.INTERNAL, "Internal server error")
        with pytest.raises(SDKError, match="INTERNAL.*Internal server error"):
            self.client._handle_grpc_error("Process", error)

    def test_cancelled(self):
        error = _make_grpc_error(grpc.StatusCode.CANCELLED, "Request cancelled")
        with pytest.raises(SDKError, match="CANCELLED.*Request cancelled"):
            self.client._handle_grpc_error("Stream", error)

    def test_resource_exhausted(self):
        error = _make_grpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED, "Too many requests")
        with pytest.raises(SDKError, match="RESOURCE_EXHAUSTED.*Too many requests"):
            self.client._handle_grpc_error("Batch", error)

    def test_already_exists(self):
        error = _make_grpc_error(grpc.StatusCode.ALREADY_EXISTS, "Bucket exists")
        with pytest.raises(SDKError, match="ALREADY_EXISTS.*Bucket exists"):
            self.client._handle_grpc_error("CreateBucket", error)

    def test_unauthenticated(self):
        error = _make_grpc_error(grpc.StatusCode.UNAUTHENTICATED, "Not authenticated")
        with pytest.raises(SDKError, match="UNAUTHENTICATED.*Not authenticated"):
            self.client._handle_grpc_error("Auth", error)

    def test_unimplemented(self):
        error = _make_grpc_error(grpc.StatusCode.UNIMPLEMENTED, "Not supported")
        with pytest.raises(SDKError, match="UNIMPLEMENTED.*Not supported"):
            self.client._handle_grpc_error("NewFeature", error)

    def test_aborted(self):
        error = _make_grpc_error(grpc.StatusCode.ABORTED, "Transaction aborted")
        with pytest.raises(SDKError, match="ABORTED.*Transaction aborted"):
            self.client._handle_grpc_error("Commit", error)

    def test_out_of_range(self):
        error = _make_grpc_error(grpc.StatusCode.OUT_OF_RANGE, "Offset too large")
        with pytest.raises(SDKError, match="OUT_OF_RANGE.*Offset too large"):
            self.client._handle_grpc_error("Seek", error)

    def test_data_loss(self):
        error = _make_grpc_error(grpc.StatusCode.DATA_LOSS, "Corrupted")
        with pytest.raises(SDKError, match="DATA_LOSS.*Corrupted"):
            self.client._handle_grpc_error("Download", error)

    def test_failed_precondition(self):
        error = _make_grpc_error(grpc.StatusCode.FAILED_PRECONDITION, "Not ready")
        with pytest.raises(SDKError, match="FAILED_PRECONDITION.*Not ready"):
            self.client._handle_grpc_error("Init", error)

    def test_unknown(self):
        error = _make_grpc_error(grpc.StatusCode.UNKNOWN, "Unknown error")
        with pytest.raises(SDKError, match="UNKNOWN.*Unknown error"):
            self.client._handle_grpc_error("Mystery", error)

    def test_ok_status_still_raises(self):
        """Even OK status raises since _handle_grpc_error is only called on errors."""
        error = _make_grpc_error(grpc.StatusCode.OK, "Weird OK error")
        with pytest.raises(SDKError):
            self.client._handle_grpc_error("Odd", error)


class TestAllStatusCodesCovered:
    """Verify all major gRPC status codes are handled without crashing."""

    def test_all_status_codes(self):
        client = GrpcClientBase()
        status_codes = [
            grpc.StatusCode.OK,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.UNKNOWN,
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.ALREADY_EXISTS,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.FAILED_PRECONDITION,
            grpc.StatusCode.ABORTED,
            grpc.StatusCode.OUT_OF_RANGE,
            grpc.StatusCode.UNIMPLEMENTED,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DATA_LOSS,
            grpc.StatusCode.UNAUTHENTICATED,
        ]
        for code in status_codes:
            error = _make_grpc_error(code, f"Error for {code.name}")
            with pytest.raises(SDKError):
                client._handle_grpc_error("TestMethod", error)


# ---------------------------------------------------------------------------
# Error details handling
# ---------------------------------------------------------------------------


class TestErrorDetails:
    """Tests for how error details are handled."""

    def setup_method(self):
        self.client = GrpcClientBase()

    def test_none_details_fallback(self):
        error = _make_grpc_error(grpc.StatusCode.UNKNOWN, None)
        with pytest.raises(SDKError, match="No details provided"):
            self.client._handle_grpc_error("Test", error)

    def test_empty_string_details_fallback(self):
        error = _make_grpc_error(grpc.StatusCode.UNKNOWN, "")
        with pytest.raises(SDKError, match="No details provided"):
            self.client._handle_grpc_error("Test", error)

    def test_details_included_in_message(self):
        error = _make_grpc_error(grpc.StatusCode.INTERNAL, "specific error info")
        with pytest.raises(SDKError, match="specific error info"):
            self.client._handle_grpc_error("Method", error)

    def test_status_code_value_in_message(self):
        """Error message should include the numeric status code value."""
        error = _make_grpc_error(grpc.StatusCode.INTERNAL, "err")
        with pytest.raises(SDKError) as exc_info:
            self.client._handle_grpc_error("M", error)
        msg = str(exc_info.value)
        # Should contain both name and value
        assert "INTERNAL" in msg


# ---------------------------------------------------------------------------
# Error chaining
# ---------------------------------------------------------------------------


class TestErrorChaining:
    """Tests for exception chaining (from error)."""

    def test_non_deadline_chains_original(self):
        client = GrpcClientBase()
        error = _make_grpc_error(grpc.StatusCode.UNAVAILABLE, "down")

        with pytest.raises(SDKError) as exc_info:
            client._handle_grpc_error("Test", error)

        assert exc_info.value.__cause__ is error

    def test_deadline_chains_original(self):
        client = GrpcClientBase(connection_timeout=10)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED, "timeout")

        with pytest.raises(SDKError) as exc_info:
            client._handle_grpc_error("Test", error)

        assert exc_info.value.__cause__ is error


# ---------------------------------------------------------------------------
# Logging behavior
# ---------------------------------------------------------------------------


class TestLogging:
    """Tests for logging behavior in error handling."""

    @patch("sdk.shared.grpc_base.logging.error")
    def test_non_deadline_logs_error(self, mock_error_log):
        client = GrpcClientBase()
        error = _make_grpc_error(grpc.StatusCode.INTERNAL, "Server error")

        with pytest.raises(SDKError):
            client._handle_grpc_error("Upload", error)

        mock_error_log.assert_called_once()
        log_msg = str(mock_error_log.call_args)
        assert "Upload" in log_msg
        assert "INTERNAL" in log_msg

    @patch("sdk.shared.grpc_base.logging.warning")
    def test_deadline_does_not_log_error(self, mock_warning):
        client = GrpcClientBase(connection_timeout=5)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError):
            client._handle_grpc_error("Slow", error)

        mock_warning.assert_called_once()

    @patch("sdk.shared.grpc_base.logging.error")
    @patch("sdk.shared.grpc_base.logging.warning")
    def test_deadline_uses_warning_not_error(self, mock_warning, mock_error_log):
        client = GrpcClientBase(connection_timeout=5)
        error = _make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

        with pytest.raises(SDKError):
            client._handle_grpc_error("Slow", error)

        mock_warning.assert_called_once()
        mock_error_log.assert_not_called()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge case inputs."""

    def setup_method(self):
        self.client = GrpcClientBase()

    def test_special_characters_in_details(self):
        error = _make_grpc_error(grpc.StatusCode.INVALID_ARGUMENT, "Error: <tag> & 'quotes' \"double\"")
        with pytest.raises(SDKError):
            self.client._handle_grpc_error("Parse", error)

    def test_unicode_in_details(self):
        error = _make_grpc_error(grpc.StatusCode.INVALID_ARGUMENT, "错误信息 🔥")
        with pytest.raises(SDKError):
            self.client._handle_grpc_error("Intl", error)

    def test_very_long_method_name(self):
        error = _make_grpc_error(grpc.StatusCode.UNKNOWN, "err")
        long_name = "A" * 500
        with pytest.raises(SDKError):
            self.client._handle_grpc_error(long_name, error)

    def test_very_long_details(self):
        error = _make_grpc_error(grpc.StatusCode.UNKNOWN, "x" * 10000)
        with pytest.raises(SDKError):
            self.client._handle_grpc_error("Test", error)

    def test_empty_method_name(self):
        error = _make_grpc_error(grpc.StatusCode.INTERNAL, "err")
        with pytest.raises(SDKError):
            self.client._handle_grpc_error("", error)


# ---------------------------------------------------------------------------
# Subclassing
# ---------------------------------------------------------------------------


class TestSubclassing:
    """Tests for subclassing GrpcClientBase."""

    def test_subclass_inherits_timeout(self):
        class MyClient(GrpcClientBase):
            def __init__(self):
                super().__init__(connection_timeout=15)

        client = MyClient()
        assert client.connection_timeout == 15

    def test_subclass_can_use_error_handler(self):
        class MyClient(GrpcClientBase):
            def __init__(self):
                super().__init__(connection_timeout=10)

            def do_rpc(self):
                error = _make_grpc_error(grpc.StatusCode.UNAVAILABLE, "down")
                self._handle_grpc_error("do_rpc", error)

        client = MyClient()
        with pytest.raises(SDKError, match="UNAVAILABLE.*down"):
            client.do_rpc()

    def test_multiple_errors_handled_independently(self):
        client = GrpcClientBase(connection_timeout=30)
        codes = [
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.INTERNAL,
        ]
        for code in codes:
            error = _make_grpc_error(code, f"{code.name} details")
            with pytest.raises(SDKError):
                client._handle_grpc_error("Multi", error)
