"""
Unit tests for the HTTP Extension module (private/httpext/httpext.py).
Tests cover:
- range_download() with various byte ranges
- HTTP headers handling
- Response handling (200, 206, 416)
- Network errors
- Edge cases
"""
import io
import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from private.httpext import (
    HTTPExtClient,
    RangeDownloadResult,
    HTTPExtError,
    RangeNotSatisfiableError,
)
from private.httpext.httpext import NetworkError, InvalidRangeError


class TestHTTPExtClientInit:
    """Tests for HTTPExtClient initialization."""

    def test_init_default_values(self):
        """Test client initialization with default values."""
        client = HTTPExtClient()
        assert client.timeout == HTTPExtClient.DEFAULT_TIMEOUT
        assert client.session is not None
        client.close()

    def test_init_custom_values(self):
        """Test client initialization with custom values."""
        client = HTTPExtClient(timeout=60, retries=5, backoff_factor=0.5)
        assert client.timeout == 60
        client.close()

    def test_context_manager(self):
        """Test client as context manager."""
        with HTTPExtClient() as client:
            assert client.session is not None
        # Session should be closed after context exits

    def test_close(self):
        """Test client close method."""
        client = HTTPExtClient()
        client.close()
        # Should not raise even if called multiple times
        client.close()


class TestRangeValidation:
    """Tests for range parameter validation."""

    def test_validate_range_valid(self):
        """Test validation passes for valid ranges."""
        client = HTTPExtClient()
        # Should not raise
        client._validate_range(0, 100)
        client._validate_range(0, None)
        client._validate_range(100, 200)
        client._validate_range(0, 0)  # Single byte
        client.close()

    def test_validate_range_negative_start(self):
        """Test validation fails for negative start."""
        client = HTTPExtClient()
        with pytest.raises(InvalidRangeError, match="Start position cannot be negative"):
            client._validate_range(-1, 100)
        client.close()

    def test_validate_range_negative_end(self):
        """Test validation fails for negative end."""
        client = HTTPExtClient()
        with pytest.raises(InvalidRangeError, match="End position cannot be negative"):
            client._validate_range(0, -1)
        client.close()

    def test_validate_range_end_less_than_start(self):
        """Test validation fails when end < start."""
        client = HTTPExtClient()
        with pytest.raises(InvalidRangeError, match="End position cannot be less than start"):
            client._validate_range(100, 50)
        client.close()


class TestBuildRangeHeader:
    """Tests for Range header construction."""

    def test_build_range_header_with_end(self):
        """Test Range header with both start and end."""
        client = HTTPExtClient()
        header = client._build_range_header(0, 499)
        assert header == "bytes=0-499"
        client.close()

    def test_build_range_header_without_end(self):
        """Test Range header with only start (suffix range)."""
        client = HTTPExtClient()
        header = client._build_range_header(500, None)
        assert header == "bytes=500-"
        client.close()

    def test_build_range_header_single_byte(self):
        """Test Range header for single byte."""
        client = HTTPExtClient()
        header = client._build_range_header(100, 100)
        assert header == "bytes=100-100"
        client.close()

    def test_build_range_header_large_range(self):
        """Test Range header for large range."""
        client = HTTPExtClient()
        header = client._build_range_header(0, 1073741823)  # ~1GB
        assert header == "bytes=0-1073741823"
        client.close()


class TestParseContentRange:
    """Tests for Content-Range header parsing."""

    def test_parse_content_range_valid(self):
        """Test parsing valid Content-Range header."""
        client = HTTPExtClient()
        start, end, total = client._parse_content_range("bytes 0-499/1000")
        assert start == 0
        assert end == 499
        assert total == 1000
        client.close()

    def test_parse_content_range_unknown_total(self):
        """Test parsing Content-Range with unknown total."""
        client = HTTPExtClient()
        start, end, total = client._parse_content_range("bytes 0-499/*")
        assert start == 0
        assert end == 499
        assert total is None
        client.close()

    def test_parse_content_range_none(self):
        """Test parsing None Content-Range."""
        client = HTTPExtClient()
        start, end, total = client._parse_content_range(None)
        assert start is None
        assert end is None
        assert total is None
        client.close()

    def test_parse_content_range_invalid_format(self):
        """Test parsing invalid Content-Range format."""
        client = HTTPExtClient()
        # Missing "bytes " prefix
        start, end, total = client._parse_content_range("0-499/1000")
        assert start is None
        # Invalid range format
        start, end, total = client._parse_content_range("bytes invalid")
        assert start is None
        client.close()


class TestRangeDownload:
    """Tests for range_download() method."""

    @patch.object(requests.Session, 'get')
    def test_range_download_206_partial_content(self, mock_get):
        """Test successful range download with 206 Partial Content."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"Hello"
        mock_response.headers = {"Content-Range": "bytes 0-4/1000"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 0, 4)
        assert result.data == b"Hello"
        assert result.start == 0
        assert result.end == 4
        assert result.total_size == 1000
        assert result.content_length == 5
        assert result.is_partial is True

        # Verify Range header was sent
        mock_get.assert_called_once()
        call_headers = mock_get.call_args[1]["headers"]
        assert call_headers["Range"] == "bytes=0-4"
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_200_full_content(self, mock_get):
        """Test range download when server returns full content (200)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"Full file content"
        mock_response.headers = {}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 0, 4)
        assert result.data == b"Full file content"
        assert result.start == 0
        assert result.end == len(b"Full file content") - 1
        assert result.total_size == len(b"Full file content")
        assert result.is_partial is False
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_416_range_not_satisfiable(self, mock_get):
        """Test range download with 416 Range Not Satisfiable."""
        mock_response = Mock()
        mock_response.status_code = 416
        mock_response.headers = {"Content-Range": "bytes */1000"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        with pytest.raises(RangeNotSatisfiableError) as exc_info:
            client.range_download("http://example.com/file", 2000, 3000)
        assert exc_info.value.content_length == 1000
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_416_without_content_length(self, mock_get):
        """Test 416 response without content length info."""
        mock_response = Mock()
        mock_response.status_code = 416
        mock_response.headers = {}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        with pytest.raises(RangeNotSatisfiableError) as exc_info:
            client.range_download("http://example.com/file", 2000, 3000)
        assert exc_info.value.content_length is None
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_unexpected_status(self, mock_get):
        """Test range download with unexpected status code."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        with pytest.raises(HTTPExtError, match="Unexpected status code: 403"):
            client.range_download("http://example.com/file", 0, 100)
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_with_custom_headers(self, mock_get):
        """Test range download with additional custom headers."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"data"
        mock_response.headers = {"Content-Range": "bytes 0-3/100"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download(
            "http://example.com/file",
            0, 3,
            headers={"Authorization": "Bearer token123"}
        )
        call_headers = mock_get.call_args[1]["headers"]
        assert call_headers["Authorization"] == "Bearer token123"
        assert call_headers["Range"] == "bytes=0-3"
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_suffix_range(self, mock_get):
        """Test range download with suffix range (no end specified)."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"end of file"
        mock_response.headers = {"Content-Range": "bytes 900-999/1000"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 900, None)
        call_headers = mock_get.call_args[1]["headers"]
        assert call_headers["Range"] == "bytes=900-"
        assert result.start == 900
        assert result.end == 999
        client.close()


class TestRangeDownloadNetworkErrors:
    """Tests for network error handling in range_download()."""

    @patch.object(requests.Session, 'get')
    def test_range_download_timeout(self, mock_get):
        """Test range download timeout handling."""
        mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")
        client = HTTPExtClient()
        with pytest.raises(NetworkError, match="Request timed out"):
            client.range_download("http://example.com/file", 0, 100)
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_connection_error(self, mock_get):
        """Test range download connection error handling."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")
        client = HTTPExtClient()
        with pytest.raises(NetworkError, match="Connection error"):
            client.range_download("http://example.com/file", 0, 100)
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_request_exception(self, mock_get):
        """Test range download generic request exception."""
        mock_get.side_effect = requests.exceptions.RequestException("Unknown error")
        client = HTTPExtClient()
        with pytest.raises(HTTPExtError, match="Request failed"):
            client.range_download("http://example.com/file", 0, 100)
        client.close()


class TestRangeDownloadEdgeCases:
    """Tests for edge cases in range_download()."""

    @patch.object(requests.Session, 'get')
    def test_range_download_single_byte(self, mock_get):
        """Test downloading a single byte."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"X"
        mock_response.headers = {"Content-Range": "bytes 50-50/1000"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 50, 50)
        assert result.data == b"X"
        assert result.start == 50
        assert result.end == 50
        assert result.content_length == 1
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_first_byte(self, mock_get):
        """Test downloading the first byte only."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"F"
        mock_response.headers = {"Content-Range": "bytes 0-0/1000"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 0, 0)
        assert result.data == b"F"
        assert result.start == 0
        assert result.end == 0
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_last_byte(self, mock_get):
        """Test downloading the last byte only."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"L"
        mock_response.headers = {"Content-Range": "bytes 999-999/1000"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 999, 999)
        assert result.data == b"L"
        assert result.end == 999
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_empty_response(self, mock_get):
        """Test handling empty response content."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b""
        mock_response.headers = {"Content-Range": "bytes 0-0/0"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 0, 0)
        assert result.data == b""
        assert result.content_length == 0
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_large_range(self, mock_get):
        """Test downloading a large range."""
        large_data = b"X" * 1024 * 1024  # 1MB
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = large_data
        mock_response.headers = {"Content-Range": f"bytes 0-{len(large_data)-1}/{len(large_data)}"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 0, len(large_data) - 1)
        assert len(result.data) == len(large_data)
        assert result.content_length == len(large_data)
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_missing_content_range_header(self, mock_get):
        """Test 206 response without Content-Range header."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"data"
        mock_response.headers = {}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        result = client.range_download("http://example.com/file", 0, 3)
        # Should use fallback values
        assert result.data == b"data"
        assert result.start == 0
        assert result.end == 3  # start + len(content) - 1
        assert result.total_size is None
        client.close()

    def test_range_download_invalid_start(self):
        """Test range download with invalid start position."""
        client = HTTPExtClient()
        with pytest.raises(InvalidRangeError):
            client.range_download("http://example.com/file", -1, 100)
        client.close()

    def test_range_download_invalid_end(self):
        """Test range download with end < start."""
        client = HTTPExtClient()
        with pytest.raises(InvalidRangeError):
            client.range_download("http://example.com/file", 100, 50)
        client.close()


class TestRangeDownloadToFile:
    """Tests for range_download_to_file() method."""

    @patch.object(requests.Session, 'get')
    def test_range_download_to_file_success(self, mock_get):
        """Test successful streaming download to file."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.headers = {"Content-Range": "bytes 0-99/1000"}
        mock_response.iter_content = Mock(return_value=[b"chunk1", b"chunk2", b"chunk3"])
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        buffer = io.BytesIO()
        result = client.range_download_to_file(
            "http://example.com/file", 0, 99, buffer
        )
        assert buffer.getvalue() == b"chunk1chunk2chunk3"
        assert result.start == 0
        assert result.end == 99
        assert result.total_size == 1000
        assert result.data == b""  # Data written to file, not returned
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_to_file_416(self, mock_get):
        """Test streaming download with 416 response."""
        mock_response = Mock()
        mock_response.status_code = 416
        mock_response.headers = {"Content-Range": "bytes */500"}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        buffer = io.BytesIO()
        with pytest.raises(RangeNotSatisfiableError) as exc_info:
            client.range_download_to_file(
                "http://example.com/file", 1000, 2000, buffer
            )
        assert exc_info.value.content_length == 500
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_to_file_timeout(self, mock_get):
        """Test streaming download timeout."""
        mock_get.side_effect = requests.exceptions.Timeout()
        client = HTTPExtClient()
        buffer = io.BytesIO()
        with pytest.raises(NetworkError, match="Request timed out"):
            client.range_download_to_file(
                "http://example.com/file", 0, 100, buffer
            )
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_to_file_unexpected_status(self, mock_get):
        """Test streaming download with unexpected status code."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.headers = {}
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        buffer = io.BytesIO()
        with pytest.raises(HTTPExtError, match="Unexpected status code: 403"):
            client.range_download_to_file(
                "http://example.com/file", 0, 100, buffer
            )
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_to_file_connection_error(self, mock_get):
        """Test streaming download connection error."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")
        client = HTTPExtClient()
        buffer = io.BytesIO()
        with pytest.raises(NetworkError, match="Connection error"):
            client.range_download_to_file(
                "http://example.com/file", 0, 100, buffer
            )
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_to_file_request_exception(self, mock_get):
        """Test streaming download generic request exception."""
        mock_get.side_effect = requests.exceptions.RequestException("Unknown error")
        client = HTTPExtClient()
        buffer = io.BytesIO()
        with pytest.raises(HTTPExtError, match="Request failed"):
            client.range_download_to_file(
                "http://example.com/file", 0, 100, buffer
            )
        client.close()

    @patch.object(requests.Session, 'get')
    def test_range_download_to_file_with_custom_headers(self, mock_get):
        """Test streaming download with custom headers."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.headers = {"Content-Range": "bytes 0-99/1000"}
        mock_response.iter_content = Mock(return_value=[b"data"])
        mock_get.return_value = mock_response

        client = HTTPExtClient()
        buffer = io.BytesIO()
        client.range_download_to_file(
            "http://example.com/file", 0, 99, buffer,
            headers={"Authorization": "Bearer token"}
        )
        call_headers = mock_get.call_args[1]["headers"]
        assert call_headers["Authorization"] == "Bearer token"
        assert call_headers["Range"] == "bytes=0-99"
        client.close()


class TestGetContentLength:
    """Tests for get_content_length() method."""

    @patch.object(requests.Session, 'head')
    def test_get_content_length_success(self, mock_head):
        """Test successful content length retrieval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "12345"}
        mock_response.raise_for_status = Mock()
        mock_head.return_value = mock_response

        client = HTTPExtClient()
        length = client.get_content_length("http://example.com/file")
        assert length == 12345
        client.close()

    @patch.object(requests.Session, 'head')
    def test_get_content_length_not_available(self, mock_head):
        """Test when Content-Length header is not available."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.raise_for_status = Mock()
        mock_head.return_value = mock_response

        client = HTTPExtClient()
        length = client.get_content_length("http://example.com/file")
        assert length is None
        client.close()

    @patch.object(requests.Session, 'head')
    def test_get_content_length_network_error(self, mock_head):
        """Test content length with network error."""
        mock_head.side_effect = requests.exceptions.ConnectionError()
        client = HTTPExtClient()
        with pytest.raises(NetworkError, match="Connection error"):
            client.get_content_length("http://example.com/file")
        client.close()

    @patch.object(requests.Session, 'head')
    def test_get_content_length_timeout(self, mock_head):
        """Test content length with timeout error."""
        mock_head.side_effect = requests.exceptions.Timeout()
        client = HTTPExtClient()
        with pytest.raises(NetworkError, match="Request timed out"):
            client.get_content_length("http://example.com/file")
        client.close()

    @patch.object(requests.Session, 'head')
    def test_get_content_length_request_exception(self, mock_head):
        """Test content length with generic request exception."""
        mock_head.side_effect = requests.exceptions.RequestException("Unknown")
        client = HTTPExtClient()
        with pytest.raises(HTTPExtError, match="Request failed"):
            client.get_content_length("http://example.com/file")
        client.close()


class TestRangeDownloadResult:
    """Tests for RangeDownloadResult dataclass."""

    def test_is_partial_true(self):
        """Test is_partial returns True for partial content."""
        result = RangeDownloadResult(
            data=b"data",
            start=0,
            end=99,
            total_size=1000,
            content_length=100,
        )
        assert result.is_partial is True

    def test_is_partial_false_full_content(self):
        """Test is_partial returns False for full content."""
        result = RangeDownloadResult(
            data=b"full",
            start=0,
            end=3,
            total_size=4,
            content_length=4,
        )
        assert result.is_partial is False

    def test_is_partial_false_unknown_total(self):
        """Test is_partial returns False when total_size is unknown."""
        result = RangeDownloadResult(
            data=b"data",
            start=0,
            end=99,
            total_size=None,
            content_length=100,
        )
        assert result.is_partial is False


class TestHTTPExtErrorHierarchy:
    """Tests for exception hierarchy."""

    def test_range_not_satisfiable_is_httpext_error(self):
        """Test RangeNotSatisfiableError inherits from HTTPExtError."""
        error = RangeNotSatisfiableError("test")
        assert isinstance(error, HTTPExtError)

    def test_network_error_is_httpext_error(self):
        """Test NetworkError inherits from HTTPExtError."""
        error = NetworkError("test")
        assert isinstance(error, HTTPExtError)

    def test_invalid_range_error_is_httpext_error(self):
        """Test InvalidRangeError inherits from HTTPExtError."""
        error = InvalidRangeError("test")
        assert isinstance(error, HTTPExtError)

    def test_range_not_satisfiable_with_content_length(self):
        """Test RangeNotSatisfiableError stores content_length."""
        error = RangeNotSatisfiableError("test", content_length=1000)
        assert error.content_length == 1000
        assert str(error) == "test"
