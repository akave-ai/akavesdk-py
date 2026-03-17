"""Unit tests for the HTTP Extension module (private/httpext/httpext.py).
Tests cover:
- range_download() with various byte ranges
- HTTP headers handling
- Response handling (200, 206, 416)
- Network errors
- Edge cases
"""
import io
import pytest
from unittest.mock import Mock, patch
import requests
from private.httpext import range_download


class TestRangeDownloadBasic:
    """Tests for basic range_download() functionality."""

    @patch.object(requests.Session, 'get')
    def test_range_download_success_206(self, mock_get):
        """Test successful range download with 206 Partial Content."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"Hello World"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 0, 10)
        
        assert result == b"Hello World"
        assert mock_session.get.called
        mock_response.close.assert_called_once()

    @patch.object(requests.Session, 'get')
    def test_range_download_success_200(self, mock_get):
        """Test range download when server returns 200 OK (no range support)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"Full content"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 0, 5)
        
        assert result == b"Full content"
        mock_response.close.assert_called_once()

    def test_range_download_invalid_length(self):
        """Test range_download with invalid length (zero or negative)."""
        mock_session = Mock()
        
        with pytest.raises(ValueError, match="length must be positive"):
            range_download(mock_session, "http://example.com/file", 0, 0)
        
        with pytest.raises(ValueError, match="length must be positive"):
            range_download(mock_session, "http://example.com/file", 0, -1)

    def test_range_download_invalid_offset(self):
        """Test range_download with negative offset."""
        mock_session = Mock()
        
        with pytest.raises(ValueError, match="offset must be non-negative"):
            range_download(mock_session, "http://example.com/file", -1, 100)

    @patch.object(requests.Session, 'get')
    def test_range_download_range_header(self, mock_get):
        """Test that correct Range header is sent."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"data"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        range_download(mock_session, "http://example.com/file", 100, 50)
        
        # Verify Range header was sent correctly (bytes=100-149)
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        headers = call_args[1]["headers"]
        assert headers["Range"] == "bytes=100-149"


class TestRangeDownloadErrors:
    """Tests for error handling in range_download()."""

    @patch.object(requests.Session, 'get')
    def test_range_download_416_range_not_satisfiable(self, mock_get):
        """Test handling of 416 Range Not Satisfiable response."""
        mock_response = Mock()
        mock_response.status_code = 416
        mock_response.content = b"Range Not Satisfiable"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        with pytest.raises(Exception, match="download failed with status 416"):
            range_download(mock_session, "http://example.com/file", 5000, 1000)

    @patch.object(requests.Session, 'get')
    def test_range_download_403_forbidden(self, mock_get):
        """Test handling of 403 Forbidden response."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.content = b"Forbidden"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        with pytest.raises(Exception, match="download failed with status 403"):
            range_download(mock_session, "http://example.com/file", 0, 100)

    @patch.object(requests.Session, 'get')
    def test_range_download_404_not_found(self, mock_get):
        """Test handling of 404 Not Found response."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.content = b"Not Found"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        with pytest.raises(Exception, match="download failed with status 404"):
            range_download(mock_session, "http://example.com/file", 0, 100)

    @patch.object(requests.Session, 'get')
    def test_range_download_request_exception(self, mock_get):
        """Test handling of request exceptions."""
        mock_session = Mock(spec=requests.Session)
        mock_session.get.side_effect = requests.RequestException("Connection failed")
        
        with pytest.raises(Exception, match="request failed"):
            range_download(mock_session, "http://example.com/file", 0, 100)

    @patch.object(requests.Session, 'get')
    def test_range_download_close_error_ignored(self, mock_get):
        """Test that errors closing response are logged but not raised."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"data"
        mock_response.close.side_effect = Exception("Close failed")
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        # Should not raise, but log warning
        result = range_download(mock_session, "http://example.com/file", 0, 10)
        assert result == b"data"


class TestRangeDownloadEdgeCases:
    """Tests for edge cases in range_download()."""

    @patch.object(requests.Session, 'get')
    def test_range_download_single_byte(self, mock_get):
        """Test downloading a single byte (length=1)."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"X"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 100, 1)
        
        assert result == b"X"
        call_args = mock_session.get.call_args
        headers = call_args[1]["headers"]
        assert headers["Range"] == "bytes=100-100"

    @patch.object(requests.Session, 'get')
    def test_range_download_large_range(self, mock_get):
        """Test downloading a large range."""
        large_data = b"X" * (10 * 1024 * 1024)  # 10MB
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = large_data
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 0, len(large_data))
        
        assert len(result) == len(large_data)

    @patch.object(requests.Session, 'get')
    def test_range_download_boundary_offset_max(self, mock_get):
        """Test with very large offset value."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"data"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        large_offset = 2**63 - 1  # Max 64-bit int
        result = range_download(mock_session, "http://example.com/file", large_offset, 100)
        
        call_args = mock_session.get.call_args
        headers = call_args[1]["headers"]
        end = large_offset + 100 - 1
        assert headers["Range"] == f"bytes={large_offset}-{end}"

    @patch.object(requests.Session, 'get')
    def test_range_download_timeout_parameter(self, mock_get):
        """Test that timeout parameter is passed correctly."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"data"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 0, 100, timeout=30.0)
        
        call_args = mock_session.get.call_args
        assert call_args[1]["timeout"] == 30.0
        assert result == b"data"

    @patch.object(requests.Session, 'get')
    def test_range_download_default_timeout(self, mock_get):
        """Test that default timeout is applied."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"data"
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 0, 100)
        
        call_args = mock_session.get.call_args
        assert call_args[1]["timeout"] == 10.0
        assert result == b"data"

    @patch.object(requests.Session, 'get')
    def test_range_download_empty_response(self, mock_get):
        """Test handling of empty response content."""
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b""
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 0, 100)
        
        assert result == b""

    @patch.object(requests.Session, 'get')
    def test_range_download_binary_data(self, mock_get):
        """Test downloading binary data."""
        binary_data = bytes(range(256))  # All byte values 0-255
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = binary_data
        mock_response.close = Mock()
        
        mock_session = Mock(spec=requests.Session)
        mock_session.get.return_value = mock_response
        
        result = range_download(mock_session, "http://example.com/file", 0, len(binary_data))
        
        assert result == binary_data
        assert len(result) == 256
