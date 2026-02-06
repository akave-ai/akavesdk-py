"""
HTTP Extension module for range-based downloads.

This module provides HTTP Range header support for partial content downloads,
enabling efficient retrieval of specific byte ranges from remote resources.
"""

from dataclasses import dataclass
from typing import Optional, Tuple, BinaryIO
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class HTTPExtError(Exception):
    """Base exception for HTTP extension errors."""
    pass


class RangeNotSatisfiableError(HTTPExtError):
    """Raised when the requested range cannot be satisfied (HTTP 416)."""

    def __init__(self, message: str, content_length: Optional[int] = None):
        super().__init__(message)
        self.content_length = content_length


class NetworkError(HTTPExtError):
    """Raised when a network-related error occurs."""
    pass


class InvalidRangeError(HTTPExtError):
    """Raised when an invalid range is specified."""
    pass


@dataclass
class RangeDownloadResult:
    """Result of a range download operation."""

    data: bytes
    start: int
    end: int
    total_size: Optional[int]
    content_length: int

    @property
    def is_partial(self) -> bool:
        """Returns True if this is a partial content response."""
        return self.total_size is not None and self.content_length < self.total_size


class HTTPExtClient:
    """
    HTTP client with support for range-based downloads.

    This client handles HTTP Range requests for partial content retrieval,
    with proper handling of various response codes and error conditions.
    """

    DEFAULT_TIMEOUT = 30
    DEFAULT_RETRIES = 3
    DEFAULT_BACKOFF_FACTOR = 0.3

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    ):
        """
        Initialize the HTTP extension client.

        Args:
            timeout: Request timeout in seconds.
            retries: Number of retry attempts for failed requests.
            backoff_factor: Backoff factor for retry delays.
        """
        self.timeout = timeout
        self.session = requests.Session()

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def close(self) -> None:
        """Close the HTTP session and release resources."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def range_download(
        self,
        url: str,
        start: int,
        end: Optional[int] = None,
        headers: Optional[dict] = None,
    ) -> RangeDownloadResult:
        """
        Download a specific byte range from a URL.

        Implements HTTP Range requests as per RFC 7233. Supports both
        bounded ranges (start-end) and suffix ranges (start-).

        Args:
            url: The URL to download from.
            start: The starting byte position (0-indexed).
            end: The ending byte position (inclusive). If None, downloads to end of file.
            headers: Additional headers to include in the request.

        Returns:
            RangeDownloadResult containing the downloaded data and metadata.

        Raises:
            InvalidRangeError: If the range parameters are invalid.
            RangeNotSatisfiableError: If the server returns 416 (Range Not Satisfiable).
            NetworkError: If a network error occurs.
            HTTPExtError: For other HTTP errors.
        """
        self._validate_range(start, end)

        range_header = self._build_range_header(start, end)
        request_headers = {"Range": range_header}

        if headers:
            request_headers.update(headers)

        try:
            response = self.session.get(
                url,
                headers=request_headers,
                timeout=self.timeout,
            )

            return self._handle_response(response, start, end)

        except requests.exceptions.Timeout as e:
            raise NetworkError(f"Request timed out: {e}") from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {e}") from e
        except requests.exceptions.RequestException as e:
            raise HTTPExtError(f"Request failed: {e}") from e

    def range_download_to_file(
        self,
        url: str,
        start: int,
        end: Optional[int],
        writer: BinaryIO,
        headers: Optional[dict] = None,
        chunk_size: int = 8192,
    ) -> RangeDownloadResult:
        """
        Download a specific byte range from a URL directly to a file.

        This method streams the response to avoid loading large ranges into memory.

        Args:
            url: The URL to download from.
            start: The starting byte position (0-indexed).
            end: The ending byte position (inclusive). If None, downloads to end of file.
            writer: A binary file-like object to write the data to.
            headers: Additional headers to include in the request.
            chunk_size: Size of chunks to read/write at a time.

        Returns:
            RangeDownloadResult containing metadata (data field will be empty).

        Raises:
            InvalidRangeError: If the range parameters are invalid.
            RangeNotSatisfiableError: If the server returns 416 (Range Not Satisfiable).
            NetworkError: If a network error occurs.
            HTTPExtError: For other HTTP errors.
        """
        self._validate_range(start, end)

        range_header = self._build_range_header(start, end)
        request_headers = {"Range": range_header}

        if headers:
            request_headers.update(headers)

        try:
            response = self.session.get(
                url,
                headers=request_headers,
                timeout=self.timeout,
                stream=True,
            )

            # Handle error responses before streaming
            if response.status_code == 416:
                content_length = self._parse_content_length_from_416(response)
                raise RangeNotSatisfiableError(
                    f"Range not satisfiable: {start}-{end}",
                    content_length=content_length,
                )

            if response.status_code not in (200, 206):
                raise HTTPExtError(
                    f"Unexpected status code: {response.status_code}"
                )

            # Stream content to writer
            bytes_written = 0
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    writer.write(chunk)
                    bytes_written += len(chunk)

            # Parse content range
            actual_start, actual_end, total_size = self._parse_content_range(
                response.headers.get("Content-Range")
            )

            return RangeDownloadResult(
                data=b"",
                start=actual_start if actual_start is not None else start,
                end=actual_end if actual_end is not None else start + bytes_written - 1,
                total_size=total_size,
                content_length=bytes_written,
            )

        except requests.exceptions.Timeout as e:
            raise NetworkError(f"Request timed out: {e}") from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {e}") from e
        except requests.exceptions.RequestException as e:
            raise HTTPExtError(f"Request failed: {e}") from e

    def get_content_length(self, url: str, headers: Optional[dict] = None) -> Optional[int]:
        """
        Get the content length of a resource using a HEAD request.

        Args:
            url: The URL to query.
            headers: Additional headers to include in the request.

        Returns:
            The content length in bytes, or None if not available.

        Raises:
            NetworkError: If a network error occurs.
            HTTPExtError: For other HTTP errors.
        """
        try:
            response = self.session.head(url, headers=headers, timeout=self.timeout)
            response.raise_for_status()

            content_length = response.headers.get("Content-Length")
            if content_length:
                return int(content_length)
            return None

        except requests.exceptions.Timeout as e:
            raise NetworkError(f"Request timed out: {e}") from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {e}") from e
        except requests.exceptions.RequestException as e:
            raise HTTPExtError(f"Request failed: {e}") from e

    def _validate_range(self, start: int, end: Optional[int]) -> None:
        """Validate range parameters."""
        if start < 0:
            raise InvalidRangeError("Start position cannot be negative")

        if end is not None:
            if end < 0:
                raise InvalidRangeError("End position cannot be negative")
            if end < start:
                raise InvalidRangeError("End position cannot be less than start position")

    def _build_range_header(self, start: int, end: Optional[int]) -> str:
        """Build the HTTP Range header value."""
        if end is not None:
            return f"bytes={start}-{end}"
        return f"bytes={start}-"

    def _handle_response(
        self, response: requests.Response, start: int, end: Optional[int]
    ) -> RangeDownloadResult:
        """Handle the HTTP response and build the result."""
        if response.status_code == 416:
            content_length = self._parse_content_length_from_416(response)
            raise RangeNotSatisfiableError(
                f"Range not satisfiable: {start}-{end}",
                content_length=content_length,
            )

        if response.status_code == 206:
            # Partial content - parse Content-Range header
            actual_start, actual_end, total_size = self._parse_content_range(
                response.headers.get("Content-Range")
            )

            return RangeDownloadResult(
                data=response.content,
                start=actual_start if actual_start is not None else start,
                end=actual_end if actual_end is not None else start + len(response.content) - 1,
                total_size=total_size,
                content_length=len(response.content),
            )

        if response.status_code == 200:
            # Server doesn't support range requests, returned full content
            return RangeDownloadResult(
                data=response.content,
                start=0,
                end=len(response.content) - 1,
                total_size=len(response.content),
                content_length=len(response.content),
            )

        raise HTTPExtError(f"Unexpected status code: {response.status_code}")

    def _parse_content_range(
        self, content_range: Optional[str]
    ) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Parse the Content-Range header.

        Format: bytes start-end/total or bytes start-end/*

        Returns:
            Tuple of (start, end, total_size). total_size may be None if unknown.
        """
        if not content_range:
            return None, None, None

        try:
            # Format: "bytes start-end/total"
            if not content_range.startswith("bytes "):
                return None, None, None

            range_part = content_range[6:]  # Remove "bytes "
            range_spec, size_spec = range_part.split("/")

            start_str, end_str = range_spec.split("-")
            start = int(start_str)
            end = int(end_str)

            total_size = None if size_spec == "*" else int(size_spec)

            return start, end, total_size
        except (ValueError, IndexError):
            return None, None, None

    def _parse_content_length_from_416(self, response: requests.Response) -> Optional[int]:
        """Extract content length from a 416 response if available."""
        content_range = response.headers.get("Content-Range")
        if content_range:
            # Format might be "bytes */total"
            try:
                if content_range.startswith("bytes */"):
                    return int(content_range[8:])
            except ValueError:
                pass
        return None
