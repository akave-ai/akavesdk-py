import logging
from typing import Optional

import requests


def range_download(
    client: requests.Session,
    url: str,
    offset: int,
    length: int,
    timeout: Optional[float] = 10.0,
) -> bytes:
    """
    Download a specific byte range from *url* using the given HTTP client.

    The function raises ``ValueError`` if the range is invalid and a generic
    ``Exception`` for network or HTTP errors.
    """
    if length <= 0 or offset < 0:
        raise ValueError("length must be positive and offset must be non-negative")

    end = offset + length - 1
    headers = {"Range": f"bytes={offset}-{end}"}

    try:
        response = client.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise Exception(f"request failed: {exc}") from exc

    try:
        # Some CDNs may return 200 OK for range requests.
        if response.status_code not in (requests.codes.partial_content, requests.codes.ok):
            try:
                body = response.content
            except Exception as body_exc:  # pragma: no cover - extremely rare
                logging.warning("failed to read error response body: %s", body_exc)
                body = b""

            body_text = body.decode(errors="replace")
            raise Exception(
                f"download failed with status {response.status_code}: {body_text}"
            )

        try:
            data = response.content
        except requests.RequestException as exc:
            raise Exception(f"failed to read response body: {exc}") from exc

        return data
    finally:
        try:
            response.close()
        except Exception as close_exc:  # pragma: no cover - defensive
            logging.debug("error closing HTTP response: %s", close_exc)


