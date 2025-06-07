import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import multiformats.cid as cid
import multiformats.multihash as multihash


class SPClient:
    """Client for communication with Filecoin Storage Provider (SP)."""

    def __init__(self) -> None:
        """
        Initializes the SPClient with a session for HTTP requests.
        Sets up retry logic for handling transient errors.
        """
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def close(self) -> None:
        """Closes the HTTP session."""
        self.session.close()

    def fetch_block(self, sp_base_url: str, cid_str: str) -> bytes:
        """
        Fetches a block from the Filecoin provider.

        :param sp_base_url: Base URL of the storage provider.
        :param cid_str: Content Identifier (CID) of the block.
        :return: Raw block data.
        :raises: Exception if the request fails or block retrieval fails.
        """
        url = f"{sp_base_url}/ipfs/{cid_str}?format=raw"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch block: {e}") from e


# Example usage:
if __name__ == "__main__":
    sp_client = SPClient()
    try:
        block_cid = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"  # Example CID
        base_url = "https://filecoin-provider.com"
        block_data = sp_client.fetch_block(base_url, block_cid)
        print(f"Fetched block ({block_cid}): {block_data[:25].hex()}...")  # Print first 25 bytes
    finally:
        sp_client.close()
