import hashlib
import logging
from typing import Optional, Tuple
import grpc
from .sdk import SDKError

logger = logging.getLogger(__name__)

class ChunkVerifier:
    """Handles chunk verification and integrity checks"""
    
    def __init__(self, chunk_size: int = 1024 * 1024):
        self.chunk_size = chunk_size
    
    def calculate_chunk_checksum(self, data: bytes) -> str:
        """Calculate SHA-256 checksum for a chunk"""
        return hashlib.sha256(data).hexdigest()
    
    def verify_chunk(self, data: bytes, expected_checksum: str) -> Tuple[bool, Optional[str]]:
        """
        Verify chunk integrity
        
        Args:
            data: Chunk data to verify
            expected_checksum: Expected SHA-256 checksum
            
        Returns:
            Tuple[bool, Optional[str]]: (verification success, error message if failed)
        """
        try:
            actual_checksum = self.calculate_chunk_checksum(data)
            if actual_checksum != expected_checksum:
                return False, f"Checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
            return True, None
        except Exception as e:
            logger.error(f"Chunk verification failed: {str(e)}")
            return False, str(e)
    
    def verify_chunk_sequence(self, chunks: list[bytes], expected_checksums: list[str]) -> Tuple[bool, Optional[str]]:
        """
        Verify a sequence of chunks
        
        Args:
            chunks: List of chunk data
            expected_checksums: List of expected checksums
            
        Returns:
            Tuple[bool, Optional[str]]: (verification success, error message if failed)
        """
        if len(chunks) != len(expected_checksums):
            return False, f"Chunk count mismatch: expected {len(expected_checksums)}, got {len(chunks)}"
        
        for i, (chunk, expected_checksum) in enumerate(zip(chunks, expected_checksums)):
            success, error = self.verify_chunk(chunk, expected_checksum)
            if not success:
                return False, f"Chunk {i} verification failed: {error}"
        
        return True, None
    
    def verify_chunk_with_retry(self, data: bytes, expected_checksum: str, max_retries: int = 3) -> Tuple[bool, Optional[str]]:
        """
        Verify chunk with retry mechanism
        
        Args:
            data: Chunk data to verify
            expected_checksum: Expected SHA-256 checksum
            max_retries: Maximum number of retry attempts
            
        Returns:
            Tuple[bool, Optional[str]]: (verification success, error message if failed)
        """
        for attempt in range(max_retries):
            success, error = self.verify_chunk(data, expected_checksum)
            if success:
                return True, None
            logger.warning(f"Chunk verification attempt {attempt + 1} failed: {error}")
        
        return False, f"Chunk verification failed after {max_retries} attempts"
    
    def verify_chunk_with_timeout(self, data: bytes, expected_checksum: str, timeout: float = 5.0) -> Tuple[bool, Optional[str]]:
        """
        Verify chunk with timeout
        
        Args:
            data: Chunk data to verify
            expected_checksum: Expected SHA-256 checksum
            timeout: Timeout in seconds
            
        Returns:
            Tuple[bool, Optional[str]]: (verification success, error message if failed)
        """
        try:
            import threading
            result = [None]
            error = [None]
            
            def verify():
                try:
                    success, err = self.verify_chunk(data, expected_checksum)
                    result[0] = success
                    error[0] = err
                except Exception as e:
                    error[0] = str(e)
            
            thread = threading.Thread(target=verify)
            thread.start()
            thread.join(timeout)
            
            if thread.is_alive():
                return False, f"Chunk verification timed out after {timeout} seconds"
            
            if error[0]:
                return False, error[0]
            
            return result[0], None
            
        except Exception as e:
            logger.error(f"Chunk verification with timeout failed: {str(e)}")
            return False, str(e) 