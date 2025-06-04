#!/usr/bin/env python3
"""
Test script to verify Akave SDK Streaming API functionality
"""
import os
import sys
import time
import uuid
import logging
import tempfile
import random
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from akavesdk import SDK
from akavesdk import SDKError

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DEFAULT_CONFIG = {
    'AKAVE_SDK_NODE': 'connect.akave.ai:5000',  
    'ENCRYPTION_KEY': '',  
}

def get_env_or_default(key: str) -> str:
    return os.getenv(key, DEFAULT_CONFIG.get(key, ''))

def validate_configuration():
    return True

def create_test_file(size_kb: int = 200):
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    data = bytes(random.getrandbits(8) for _ in range(size_kb * 1024))
    with open(temp_file.name, 'wb') as f:
     f.write(data)
    return temp_file.name

def test_streaming_connection(sdk):
    try:
        streaming = sdk.streaming_api()
        logging.info("✓ Streaming API initialized successfully")
        return True
    except Exception as e:
        logging.error(f"Error initializing streaming API: {str(e)}")
        return False

def test_file_operations(sdk, bucket_name: str):
    try:
        streaming = sdk.streaming_api()
        test_file_path = create_test_file(200)  # 200KB test file
        test_file_name = f"test-file-{uuid.uuid4().hex[:8]}.dat"
        
        logging.info(f"Using test file: {test_file_path} ({os.path.getsize(test_file_path)} bytes)")
        logging.info(f"Uploading as: {test_file_name}")
        with open(test_file_path, 'rb') as f:
            try:
                start_time = time.time()
                file_upload = streaming.create_file_upload(None, bucket_name, test_file_name)
                file_meta = streaming.upload(None, file_upload, f)
                upload_time = time.time() - start_time
                
                logging.info(f"✓ File uploaded: {file_meta.name}")
                logging.info(f"  Size: {file_meta.size} bytes")
                logging.info(f"  Upload time: {upload_time:.2f} seconds")
                logging.info(f"  Upload speed: {(file_meta.size / upload_time / 1024):.2f} KB/s")
            except Exception as e:
                logging.error(f"Error uploading file: {str(e)}")
                return False
        
        try:
            files = streaming.list_files(None, bucket_name)
            logging.info(f"✓ Listed files in bucket: {len(files)} found")
            
            found = False
            for file in files:
                logging.info(f"  - {file.name} ({file.size} bytes)")
                if file.name == test_file_name:
                    found = True
            
            if not found:
                logging.warning(f"Uploaded file {test_file_name} not found in file listing")
        except Exception as e:
            logging.error(f"Error listing files: {str(e)}")
            return False
        
        try:
            file_info = streaming.file_info(None, bucket_name, test_file_name)
            logging.info(f"✓ File info retrieved")
            logging.info(f"  Name: {file_info.name}")
            logging.info(f"  Size: {file_info.size} bytes")
            logging.info(f"  Created: {file_info.created_at}")
        except Exception as e:
            logging.error(f"Error getting file info: {str(e)}")
            return False
        
        download_path = f"{test_file_path}_downloaded"
        try:
            with open(download_path, 'wb') as f:
                start_time = time.time()
                download = streaming.create_file_download(None, bucket_name, test_file_name)
                streaming.download(None, download, f)
                download_time = time.time() - start_time
                
                download_size = os.path.getsize(download_path)
                logging.info(f"✓ File downloaded to: {download_path}")
                logging.info(f"  Size: {download_size} bytes")
                logging.info(f"  Download time: {download_time:.2f} seconds")
                logging.info(f"  Download speed: {(download_size / download_time / 1024):.2f} KB/s")
        except Exception as e:
            logging.error(f"Error downloading file: {str(e)}")
            return False
        
        try:
            with open(test_file_path, 'rb') as f1, open(download_path, 'rb') as f2:
                original_data = f1.read()
                downloaded_data = f2.read()
                
                if original_data == downloaded_data:
                    logging.info("✓ Downloaded file integrity verified (matches original)")
                else:
                    logging.error("✗ Downloaded file does not match original")
                    return False
        except Exception as e:
            logging.error(f"Error verifying file integrity: {str(e)}")
            return False
        
        try:
            os.unlink(test_file_path)
            os.unlink(download_path)
            logging.info("✓ Test files cleaned up")
        except Exception as e:
            logging.warning(f"Could not clean up test files: {str(e)}")
        
        try:
            streaming.file_delete(None, bucket_name, test_file_name)
            logging.info(f"✓ File {test_file_name} deleted from bucket")
        except Exception as e:
            logging.error(f"Error deleting file: {str(e)}")
            return False
        
        return True
    except Exception as e:
        logging.error(f"Error in file operations test: {str(e)}")
        logging.error("Stack trace:", exc_info=True)
        return False

def main():
    if not validate_configuration():
        sys.exit(1)

    sdk_node_address = get_env_or_default("AKAVE_SDK_NODE")
    encryption_key =  None
    
    bucket_name = f"test-bucket-{uuid.uuid4().hex[:8]}"
    
    logging.getLogger().setLevel(logging.INFO)

    print(f"=== Akave SDK Streaming API Test ===\n")
    logging.info(f"Connecting to Akave streaming node: {sdk_node_address}")
    logging.info(f"Using test bucket: {bucket_name}")

    try:
        sdk = SDK(
            address=sdk_node_address,
            max_concurrency=10,
            block_part_size=100000,  # ~100KB chunks
            use_connection_pool=True,
            encryption_key=encryption_key,
            streaming_max_blocks_in_chunk=32,
            parity_blocks_count=0
        )
        logging.info("✓ SDK initialized successfully")

        try:
            logging.info(f"Creating test bucket: {bucket_name}")
            result = sdk.create_bucket(None, bucket_name)
            logging.info(f"✓ Test bucket created: {result.name}")
        except Exception as e:
            logging.error(f"Failed to create test bucket: {str(e)}")
            sys.exit(1)

        connection_ok = test_streaming_connection(sdk)
        file_ops_ok = test_file_operations(sdk, bucket_name)

        print("\nTest Summary:")
        print(f"Streaming Connection: {'✓' if connection_ok else '✗'}")
        print(f"File Operations: {'✓' if file_ops_ok else '✗'}")

        success = connection_ok and file_ops_ok
        print(f"\nOverall: {'✓ SUCCESS' if success else '✗ FAILURE'}")

        try:
            logging.info(f"Cleaning up test bucket: {bucket_name}")
            sdk.delete_bucket(None, bucket_name)
            logging.info(f"✓ Test bucket deleted")
        except Exception as e:
            logging.warning(f"Could not delete test bucket: {str(e)}")

        sdk.close()
        logging.info("Connection closed")

        if not success:
            sys.exit(1)

    except Exception as e:
        logging.error(f"Error during testing: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 