#!/usr/bin/env python3
"""
Test script to verify SDK functionality using installed akavesdk package
"""
import os
import sys
import time
import uuid
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from akavesdk import SDK, SDKError
import private.pb.ipcnodeapi_pb2 as ipcnodeapi_pb2

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Default configuration
DEFAULT_CONFIG = {
    'AKAVE_SDK_NODE': 'connect.akave.ai:5000',  # For streaming operations
    'AKAVE_IPC_NODE': 'connect.akave.ai:5500',  # For IPC operations
    'ETHEREUM_NODE_URL': 'https://n3-us.akave.ai/ext/bc/2JMWNmZbYvWcJRPPy1siaDBZaDGTDAaqXoY5UBKh4YrhNFzEce/rpc',
    'STORAGE_CONTRACT_ADDRESS': '0x9Aa8ff1604280d66577ecB5051a3833a983Ca3aF',  # Will be obtained from node
    'ACCESS_CONTRACT_ADDRESS': '',   # Will be obtained from node
}

def get_env_or_default(key: str) -> str:
    """Get environment variable or default value."""
    return os.getenv(key, DEFAULT_CONFIG.get(key, ''))

def validate_configuration():
    """Validate required configuration."""
    missing = []
    
    # Check for required environment variables
    if not os.getenv('PRIVATE_KEY'):
        missing.append('PRIVATE_KEY')
    
    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        logging.error("\nPlease create a .env file with the following content:")
        logging.error("""
PRIVATE_KEY=your_ethereum_private_key
# Optional configurations (will use defaults if not set):
AKAVE_SDK_NODE=connect.akave.ai:5000
AKAVE_IPC_NODE=connect.akave.ai:5500
ETHEREUM_NODE_URL=https://n3-us.akave.ai/ext/bc/2JMWNmZbYvWcJRPPy1siaDBZaDGTDAaqXoY5UBKh4YrhNFzEce/rpc
ENCRYPTION_KEY=your_encryption_key
""")
        return False
    return True

def test_basic_operations(sdk):
    """Test basic bucket operations"""
    try:
        # Generate a unique bucket name
        bucket_name = f"test-bucket-{uuid.uuid4().hex[:8]}"
        logging.info(f"Testing with bucket: {bucket_name}")

        # Test bucket creation
        logging.info("Creating bucket...")
        result = sdk.create_bucket(None, bucket_name)
        logging.info(f"✓ Bucket created: {result.name}")
        logging.info(f"  Created at: {result.created_at}")

        # Test bucket viewing
        logging.info("\nViewing bucket...")
        bucket = sdk.view_bucket(None, bucket_name)
        logging.info(f"✓ Bucket found: {bucket.name}")
        logging.info(f"  Created at: {bucket.created_at}")

        logging.info("\nDeleting bucket...")
        sdk.delete_bucket(None, bucket_name)
        logging.info(f"✓ Bucket {bucket_name} deleted")

        return True

    except SDKError as e:
        logging.error(f"SDK Error in basic operations: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error in basic operations: {str(e)}")
        return False

def test_streaming_api(sdk):
    """Test streaming API operations"""
    try:
        streaming = sdk.streaming_api()
        logging.info("✓ Streaming API initialized")
        return True
    except Exception as e:
        logging.error(f"Error initializing streaming API: {str(e)}")
        return False

def test_ipc_api(sdk):
    """Test IPC API operations"""
    try:
        logging.info("Initializing IPC API...")
        
        # Debug: Print SDK client type
        logging.debug(f"SDK client type: {type(sdk.client)}")
        
        # Debug: Print available methods
        logging.debug("Available methods on client:")
        for method in dir(sdk.client):
            if not method.startswith('_'):
                logging.debug(f"  - {method}")
        
        # Try to get IPC instance
        logging.info("Getting IPC instance...")
        ipc = sdk.ipc()
        
        if ipc:
            logging.info("✓ IPC API initialized successfully")
            
            # Debug: Print IPC client type
            logging.debug(f"IPC client type: {type(ipc.client)}")
            
            # Debug: Print available methods on IPC client
            logging.debug("Available methods on IPC client:")
            for method in dir(ipc.client):
                if not method.startswith('_'):
                    logging.debug(f"  - {method}")
            
            # Test basic IPC functionality if possible
            try:
                logging.info("Testing IPC connection parameters...")
                params_request = ipcnodeapi_pb2.ConnectionParamsRequest()
                conn_params = ipc.client.ConnectionParams(params_request)
                logging.info(f"✓ Connection parameters received: {conn_params}")
            except Exception as e:
                logging.warning(f"Could not get connection parameters: {str(e)}")
        
        return True
    except Exception as e:
        logging.error(f"Error initializing IPC API: {str(e)}")
        logging.error("Stack trace:", exc_info=True)  # Print full stack trace
        return False

def main():
    # Validate configuration before proceeding
    if not validate_configuration():
        sys.exit(1)

    # Get configuration from environment or use defaults
    sdk_node_address = get_env_or_default("AKAVE_SDK_NODE")
    ipc_node_address = get_env_or_default("AKAVE_IPC_NODE")
    ethereum_node = get_env_or_default("ETHEREUM_NODE_URL")
    private_key = os.getenv("PRIVATE_KEY")
    encryption_key = None  # Setting to None to avoid length errors
    
    # Set debug level for more detailed logging
    logging.getLogger().setLevel(logging.DEBUG)

    print("=== Akave SDK Functionality Test ===\n")
    logging.info(f"Connecting to Akave SDK node: {sdk_node_address}")
    logging.info(f"Connecting to Akave IPC node: {ipc_node_address}")
    logging.info(f"Using Ethereum node: {ethereum_node}")

    try:
        # Initialize SDK with default parameters
        sdk = SDK(
            address=sdk_node_address,  # Use SDK node for general operations
            max_concurrency=10,
            block_part_size=100000,  # ~100KB for testing
            use_connection_pool=True,
            encryption_key=encryption_key,
            private_key=private_key,
            streaming_max_blocks_in_chunk=32,
            parity_blocks_count=0,
            ipc_address=ipc_node_address  # Use IPC node for IPC operations
        )
        logging.info("✓ SDK initialized successfully")

        # Run tests
        basic_ok = test_basic_operations(sdk)
        streaming_ok = test_streaming_api(sdk)
        ipc_ok = test_ipc_api(sdk)

        # Print summary
        print("\nTest Summary:")
        print(f"Basic Operations: {'✓' if basic_ok else '✗'}")
        print(f"Streaming API: {'✓' if streaming_ok else '✗'}")
        print(f"IPC API: {'✓' if ipc_ok else '✗'}")

        # Cleanup
        sdk.close()
        logging.info("Connection closed")

    except Exception as e:
        logging.error(f"Error during testing: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 