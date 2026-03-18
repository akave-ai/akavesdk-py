# Akave SDK for Python

The Akave SDK for Python provides a simple interface to interact with the Akave decentralized network.

## Installation

You can install the SDK using pip:

```bash
pip install akavesdk
```

Or install directly from GitHub:

```bash
pip install git+https://github.com/d4v1d03/akavesdk-py.git
```

## Authentication

The Akave SDK uses two main authentication methods:

1. **Blockchain-based authentication (IPC API)** - This uses Ethereum wallet private keys for IPC operations on the blockchain. This is required for operations that require blockchain transactions (creating buckets, managing file permissions, etc.).

2. **Standard API connection** - Basic operations like file uploads/downloads use gRPC connections to Akave nodes.

### Private Key Security

**Always be careful when dealing with your private key. Double-check that you're not hardcoding it anywhere or committing it to Git. Remember: anyone with access to your private key has complete control over your funds.**

Ensure you're not reusing a private key that's been deployed on other EVM chains. Each blockchain has its own attack vectors, and reusing keys across chains exposes you to cross-chain vulnerabilities. Keep separate keys to maintain isolation and protect your assets.

You can set up your authentication in several ways:

### Environment variables (recommended)

```bash
# Set these environment variables
export AKAVE_SDK_NODE="connect.akave.ai:5000"  # Default Akave node endpoint for streaming operations
export AKAVE_IPC_NODE="connect.akave.ai:5500"  # Default Akave node endpoint for IPC operations
export AKAVE_PRIVATE_KEY="your_ethereum_private_key"  # Required for blockchain operations
export AKAVE_ENCRYPTION_KEY="your_32_byte_encryption_key"  # Optional, for file encryption
```

#### Secure Private Key Management

For better security, store your private key in a file with restricted permissions:

```bash
# Create a secure key file
mkdir -p ~/.key
echo "your-private-key-content" > ~/.key/user.akvf.key
chmod 600 ~/.key/user.akvf.key

# Use the key file in your environment
export AKAVE_PRIVATE_KEY="$(cat ~/.key/user.akvf.key)"
```

### Direct initialization

```python
from akavesdk import SDK, SDKConfig

# Configuration
config = SDKConfig(
    address="connect.akave.ai:5000",  # Akave endpoint for streaming operations
    max_concurrency=10,
    block_part_size=1 * 1024 * 1024,  # 1MB
    use_connection_pool=True,
    private_key="your_ethereum_private_key",  # Required for IPC API operations
    encryption_key=b"your_32_byte_encryption_key",  # Optional, for file encryption
    ipc_address="connect.akave.ai:5500",  # Akave endpoint for IPC operations (optional)
    connection_timeout = 30 # 30 seconds (optional) 
)

# Initialize with explicit parameters
sdk = SDK(config)
```

### Getting credentials

To get an Akave wallet address and add the chain to MetaMask:

1. Visit the [Akave Faucet](https://faucet.akave.ai) to connect and add the Akave chain to MetaMask
2. Request funds from the faucet
3. Export your private key from MetaMask (Settings -> Account details -> Export private key)

You can check your transactions on the [Akave Blockchain Explorer](https://explorer.akave.ai)

## Usage

### IPC API Usage (Blockchain-based, Recommended)

The IPC API is the recommended approach for interacting with Akave's decentralized storage. It provides access to Akave's smart contracts, enabling secure, blockchain-based bucket and file operations.

```python
import os
import time
from akavesdk import SDK, SDKConfig, SDKError

# Initialize the SDK with IPC configuration
config = SDKConfig(
    address="connect.akave.ai:5500",  # IPC node endpoint
    private_key=os.environ.get("AKAVE_PRIVATE_KEY"),  # Required for IPC operations
    max_concurrency=5,
    block_part_size=128 * 1024,  # 128KB
    use_connection_pool=True,
    chunk_buffer=10,
    connection_timeout=30  # 30 seconds (optional)
)

sdk = SDK(config)

try:
    # Step 1: Get IPC API interface
    ipc = sdk.ipc()
    print("✅ IPC instance created")
    
    # Step 2: Check if bucket exists, create if it doesn't
    bucket_name = "my-bucket"
    existing_bucket = ipc.view_bucket(None, bucket_name)
    
    if existing_bucket is None:
        print(f"Creating bucket '{bucket_name}'...")
        bucket_result = ipc.create_bucket(None, bucket_name)
        print(f"✅ Bucket created: {bucket_result.name} (ID: {bucket_result.id})")
        time.sleep(2)  # Wait for blockchain confirmation
    else:
        print(f"✅ Bucket already exists: {existing_bucket.name}")
    
    # Step 3: List buckets
    # list_buckets(ctx, offset=0, limit=0) - limit=0 returns all buckets
    buckets = ipc.list_buckets(None, offset=0, limit=0)
    print(f"Found {len(buckets)} bucket(s):")
    for bucket in buckets:
        print(f"  - {bucket.name} (Created: {bucket.created_at})")
    
    # You can also use pagination:
    # first_10 = ipc.list_buckets(None, offset=0, limit=10)  # First 10 buckets
    # next_10 = ipc.list_buckets(None, offset=10, limit=10)  # Next 10 buckets
    
    # Step 4: Upload a file (minimum file size is 127 bytes, max recommended test size: 100MB)
    # Note: upload() handles file creation and all transactions automatically
    file_name = f"test-file-{int(time.time())}.txt"
    
    # Create test file
    with open(file_name, "w") as f:
        f.write("Hello from Akave SDK! This is a test file. " * 5)
    print(f"Created test file: {file_name}")
    
    with open(file_name, "rb") as f:
        file_meta = ipc.upload(None, bucket_name, file_name, f)
        print(f"✅ Uploaded file: {file_meta.name}")
        print(f"   Root CID: {file_meta.root_cid}")
        print(f"   Size: {file_meta.size} bytes")
        print(f"   Encoded Size: {file_meta.encoded_size} bytes")
    
    # Step 5: Verify file upload
    retrieved_meta = ipc.file_info(None, bucket_name, file_name)
    if retrieved_meta:
        print(f"✅ File metadata verified:")
        print(f"   Name: {retrieved_meta.name}")
        print(f"   Bucket: {retrieved_meta.bucket_name}")
        print(f"   Root CID: {retrieved_meta.root_cid}")
    
    # Step 6: List files in bucket
    files = ipc.list_files(None, bucket_name)
    print(f"✅ Found {len(files)} file(s) in bucket '{bucket_name}':")
    for file in files:
        print(f"  - {file.name} (Size: {file.encoded_size} bytes)")
    
    # Step 7: Download a file
    with open("downloaded-file.txt", "wb") as f:
        download = ipc.create_file_download(None, bucket_name, file_name)
        ipc.download(None, download, f)
        print(f"✅ Downloaded file with {len(download.chunks)} chunks")
    
    # Step 8: Delete a file
    ipc.file_delete(None, bucket_name, file_name)
    print("✅ File deleted successfully")
    
except SDKError as e:
    print(f"❌ SDK Error: {e}")
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {str(e)}")
finally:
    # Always close the connection when done
    sdk.close()
    print("Connection closed")
```


## File Size Requirements

- **Minimum file size**: 127 bytes
- **Maximum recommended test size**: 100MB

## Development

To set up the development environment:

1. Clone the repository:
```bash
git clone https://github.com/d4v1d03/akavesdk-py.git
cd akavesdk-py
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e .
```

4. Run tests:
```bash
pytest
```

## Node Address

The current public endpoint for the blockchain-based network is:
```
connect.akave.ai:5500
```

## Data Model

The Akave SDK for Python uses a set of Python dataclasses to represent the data structures used by the Akave network. These are Python equivalents of the Go structs used in the original SDK, adapted to follow Python conventions and best practices.

### Core Data Types

- **CIDType**: Content identifier for files, chunks, and blocks
- **TimestampType**: Union type that can represent timestamps in different formats (datetime, float, int)

### Bucket Operations

- **Bucket**: Represents a storage bucket in the Akave network
- **BucketCreateResult**: Result of a bucket creation operation

### File & Streaming Operations

- **FileMeta**: Contains metadata for a file (ID, size, timestamps)
- **Chunk**: Represents a piece of a file with its own metadata
- **Block**: The smallest unit of data storage, identified by a CID

### File Operations Models

- **FileListItem**: Used when listing files in a bucket
- **FileUpload/FileDownload**: Contains file metadata for upload/download operations
- **FileChunkUpload/FileChunkDownload**: Represents chunks during file transfer operations

### IPC Operations Models

- **IPCBucket**: Blockchain-based bucket representation
- **IPCFileMetaV2**: Extended file metadata for IPC operations
- **IPCFileChunkUploadV2**: Chunk metadata for IPC operations

The model structure is designed to be intuitive to Python developers while maintaining compatibility with the Akave API. All serialization/deserialization between Python objects and gRPC messages is handled automatically by the SDK.

## Contributing

We welcome contributions! Before submitting a PR, please ensure your code passes all quality checks.

### Code Quality Requirements

Run these commands before pushing to ensure CI passes:

```bash
# Install code quality tools
pip install black isort flake8

# 1. Format code with Black
black akavesdk/ private/ sdk/ tests/

# 2. Sort imports with isort
isort akavesdk/ private/ sdk/ tests/

# 3. Check linting with flake8
flake8 akavesdk/ private/ sdk/ tests/ --count --max-complexity=10 --max-line-length=120
```

### Running Tests

```bash
pytest tests/unit -v

pytest tests/unit -v --cov=akavesdk --cov=private --cov=sdk
```

### Quick Pre-Push Checklist

```bash
# One-liner to format, sort, and check
black akavesdk/ private/ sdk/ tests/ && \
isort akavesdk/ private/ sdk/ tests/ && \
flake8 akavesdk/ private/ sdk/ tests/ --max-complexity=10 --max-line-length=120 && \
pytest tests/unit -v
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
