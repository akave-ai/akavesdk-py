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
from akavesdk import SDK

# Initialize with explicit parameters
sdk = SDK(
    address="connect.akave.ai:5000",  # Akave endpoint for streaming operations
    max_concurrency=10,
    block_part_size=1 * 1024 * 1024,  # 1MB
    use_connection_pool=True,
    private_key="your_ethereum_private_key",  # Required for IPC API operations
    encryption_key=b"your_32_byte_encryption_key",  # Optional, for file encryption
    ipc_address="connect.akave.ai:5500"  # Akave endpoint for IPC operations (optional)
)
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
from akavesdk import SDK

# Initialize the SDK with a private key
sdk = SDK(
    address="connect.akave.ai:5500",
    max_concurrency=10,
    block_part_size=1 * 1024 * 1024,  # 1MB
    use_connection_pool=True,
    private_key=os.environ.get("AKAVE_PRIVATE_KEY")  # Required for IPC operations
)

try:
    # Get IPC API interface
    ipc = sdk.ipc()
    
    # Create a bucket
    bucket_result = ipc.create_bucket({}, "my-bucket")
    print(f"Created bucket: {bucket_result.name}")
    
    # List buckets
    buckets = ipc.list_buckets({})
    for bucket in buckets:
        print(f"Bucket: {bucket.name}, Created: {bucket.created_at}")
    
    # Upload a file (minimum file size is 127 bytes, max recommended test size: 100MB)
    with open("my-file.txt", "rb") as f:
        ipc.create_file_upload({}, "my-bucket", "my-file.txt")
        file_meta = ipc.upload({}, "my-bucket", "my-file.txt", f)
        print(f"Uploaded file: {file_meta.name}, Size: {file_meta.encoded_size} bytes")
    
    # Download a file
    with open("downloaded-file.txt", "wb") as f:
        download = ipc.create_file_download({}, "my-bucket", "my-file.txt")
        ipc.download({}, download, f)
        print(f"Downloaded file with {len(download.chunks)} chunks")
    
    # Delete a file
    ipc.file_delete({}, "my-bucket", "my-file.txt")
    print("File deleted successfully")
    
    # Delete a bucket
    ipc.delete_bucket({}, "my-bucket")
    print("Bucket deleted successfully")
finally:
    # Always close the connection when done
    sdk.close()
```

### Streaming API Usage

```python
from akavesdk import SDK

# Initialize the SDK
sdk = SDK(
    address="connect.akave.ai:5500",
    max_concurrency=10,
    block_part_size=1 * 1024 * 1024,  # 1MB
    use_connection_pool=True
)

try:
    # Get streaming API
    streaming = sdk.streaming_api()
    
    # List files in a bucket
    files = streaming.list_files({}, "my-bucket")
    for file in files:
        print(f"File: {file.name}, Size: {file.size} bytes")
    
    # Get file info
    file_info = streaming.file_info({}, "my-bucket", "my-file.txt")
    print(f"File info: {file_info}")
finally:
    sdk.close()
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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
