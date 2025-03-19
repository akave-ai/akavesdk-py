# akavesdk-py

Python version of the Akavesdk - A comprehensive Python SDK for interacting with the Akave decentralized storage system.

## Overview

The akavesdk-py provides a robust Python interface for interacting with Akave's decentralized storage system. It combines blockchain technology (Ethereum), IPFS, and advanced features like erasure coding and encryption to provide a secure and reliable storage solution.

## Features

### Core Functionality
- **Bucket Management**
  - Create, view, list, and delete buckets
  - Minimum bucket name length: 3 characters
  - Bucket metadata tracking with timestamps

- **File Operations**
  - Streaming file uploads and downloads
  - Block-based file handling
  - Support for large file operations
  - Minimum file size: 127 bytes

- **Security**
  - AES-GCM encryption support
  - 32-byte encryption keys
  - 16-byte AES-GCM tag
  - 12-byte nonce
  - Optional encryption configuration

### Advanced Features
- **Erasure Coding**
  - Configurable parity blocks
  - Data redundancy and recovery
  - Customizable block configuration

- **IPFS Integration**
  - Direct IPFS node interaction
  - CID-based file addressing
  - File addition and retrieval capabilities

- **Ethereum Integration**
  - Smart contract interaction
  - POA chain support
  - Custom gas configuration
  - Transaction management

## Installation

```bash
pip install akavesdk-py
```

## Requirements

### Core Dependencies
- Python 3.x
- grpcio
- ipfshttpclient
- web3
- protobuf
- multiformats-cid

### Optional Dependencies
- eth-account (for Ethereum operations)
- google-protobuf

## Usage

### Basic SDK Initialization

```python
from sdk.sdk import SDK

# Initialize the SDK with basic configuration
sdk = SDK(
    address="localhost:50051",
    max_concurrency=5,
    block_part_size=1024,
    use_connection_pool=False
)
```

### Advanced SDK Initialization with Encryption and Erasure Coding

```python
# Initialize with all available options
sdk = SDK(
    address="localhost:50051",
    max_concurrency=5,
    block_part_size=1024,
    use_connection_pool=False,
    encryption_key=b"0123456789abcdef0123456789abcdef",  # 32-byte key
    private_key="your_ethereum_private_key",
    streaming_max_blocks_in_chunk=32,
    parity_blocks_count=2
)
```

### Bucket Operations

```python
# Create a bucket
bucket = sdk.create_bucket("my-bucket")

# View bucket details
bucket_info = sdk.view_bucket("my-bucket")
print(f"Bucket: {bucket_info.name}, Created: {bucket_info.created_at}")

# Delete bucket
sdk.delete_bucket("my-bucket")
```

### IPFS Operations

```python
from sdk.dag import IPFSClient

# Initialize IPFS client
ipfs = IPFSClient(host='localhost', port=5001)

# Add file to IPFS
cid = ipfs.add_file(file_data)

# Retrieve file from IPFS
content = ipfs.get_file(cid)
```

### Streaming API Usage

```python
# Get streaming API instance
streaming = sdk.streaming_api()

# The streaming API supports:
# - Chunked file uploads
# - Block-based operations
# - Erasure coding for data reliability
# - Encrypted data transfer (when encryption is configured)
```

### IPC (Inter-Process Communication) Usage

```python
# Get IPC instance
ipc = sdk.ipc()

# IPC provides:
# - Ethereum contract interaction
# - Bucket management through smart contracts
# - Transaction handling and monitoring
```

## Technical Specifications

### Storage Parameters
- Block Size: 1MB
- Minimum File Size: 127 bytes
- Encryption Overhead: 28 bytes
  - 16 bytes for AES-GCM tag
  - 12 bytes for nonce

### Network Configuration
- Supports gRPC communication
- Optional connection pooling
- Configurable concurrency limits

### Smart Contract Integration
- Supports POA (Proof of Authority) chains
- Automated gas price management
- Transaction receipt monitoring
- Storage and Access Manager contract support

## Error Handling

The SDK provides comprehensive error handling through the `SDKError` class. Common errors include:
- Invalid bucket names
- Invalid block sizes
- Encryption key length mismatches
- Parity block configuration errors
- Network communication errors

## Development

### Running Tests

```bash
python -m unittest tests/test_sdk.py
```

### Protocol Buffers

The SDK uses Protocol Buffers for API definitions. The proto files are located in:
- `private/pb/nodeapi.proto`
- `private/pb/ipcnodeapi.proto`


## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For support and questions, please open an issue in the GitHub repository.
