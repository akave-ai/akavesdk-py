# Akave Python SDK

The **Akave Python SDK** (`akavesdk-py`) is a powerful Python library for interacting with Akave's decentralized storage platform. It provides a comprehensive set of tools for managing data storage, file operations, and blockchain-based storage operations.

## Installation

```bash
pip install -r requirements.txt
```

## Core Features

### 1. Storage Management
- Create and manage storage buckets
- Upload and download files with streaming support
- Efficient handling of large files through chunking
- Concurrent operations for improved performance

### 2. Data Security
- Optional AES-GCM encryption
- 32-byte encryption key support
- Secure key derivation for different operations
- 28-byte encryption overhead (16 bytes AES-GCM tag, 12 bytes nonce)

### 3. Data Reliability
- Erasure coding for data redundancy
- Configurable parity blocks
- Automatic data recovery capabilities
- Chunk-based data management

### 4. Blockchain Integration
- IPC (Inter-Planetary Consensus) support
- Smart contract interactions
- Blockchain-based metadata storage
- Decentralized data verification

## Getting Started

### Initializing the SDK

```python
from akavesdk_py import SDK

sdk = SDK(
    address="your_node_address",
    max_concurrency=10,
    block_part_size=1048576,  # 1MB
    use_connection_pool=True,
    encryption_key=None,  # Optional: 32-byte key for encryption
    private_key=None,  # Optional: for IPC operations
    streaming_max_blocks_in_chunk=32,
    parity_blocks_count=0
)
```

## API Reference

### Storage Operations

#### Bucket Management
```python
# Create a new bucket
bucket_result = sdk.create_bucket("my_bucket")

# View bucket details
bucket = sdk.view_bucket("my_bucket")

# List all buckets
buckets = sdk.list_buckets()

# Delete a bucket
success = sdk.delete_bucket("my_bucket")
```

#### File Operations
```python
# Get streaming API instance
streaming = sdk.streaming_api()

# File upload operations
upload = streaming.create_file_upload("my_bucket", "file.txt")
upload.upload_file("path/to/file.txt")

# File download operations
download = streaming.create_file_download("my_bucket", "file.txt")
download.download_file("path/to/save.txt")

# List files in a bucket
files = streaming.list_files("my_bucket")

# Get file information
file_info = streaming.file_info("my_bucket", "file.txt")
```

#### IPC Operations
```python
# Get IPC API instance
ipc = sdk.ipc()

# Interact with blockchain storage
ipc_bucket = ipc.create_bucket("my_bucket")
ipc_files = ipc.list_files("my_bucket")
```

## Advanced Usage

### Encryption Configuration
```python
import os

# Generate a secure encryption key
encryption_key = os.urandom(32)

# Initialize SDK with encryption
sdk = SDK(
    address="your_node_address",
    encryption_key=encryption_key,
    # ... other parameters
)
```

### Erasure Coding Setup
```python
# Initialize SDK with erasure coding
sdk = SDK(
    address="your_node_address",
    streaming_max_blocks_in_chunk=32,
    parity_blocks_count=4,  # Creates 4 parity blocks for recovery
    # ... other parameters
)
```

### Concurrent Operations
```python
# Configure high concurrency for better performance
sdk = SDK(
    address="your_node_address",
    max_concurrency=20,
    use_connection_pool=True,
    # ... other parameters
)
```

## Best Practices

### 1. Resource Management
```python
# Use context manager for automatic cleanup
with SDK(...) as sdk:
    sdk.create_bucket("my_bucket")
    # ... perform operations
# Connection automatically closed
```

### 2. Error Handling
```python
from akavesdk_py import SDKError

try:
    sdk.create_bucket("my_bucket")
except SDKError as e:
    logging.error(f"Operation failed: {e}")
```

### 3. Performance Optimization
- Use appropriate block sizes for your use case
- Configure concurrency based on system resources
- Enable connection pooling for multiple operations
- Consider chunk size for large file operations

## Technical Specifications

### Storage Limits
- Minimum bucket name length: 3 characters
- Minimum file size: 127 bytes
- Maximum block size: 1MB
- Maximum chunk size: 32MB
- Maximum blocks per chunk: 32

### Performance Considerations
- Block size affects upload/download speed
- Concurrency impacts system resource usage
- Connection pooling improves multiple operation performance
- Erasure coding adds overhead but improves reliability

## Contributing

We welcome contributions to improve the SDK! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the terms specified in the LICENSE file.

## Support

For issues, questions, or contributions:
1. Check existing issues in the repository
2. Create a new issue with detailed information
3. Follow the contribution guidelines
