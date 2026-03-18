# PDP Test Utilities

This module provides utilities for Proof of Data Possession (PDP) tests with Filecoin.

## Constants

- `CALIBRATION_WARM_STORAGE_CONTRACT`: Warm storage contract address on Filecoin calibration network
- `CALIBRATION_FILECOIN_RPC`: RPC URL for Filecoin calibration network

## Functions

### `pick_private_key() -> str`
Returns the PDP private key from the `PDP_PRIVATE_KEY` environment variable. Skips the test if not provided.

### `pick_server_url() -> str`
Returns the PDP server URL from the `PDP_SERVER_URL` environment variable. Skips the test if not provided.

### `calculate_piece_cid(data: bytes) -> str`
Calculates the Filecoin piece CID (CommP) from data.

**Important**: This function requires the Go toolchain and Filecoin libraries to be installed.

## Setup for Piece CID Calculation

Since Python doesn't have native Filecoin piece commitment libraries, this implementation uses Go via subprocess. To use it:

1. **Install Go**: Download from https://go.dev/

2. **Install required Go modules**:
   ```bash
   go get github.com/filecoin-project/go-fil-commcid
   go get github.com/filecoin-project/go-fil-commp-hashhash
   ```

## Alternative Approaches

If the Go-based approach is not suitable for your use case, consider:

1. **Use the Go SDK directly** for PDP operations instead of Python
2. **Create a Go microservice** that exposes piece CID calculation via HTTP/gRPC
3. **Use FFI bindings** to call Go code from Python (e.g., using `ctypes` with a compiled Go shared library)

## Example Usage

```python
import os
from private.pdptest import calculate_piece_cid, pick_private_key

# Set environment variables
os.environ['PDP_PRIVATE_KEY'] = 'your-private-key'

# Calculate piece CID
data = b"your data here"
piece_cid = calculate_piece_cid(data)
print(f"Piece CID: {piece_cid}")

# In tests
def test_something():
    private_key = pick_private_key()  # Will skip if not set
    # ... rest of test
```
