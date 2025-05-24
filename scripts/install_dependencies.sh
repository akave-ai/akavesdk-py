#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Adding Core dependencies..."
poetry add grpcio=">=1.60.0"
poetry add grpcio-tools=">=1.60.0"
poetry add protobuf=">=5.26.1,<6.0dev"
poetry add uuid="==1.30"

echo "Adding IPFS & MerkleDAG dependencies..."
poetry add ipfshttpclient="==0.8.0a2"
poetry add cid="==0.1.3"
poetry add multiformats="==0.3.1.post4"

echo "Adding Ethereum-related dependencies (revised order)..."
# Install pycryptodome first as it's a specific version and a dependency for eth-hash extra
poetry add pycryptodome="==3.20.0"

# Install eth-utils - web3 requires eth-utils>=2.1.0,<3.0.0
# Your original eth-utils>=2.1.0 is compatible.
poetry add eth-utils=">=2.1.0"

# Install a specific, recent, and Py3.11-compatible version of eth-hash.
# web3==6.11.3 requires eth-hash[pycryptodome]>=0.5.1. Version 0.7.1 meets this and your script's original >=0.5.2.
poetry add eth-hash="==0.7.1"

# Install eth-account - web3==6.11.3 requires eth-account>=0.10.0,<0.13.0.
# This satisfies your original >=0.8.0.
poetry add eth-account=">=0.10.0"

# eth-keys and rlp (if needed directly by your project or other dependencies)
poetry add eth-keys=">=0.4.0"
poetry add rlp=">=2.0.1"

# Now add web3, which should find its necessary eth-* dependencies already present and compatible.
poetry add web3="==6.11.3"

echo "Adding Logging & monitoring dependencies..."
poetry add opentelemetry-api="==1.28.0"
poetry add opentelemetry-sdk="==1.28.0"
poetry add opentelemetry-exporter-jaeger="==1.21.0"
poetry add opentelemetry-instrumentation-grpc="==0.49b0"
poetry add prometheus-client="==0.19.0"
poetry add structlog="==24.1.0"

echo "Adding Error handling & utilities..."
# pydantic is also a dependency of web3 (>=1.10.0,<3.0.0).
# Your 2.6.3 is compatible. Add it; Poetry will manage the correct version.
poetry add pydantic="==2.6.3"
poetry add rich="==13.7.0"

echo "Adding Cobra CLI equivalent..."
poetry add click="==8.1.7"

echo "Adding File utilities..."
poetry add watchdog="==3.0.0"

echo "Adding YAML parsing..."
poetry add PyYAML="==6.0.1"

echo "Adding math dependency..."
poetry add cryptography="==36.0.0" # Note: pycryptodome is different from cryptography

echo "Adding erasure dependencies..."
poetry add reedsolo
poetry add numpy

echo "Adding Testing dependencies to dev group..."
poetry add pytest=">=7.4.0" --group dev
poetry add pytest-mock=">=3.12.0" --group dev

echo "All dependencies added successfully!"