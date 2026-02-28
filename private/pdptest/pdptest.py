# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import os
import pytest

CALIBRATION_WARM_STORAGE_CONTRACT = "0x02925630df557F957f70E112bA06e50965417CA0"
CALIBRATION_FILECOIN_RPC = "https://api.calibration.node.glif.io/rpc/v1"

PDP_PRIVATE_KEY = os.getenv("PDP_PRIVATE_KEY", "")
PDP_SERVER_URL = os.getenv("PDP_SERVER_URL", "")


def pick_private_key() -> str:
    if not PDP_PRIVATE_KEY or PDP_PRIVATE_KEY.lower() == "omit":
        pytest.skip("private key flag missing, example: -PDP_PRIVATE_KEY=<deployers hex private key>")
    return PDP_PRIVATE_KEY


def pick_server_url() -> str:
    if not PDP_SERVER_URL:
        pytest.skip("PDP server URL flag missing, example: -pdp-server-url=<pdp server url>")
    return PDP_SERVER_URL

#temporary solution to calculate piece CID
def calculate_piece_cid(data: bytes) -> str:
    try:
        import subprocess
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(data)
            temp_path = f.name
        
        try:
            result = subprocess.run(
                ['go', 'run', '-', temp_path],
                input='''
package main
import (
    "fmt"
    "io/ioutil"
    "os"
    commcid "github.com/filecoin-project/go-fil-commcid"
    commp "github.com/filecoin-project/go-fil-commp-hashhash"
)
func main() {
    data, err := ioutil.ReadFile(os.Args[1])
    if err != nil {
        panic(err)
    }
    cp := new(commp.Calc)
    cp.Write(data)
    rawCommP, _, err := cp.Digest()
    if err != nil {
        panic(err)
    }
    pieceCid, err := commcid.DataCommitmentToPieceCidv2(rawCommP, uint64(len(data)))
    if err != nil {
        panic(err)
    }
    fmt.Print(pieceCid.String())
}
''',
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"Failed to calculate piece CID: {result.stderr}")
            
            return result.stdout.strip()
        finally:
            os.unlink(temp_path)
            
    except FileNotFoundError:
        raise RuntimeError(
            "Go toolchain not found. To calculate piece CIDs, you need:\n"
            "1. Install Go from https://go.dev/\n"
            "2. Run: go get github.com/filecoin-project/go-fil-commcid\n"
            "3. Run: go get github.com/filecoin-project/go-fil-commp-hashhash\n"
            "Alternative: Use the Go SDK directly for PDP operations."
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Piece CID calculation timed out")
    except Exception as e:
        raise RuntimeError(f"Failed to calculate piece CID: {e}")
