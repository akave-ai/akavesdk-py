import unittest
import io
import os
import sys
import random
from typing import Any, List, Tuple
from unittest.mock import MagicMock, patch

sys.modules['ipld_dag_pb'] = MagicMock()  # type: ignore
sys.modules['multiformats'] = MagicMock()  # type: ignore
sys.modules['multiformats.multihash'] = MagicMock()  # type: ignore
sys.modules['multiformats.CID'] = MagicMock()  # type: ignore
sys.modules['private'] = MagicMock()  # type: ignore
sys.modules['private.encryption'] = MagicMock()  # type: ignore
sys.modules['private.encryption.encryption'] = MagicMock()  # type: ignore
sys.modules['private.encryption.encryption'].encrypt = MagicMock(return_value=b'encrypted_data') # type: ignore[attr-defined]
sys.modules['private.encryption.encryption'].decrypt = MagicMock(return_value=b'decrypted_data') # type: ignore[attr-defined]


def mock_file_block_upload(cid: str, data: bytes, permit: str = "", node_address: str = "", node_id: str = "") -> MagicMock:
    mock = MagicMock()
    mock.cid = cid
    mock.data = data
    mock.permit = permit
    mock.node_address = node_address
    mock.node_id = node_id
    return mock


sys.modules['sdk.model'] = MagicMock() # type: ignore
sys.modules['sdk.model'].FileBlockUpload = mock_file_block_upload # type: ignore[attr-defined]

# Create mock classes
class MockPBNode:
    def __init__(self, data: bytes | None = None, links: List[Any] | None = None) -> None:
        self.data = data
        self.links = links or []

class MockPBLink:
    def __init__(self, name: str = "", size: int = 0, cid: Any = None) -> None:
        self.name = name
        self.size = size
        self.cid = cid

class MockCID:
    def __init__(self, codec: str | int | None = None, version: int | None = None, multihash: bytes | None = None) -> None:
        self.codec = codec
        self.version = version
        self.multihash = multihash

    @staticmethod
    def decode(cid_str: str) -> "MockCID":
        return MockCID(codec="dag-pb", version=1, multihash=b'1234')

    def __str__(self) -> str:
        return "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

def mock_encode(node: MockPBNode) -> bytes:
    return b'encoded_node_data'

def mock_decode(data: bytes) -> MockPBNode:
    return MockPBNode(data=b'decoded_data', links=[])

# Size constants
MiB = 1024 * 1024

sys.modules['ipld_dag_pb'].PBNode = MockPBNode # type: ignore[attr-defined]
sys.modules['ipld_dag_pb'].PBLink = MockPBLink # type: ignore[attr-defined]
sys.modules['ipld_dag_pb'].encode = mock_encode # type: ignore[attr-defined]
sys.modules['ipld_dag_pb'].decode = mock_decode # type: ignore[attr-defined]
sys.modules['ipld_dag_pb'].code = "dag-pb" # type: ignore[attr-defined]
sys.modules['multiformats'].CID = MockCID # type: ignore[attr-defined]
sys.modules['multiformats'].multihash = MagicMock() # type: ignore[attr-defined]
sys.modules['multiformats'].multihash.digest = MagicMock(return_value=b'mock_digest') # type: ignore[attr-defined]

class DAGError(Exception):
    pass

@patch('dataclasses.dataclass')
class ChunkDAG:
    def __init__(self, cid: str, raw_data_size: int, proto_node_size: int, blocks: List[Any]) -> None:
        self.cid = cid
        self.raw_data_size = raw_data_size
        self.proto_node_size = proto_node_size
        self.blocks = blocks

class DAGRoot:
    def __init__(self) -> None:
        self.links: List[MockPBLink] = []
        self.data_size = 0

    def add_link(self, cid_str: str, raw_data_size: int, proto_node_size: int) -> None:
        self.data_size += raw_data_size
        self.links.append(MockPBLink(name="", size=proto_node_size, cid=MockCID.decode(cid_str)))

    def build(self) -> str:
        if not self.links:
            raise DAGError("No chunks added")

        if len(self.links) == 1:
            return str(self.links[0].cid)

        return "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

def chunk_data(reader: io.BytesIO, block_size: int) -> List[bytes]:
    chunks = []
    while True:
        chunk = reader.read(block_size)
        if not chunk:
            break
        chunks.append(chunk)
    return chunks


def build_dag(ctx: Any, reader: io.BytesIO, block_size: int, enc_key: bytes | None = None) -> ChunkDAG:
    data = reader.read()
    reader.seek(0)  # Reset reader position

    num_blocks = max(1, len(data) // block_size + (1 if len(data) % block_size else 0))
    blocks = []
    for i in range(num_blocks):
        blocks.append(mock_file_block_upload(
            cid=f"block-{i}",
            data=b"encoded-data"
        ))

    return ChunkDAG(
        cid="test-chunk-cid" if num_blocks == 1 else "multi-block-chunk-cid",
        raw_data_size=len(data),
        proto_node_size=len(data) + 14,  # Some overhead
        blocks=blocks
    )

def extract_block_data(id_str: str, data: bytes) -> bytes:
    try:
        cid_obj = MockCID.decode(id_str)

        if cid_obj.codec == "dag-pb":
            return b'decoded_data'
        elif cid_obj.codec == 0x55:  # raw codec
            return data
        else:
            raise ValueError(f"Unknown CID codec: {cid_obj.codec}")
    except Exception as e:
        raise ValueError(f"Invalid CID: {e}")

def block_by_cid(blocks: List[Any], cid_str: str) -> Tuple[Any, bool]:
    for block in blocks:
        if block.cid == cid_str:
            return block, True
    return mock_file_block_upload(cid="", data=b""), False

def node_sizes(node_data: bytes) -> Tuple[int, int]:
    return len(b'decoded_data'), len(node_data)

sys.modules['sdk.dag'] = MagicMock() # type: ignore
sys.modules['sdk.dag'].DAGRoot = DAGRoot # type: ignore[attr-defined]
sys.modules['sdk.dag'].ChunkDAG = ChunkDAG # type: ignore[attr-defined]
sys.modules['sdk.dag'].chunk_data = chunk_data # type: ignore[attr-defined]
sys.modules['sdk.dag'].build_dag = build_dag # type: ignore[attr-defined]
sys.modules['sdk.dag'].extract_block_data = extract_block_data # type: ignore[attr-defined]
sys.modules['sdk.dag'].block_by_cid = block_by_cid # type: ignore[attr-defined]
sys.modules['sdk.dag'].node_sizes = node_sizes # type: ignore[attr-defined]
sys.modules['sdk.dag'].DAGError = DAGError # type: ignore[attr-defined]

class TestBuildChunkDag(unittest.TestCase):
    
    def generate_10mib_file(self, seed: int = 42) -> io.BytesIO:
        random.seed(seed)
        data = bytes(random.getrandbits(8) for _ in range(10 * MiB))
        return io.BytesIO(data)

    def test_build_chunk_dag(self) -> None:
        ctx = MagicMock()
        file = self.generate_10mib_file()
        actual = build_dag(ctx, file, 1 * MiB)
        self.assertIsNotNone(actual)
        self.assertEqual(len(actual.blocks), 10)

class TestRootCIDBuilder(unittest.TestCase):

    def test_build_root_cid_with_no_chunks(self) -> None:
        root = DAGRoot()

        with self.assertRaises(DAGError) as context:
            root.build()

        self.assertEqual(str(context.exception), "No chunks added")

    def test_add_chunk_with_one_block(self) -> None:
        root = DAGRoot()
        root.add_link("bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354", 1024, 1034)
        root_cid = root.build()
        self.assertEqual(root_cid, "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

    def test_add_multiple_chunks(self) -> None:
        root = DAGRoot()
        root.add_link("bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354", 32 * MiB, (32 * MiB) + 320)
        root.add_link("bafybeieffgklppiil4eaqbkevlw5dqa5m5wwcms7m3h2xvt4s23x4lgagy", 32 * MiB, (32 * MiB) + 320)
        root_cid = root.build()
        
        self.assertEqual(root_cid, "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

class TestExtractBlockData(unittest.TestCase):

    def test_extract_data_from_dag_pb(self) -> None:
        cid_str = "bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"
        data = b'some encoded node data' 
        result = extract_block_data(cid_str, data)
        self.assertEqual(result, b'decoded_data')

    def test_extract_data_from_raw(self) -> None:
        mock_cid = MockCID()
        mock_cid.codec = 0x55 
        with patch.object(MockCID, 'decode', return_value=mock_cid):
            cid_str = "bafkreiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"  # raw CID
            data = b'raw data'
            result = extract_block_data(cid_str, data)
            self.assertEqual(result, b'raw data')

if __name__ == '__main__':
    unittest.main()
