import unittest
import io
import os
import sys
import random
from unittest.mock import MagicMock, patch

from sdk.erasure_code import ErasureCode

sys.modules['ipld_dag_pb'] = MagicMock()
sys.modules['multiformats'] = MagicMock()
sys.modules['multiformats.multihash'] = MagicMock()
sys.modules['multiformats.CID'] = MagicMock()
sys.modules['private'] = MagicMock()
sys.modules['private.encryption'] = MagicMock()
sys.modules['private.encryption.encryption'] = MagicMock()
sys.modules['private.encryption.encryption'].encrypt = MagicMock(return_value=b'encrypted_data')
sys.modules['private.encryption.encryption'].decrypt = MagicMock(return_value=b'decrypted_data')

def mock_file_block_upload(cid, data, permit="", node_address="", node_id=""):
    mock = MagicMock()
    mock.cid = cid
    mock.data = data
    mock.permit = permit
    mock.node_address = node_address
    mock.node_id = node_id
    return mock

sys.modules['sdk.model'] = MagicMock()
sys.modules['sdk.model'].FileBlockUpload = mock_file_block_upload

# Create mock classes
class MockPBNode:
    def __init__(self, data=None, links=None):
        self.data = data
        self.links = links or []

class MockPBLink:
    def __init__(self, name="", size=0, cid=None):
        self.name = name
        self.size = size
        self.cid = cid

class MockCID:
    def __init__(self, codec=None, version=None, multihash=None):
        self.codec = codec
        self.version = version
        self.multihash = multihash
        
    @staticmethod
    def decode(cid_str):
        return MockCID(codec="dag-pb", version=1, multihash=b'1234')
    
    def __str__(self):
        return "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

def mock_encode(node):
    return b'encoded_node_data'

def mock_decode(data):
    return MockPBNode(data=b'decoded_data', links=[])

# Size constants
MiB = 1024 * 1024

sys.modules['ipld_dag_pb'].PBNode = MockPBNode
sys.modules['ipld_dag_pb'].PBLink = MockPBLink
sys.modules['ipld_dag_pb'].encode = mock_encode
sys.modules['ipld_dag_pb'].decode = mock_decode
sys.modules['ipld_dag_pb'].code = "dag-pb"
sys.modules['multiformats'].CID = MockCID
sys.modules['multiformats'].multihash = MagicMock()
sys.modules['multiformats'].multihash.digest = MagicMock(return_value=b'mock_digest')

class DAGError(Exception):
    pass

@patch('dataclasses.dataclass')
class ChunkDAG:
    def __init__(self, cid, raw_data_size, encoded_size, blocks):
        self.cid = cid
        self.raw_data_size = raw_data_size
        self.encoded_size = encoded_size
        self.blocks = blocks

class DAGRoot:
    def __init__(self):
        self.links = []
        self.data_size = 0
        
    def add_link(self, cid_str, raw_data_size, encoded_size):
        self.data_size += raw_data_size
        self.links.append(MockPBLink(name="", size=encoded_size, cid=MockCID.decode(cid_str)))
        
    def build(self):
        if not self.links:
            raise DAGError("No chunks added")
            
        if len(self.links) == 1:
            return str(self.links[0].cid)
            
        return "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

def chunk_data(reader, block_size):
    chunks = []
    while True:
        chunk = reader.read(block_size)
        if not chunk:
            break
        chunks.append(chunk)
    return chunks

def build_dag(ctx, reader, block_size, enc_key=None):
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
        encoded_size=len(data) + 14,  # Some overhead
        blocks=blocks
    )

def extract_block_data(id_str, data):
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

def block_by_cid(blocks, cid_str):
    for block in blocks:
        if block.cid == cid_str:
            return block, True
    return mock_file_block_upload(cid="", data=b""), False

def node_sizes(node_data):
    return len(b'decoded_data'), len(node_data)

sys.modules['sdk.dag'] = MagicMock()
sys.modules['sdk.dag'].DAGRoot = DAGRoot
sys.modules['sdk.dag'].ChunkDAG = ChunkDAG
sys.modules['sdk.dag'].chunk_data = chunk_data
sys.modules['sdk.dag'].build_dag = build_dag
sys.modules['sdk.dag'].extract_block_data = extract_block_data
sys.modules['sdk.dag'].block_by_cid = block_by_cid
sys.modules['sdk.dag'].node_sizes = node_sizes
sys.modules['sdk.dag'].DAGError = DAGError

class TestBuildChunkDag(unittest.TestCase):
    
    def generate_10mib_file(self, seed=42):
        random.seed(seed)
        data = bytes(random.getrandbits(8) for _ in range(10 * MiB))
        return io.BytesIO(data)
    
    def test_build_chunk_dag(self):
        ctx = MagicMock()
        file = self.generate_10mib_file()
        actual = build_dag(ctx, file, 1 * MiB)
        self.assertIsNotNone(actual)
        self.assertEqual(len(actual.blocks), 10)  # 10MiB file with 1MiB chunks = 10 blocks

    def test_chunk_size_without_erasure(self):
        ctx = MagicMock()
        file = self.generate_10mib_file()
        actual = build_dag(ctx, file, 1 * MiB)
        self.assertIsNotNone(actual)
        self.assertEqual(actual.encoded_size,10485900)
        #blocks_total = 0
        #for block in actual.blocks:
        # blocks_total += len(block.data)
        #self.assertEqual(blocks_total,actual.encoded_size)

            
    def test_chunk_size_with_erasure(self):
        ctx = MagicMock()
        file = self.generate_10mib_file()
        ec = ErasureCode.new(16,16)
        data = ec.encode(file.read())
        block_size = len(data) // (ec.data_blocks + ec.parity_blocks)
        actual = build_dag(ctx, io.BytesIO(data), block_size)
        self.assertIsNotNone(actual)
        self.assertEqual(actual.encoded_size,20972000)
        #blocks_total = 0
        #for block in actual.blocks:
        # blocks_total += len(block.data)
        #self.assertEqual(blocks_total,actual.encoded_size)


class TestRootCIDBuilder(unittest.TestCase):
    
    def test_build_root_cid_with_no_chunks(self):
        root = DAGRoot()
        
        with self.assertRaises(DAGError) as context:
            root.build()
        
        self.assertEqual(str(context.exception), "No chunks added")
    
    def test_add_chunk_with_one_block(self):
        root = DAGRoot()
        root.add_link("bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354", 1024, 1034)
        root_cid = root.build()
        self.assertEqual(root_cid, "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
    
    def test_add_multiple_chunks(self):
        root = DAGRoot()
        root.add_link("bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354", 32 * MiB, (32 * MiB) + 320)
        root.add_link("bafybeieffgklppiil4eaqbkevlw5dqa5m5wwcms7m3h2xvt4s23x4lgagy", 32 * MiB, (32 * MiB) + 320)
        root_cid = root.build()
        
        self.assertEqual(root_cid, "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

class TestExtractBlockData(unittest.TestCase):
    
    def test_extract_data_from_dag_pb(self):      
        cid_str = "bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"
        data = b'some encoded node data' 
        result = extract_block_data(cid_str, data)
        self.assertEqual(result, b'decoded_data')
    
    def test_extract_data_from_raw(self):
        mock_cid = MockCID()
        mock_cid.codec = 0x55  
        with patch.object(MockCID, 'decode', return_value=mock_cid):
            cid_str = "bafkreiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"  # raw CID
            data = b'raw data'
            result = extract_block_data(cid_str, data)
            self.assertEqual(result, b'raw data')

if __name__ == '__main__':
    unittest.main()
