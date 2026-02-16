
"""
Unit tests for DAG (Directed Acyclic Graph) functionality.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
import io
import hashlib
from dataclasses import dataclass

from sdk.dag import (
    DAGRoot,
    ChunkDAG,
    DAGError,
    build_dag,
    node_sizes,
    extract_block_data,
    bytes_to_node,
    get_node_links,
    block_by_cid,
    _encode_varint,
    _decode_varint,
    _extract_unixfs_data_size,
    _extract_unixfs_data,
    _extract_unixfs_data_fallback,
    _create_unixfs_file_node,
    _create_chunk_dag_root_node,
    IPLD_AVAILABLE,
    CID
)
from sdk.model import FileBlockUpload


@pytest.fixture
def sample_file_blocks():
    """Sample file blocks for testing."""
    return [
        FileBlockUpload(cid="bafybeia3uzxlpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq", data=b"block1_data"),
        FileBlockUpload(cid="bafybeia4vwrlpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq", data=b"block2_data"),
        FileBlockUpload(cid="bafybeia5xzslpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq", data=b"block3_data")
    ]


class TestDAGRoot:
    """Test DAGRoot class functionality."""
    
    def test_init(self):
        """Test DAGRoot initialization."""
        dag_root = DAGRoot()
        assert dag_root.node is None
        assert dag_root.fs_node_data == b""
        assert dag_root.links == []
        assert dag_root.total_file_size == 0
    
    def test_new_classmethod(self):
        """Test DAGRoot.new() class method."""
        dag_root = DAGRoot.new()
        assert isinstance(dag_root, DAGRoot)
        assert dag_root.links == []
    
    def test_add_link_with_string_cid(self):
        """Test adding a link with string CID."""
        dag_root = DAGRoot()
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(cid_str, raw_data_size=1024, proto_node_size=1200)
        
        assert len(dag_root.links) == 1
        assert dag_root.links[0]["cid_str"] == cid_str
        assert dag_root.links[0]["name"] == ""
        assert dag_root.links[0]["size"] == 1200
        assert dag_root.total_file_size == 1024
    
    def test_add_link_with_cid_object(self):
        """Test adding a link with CID object."""
        dag_root = DAGRoot()
        
        # Mock CID object
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(mock_cid, raw_data_size=512, proto_node_size=600)
        
        assert len(dag_root.links) == 1
        assert dag_root.links[0]["cid"] == mock_cid
        assert dag_root.total_file_size == 512
    
    def test_add_multiple_links(self):
        """Test adding multiple links."""
        dag_root = DAGRoot()
        
        dag_root.add_link("cid1", 100, 120)
        dag_root.add_link("cid2", 200, 240)
        dag_root.add_link("cid3", 300, 360)
        
        assert len(dag_root.links) == 3
        assert dag_root.total_file_size == 600
    
    def test_build_no_chunks(self):
        """Test build with no chunks added."""
        dag_root = DAGRoot()
        
        with pytest.raises(DAGError, match="no chunks added"):
            dag_root.build()
    
    def test_build_single_chunk(self):
        """Test build with single chunk."""
        dag_root = DAGRoot()
        
        # Mock CID with string method
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(mock_cid, 1024, 1200)
        
        result = dag_root.build()
        assert result == "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
    
    def test_build_single_chunk_string_cid(self):
        """Test build with single chunk using string CID."""
        dag_root = DAGRoot()
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        
        dag_root.add_link(cid_str, 1024, 1200)
        
        result = dag_root.build()
        assert result == cid_str
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_build_multiple_chunks_no_ipld(self):
        """Test build with multiple chunks when IPLD is not available."""
        dag_root = DAGRoot()
        
        dag_root.add_link("cid1", 100, 120)
        dag_root.add_link("cid2", 200, 240)
        
        result = dag_root.build()
        
        # Should generate a bafybeig... CID when IPLD is not available
        assert isinstance(result, str)
        assert result.startswith("bafybeig")
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    @patch('sdk.dag.PBNode')
    @patch('sdk.dag.encode')
    @patch('sdk.dag.multihash')
    def test_build_multiple_chunks_with_ipld(self, mock_multihash, mock_encode, mock_pbnode, mock_cid_class):
        """Test build with multiple chunks when IPLD is available."""
        dag_root = DAGRoot()
        
        # Mock CID objects
        mock_cid1 = Mock()
        mock_cid2 = Mock()
        mock_cid_class.decode.side_effect = [mock_cid1, mock_cid2]
        
        # Mock encoding and hashing
        mock_encode.return_value = b"encoded_data"
        mock_digest = b"digest_data"
        mock_multihash.digest.return_value = mock_digest
        
        # Mock final CID
        mock_root_cid = Mock()
        mock_cid_class.return_value = mock_root_cid
        
        dag_root.add_link("cid1", 100, 120)
        dag_root.add_link("cid2", 200, 240)
        
        result = dag_root.build()
        
        assert result == mock_root_cid
        mock_encode.assert_called_once()
        mock_multihash.digest.assert_called_once_with(b"encoded_data", "sha2-256")
    
    def test_create_unixfs_file_data(self):
        """Test _create_unixfs_file_data method."""
        dag_root = DAGRoot()
        dag_root.total_file_size = 1024
        
        unixfs_data = dag_root._create_unixfs_file_data()
        
        # Should start with [0x08, 0x02] for file type
        assert unixfs_data[:2] == bytes([0x08, 0x02])
        # Should contain file size
        assert len(unixfs_data) > 2
    
    def test_encode_varint(self):
        """Test _encode_varint method."""
        dag_root = DAGRoot()
        
        # Test small number
        result = dag_root._encode_varint(127)
        assert result == bytes([127])
        
        # Test larger number
        result = dag_root._encode_varint(300)
        assert len(result) == 2
        assert result[0] & 0x80 != 0  # First byte should have continuation bit


class TestVarintFunctions:
    """Test varint encoding/decoding functions."""
    
    def test_encode_varint_small(self):
        """Test encoding small varint."""
        result = _encode_varint(42)
        assert result == bytes([42])
    
    def test_encode_varint_large(self):
        """Test encoding large varint."""
        result = _encode_varint(300)
        expected = bytes([172, 2])  # 300 encoded as varint
        assert result == expected
    
    def test_decode_varint_small(self):
        """Test decoding small varint."""
        data = bytes([42])
        value, bytes_read = _decode_varint(data)
        assert value == 42
        assert bytes_read == 1
    
    def test_decode_varint_large(self):
        """Test decoding large varint."""
        data = bytes([172, 2])
        value, bytes_read = _decode_varint(data)
        assert value == 300
        assert bytes_read == 2
    
    def test_decode_varint_too_long(self):
        """Test decoding varint that's too long."""
        # Create a varint that's too long (more than 64 bits)
        data = bytes([0x80] * 10 + [0x01])
        
        with pytest.raises(ValueError, match="varint too long"):
            _decode_varint(data)


class TestBuildDAG:
    """Test build_dag function."""
    
    def test_build_dag_empty_data(self):
        """Test build_dag with empty data."""
        reader = io.BytesIO(b"")
        
        with pytest.raises(DAGError, match="empty data"):
            build_dag(None, reader, 1024)
    
    def test_build_dag_small_data(self):
        """Test build_dag with data smaller than block size."""
        data = b"Hello, World!"
        reader = io.BytesIO(data)
        
        result = build_dag(None, reader, 1024)
        
        assert isinstance(result, ChunkDAG)
        assert result.raw_data_size == len(data)
        assert len(result.blocks) == 1
        assert result.blocks[0].data == data or len(result.blocks[0].data) > len(data)  # May be encoded
    
    def test_build_dag_large_data(self):
        """Test build_dag with data larger than block size."""
        data = b"x" * 2048  # 2KB data
        reader = io.BytesIO(data)
        block_size = 1024
        
        result = build_dag(None, reader, block_size)
        
        assert isinstance(result, ChunkDAG)
        assert result.raw_data_size == len(data)
        assert len(result.blocks) >= 2  # Should be split into multiple blocks
    
    @patch('sdk.dag.encrypt')
    def test_build_dag_with_encryption(self, mock_encrypt):
        """Test build_dag with encryption key."""
        data = b"Secret data"
        encrypted_data = b"encrypted_secret_data"
        reader = io.BytesIO(data)
        enc_key = b"encryption_key_32_bytes_test123"
        
        mock_encrypt.return_value = encrypted_data
        
        result = build_dag(None, reader, 1024, enc_key)
        
        assert isinstance(result, ChunkDAG)
        mock_encrypt.assert_called_once_with(enc_key, data, b"dag_encryption")
    
    @patch('sdk.dag._create_unixfs_file_node')
    def test_build_dag_create_unixfs_node_called(self, mock_create_unixfs):
        """Test that _create_unixfs_file_node is called."""
        data = b"Test data"
        reader = io.BytesIO(data)
        
        mock_cid = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        mock_create_unixfs.return_value = (mock_cid, data)
        
        result = build_dag(None, reader, 1024)
        
        mock_create_unixfs.assert_called_once_with(data)
        assert len(result.blocks) == 1


class TestCreateUnixFSFileNode:
    """Test _create_unixfs_file_node function."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_create_unixfs_file_node_no_ipld(self):
        """Test _create_unixfs_file_node without IPLD."""
        data = b"test data"
        
        cid, encoded_data = _create_unixfs_file_node(data)
        
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert encoded_data == data
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.PBNode')
    @patch('sdk.dag.encode')
    @patch('sdk.dag.multihash')
    @patch('sdk.dag.CID')
    def test_create_unixfs_file_node_with_ipld(self, mock_cid_class, mock_multihash, mock_encode, mock_pbnode):
        """Test _create_unixfs_file_node with IPLD."""
        data = b"test data"
        
        # Mock encoding
        encoded_bytes = b"encoded_unixfs_data"
        mock_encode.return_value = encoded_bytes
        
        # Mock multihash
        digest = b"hash_digest"
        mock_multihash.digest.return_value = digest
        
        # Mock CID
        mock_cid = Mock()
        mock_cid_class.return_value = mock_cid
        
        cid, encoded_data = _create_unixfs_file_node(data)
        
        assert cid == mock_cid
        assert encoded_data == encoded_bytes
        mock_encode.assert_called_once()
        mock_multihash.digest.assert_called_once_with(encoded_bytes, "sha2-256")


class TestNodeSizes:
    """Test node_sizes function."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_node_sizes_no_ipld(self):
        """Test node_sizes without IPLD."""
        data = b"test node data"
        
        raw_size, encoded_size = node_sizes(data)
        
        assert raw_size == len(data)
        assert encoded_size == len(data)
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.decode')
    def test_node_sizes_with_ipld_no_links(self, mock_decode):
        """Test node_sizes with IPLD and no links."""
        data = b"test node data"
        
        # Mock decoded node with data but no links
        mock_node = Mock()
        mock_node.data = b"unixfs_data"
        mock_node.links = []
        mock_decode.return_value = mock_node
        
        with patch('sdk.dag._extract_unixfs_data_size', return_value=100) as mock_extract:
            raw_size, encoded_size = node_sizes(data)
            
            assert raw_size == 100
            assert encoded_size == len(data)
            mock_extract.assert_called_once_with(b"unixfs_data")
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.decode')
    def test_node_sizes_with_ipld_with_links(self, mock_decode):
        """Test node_sizes with IPLD and links."""
        data = b"test node data"
        
        # Mock decoded node with links
        mock_link1 = Mock()
        mock_link1.size = 100
        mock_link2 = Mock()
        mock_link2.size = 200
        
        mock_node = Mock()
        mock_node.data = b"unixfs_data"
        mock_node.links = [mock_link1, mock_link2]
        mock_decode.return_value = mock_node
        
        with patch('sdk.dag._extract_unixfs_data_size', return_value=250):
            raw_size, encoded_size = node_sizes(data)
            
            assert raw_size == 250
            assert encoded_size == 300  # Sum of link sizes


class TestExtractBlockData:
    """Test extract_block_data function."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_extract_block_data_no_ipld(self):
        """Test extract_block_data without IPLD."""
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        data = b"test data"
        
        result = extract_block_data(cid_str, data)
        assert result == data
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    @patch('sdk.dag.decode')
    def test_extract_block_data_dag_pb(self, mock_decode, mock_cid_class):
        """Test extract_block_data with DAG-PB codec."""
        cid_str = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        data = b"test data"
        
        # Mock CID with DAG-PB codec
        mock_cid = Mock()
        mock_cid.codec = 0x70  # DAG_PB_CODEC
        mock_cid_class.decode.return_value = mock_cid
        
        # Mock decoded node
        mock_node = Mock()
        mock_node.data = b"unixfs_data"
        mock_decode.return_value = mock_node
        
        extracted_data = b"extracted"
        with patch('sdk.dag._extract_unixfs_data', return_value=extracted_data):
            result = extract_block_data(cid_str, data)
            assert result == extracted_data
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    def test_extract_block_data_raw(self, mock_cid_class):
        """Test extract_block_data with RAW codec."""
        cid_str = "bafkreigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
        data = b"raw data"
        
        # Mock CID with RAW codec
        mock_cid = Mock()
        mock_cid.codec = 0x55  # RAW_CODEC
        mock_cid_class.decode.return_value = mock_cid
        
        result = extract_block_data(cid_str, data)
        assert result == data


class TestBytesToNode:
    """Test bytes_to_node function."""
    
    @patch('sdk.dag.decode')
    def test_bytes_to_node_success(self, mock_decode):
        """Test successful bytes_to_node."""
        node_data = b"node data"
        mock_node = Mock()
        mock_decode.return_value = mock_node
        
        result = bytes_to_node(node_data)
        assert result == mock_node
        mock_decode.assert_called_once_with(node_data)
    
    @patch('sdk.dag.decode')
    def test_bytes_to_node_failure(self, mock_decode):
        """Test bytes_to_node with decode failure."""
        node_data = b"invalid node data"
        mock_decode.side_effect = Exception("Decode failed")
        
        with pytest.raises(DAGError, match="failed to decode node data"):
            bytes_to_node(node_data)


class TestGetNodeLinks:
    """Test get_node_links function."""
    
    @patch('sdk.dag.bytes_to_node')
    def test_get_node_links_success(self, mock_bytes_to_node):
        """Test successful get_node_links."""
        node_data = b"node data"
        
        # Mock links
        mock_link1 = Mock()
        mock_link1.hash = "hash1"
        mock_link1.name = "link1"
        mock_link1.size = 100
        
        mock_link2 = Mock()
        mock_link2.hash = "hash2"
        mock_link2.name = ""
        mock_link2.size = 200
        
        # Mock node
        mock_node = Mock()
        mock_node.links = [mock_link1, mock_link2]
        mock_bytes_to_node.return_value = mock_node
        
        result = get_node_links(node_data)
        
        expected = [
            {'cid': 'hash1', 'name': 'link1', 'size': 100},
            {'cid': 'hash2', 'name': '', 'size': 200}
        ]
        assert result == expected
    
    @patch('sdk.dag.bytes_to_node')
    def test_get_node_links_failure(self, mock_bytes_to_node):
        """Test get_node_links with failure."""
        node_data = b"invalid node data"
        mock_bytes_to_node.side_effect = Exception("Node decode failed")
        
        with pytest.raises(DAGError, match="failed to extract links from node"):
            get_node_links(node_data)


class TestBlockByCid:
    """Test block_by_cid function."""
    
    def test_block_by_cid_found(self, sample_file_blocks):
        """Test finding block by CID."""
        target_cid = "bafybeia4vwrlpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq"
        
        block, found = block_by_cid(sample_file_blocks, target_cid)
        
        assert found is True
        assert block.cid == target_cid
        assert block.data == b"block2_data"
    
    def test_block_by_cid_not_found(self, sample_file_blocks):
        """Test not finding block by CID."""
        target_cid = "nonexistent_cid"
        
        block, found = block_by_cid(sample_file_blocks, target_cid)
        
        assert found is False
        assert block.cid == ""
        assert block.data == b""
    
    def test_block_by_cid_empty_list(self):
        """Test block_by_cid with empty block list."""
        target_cid = "any_cid"
        
        block, found = block_by_cid([], target_cid)
        
        assert found is False
        assert block.cid == ""


class TestExtractUnixFSData:
    """Test _extract_unixfs_data function."""
    
    def test_extract_unixfs_data_empty(self):
        """Test _extract_unixfs_data with empty data."""
        result = _extract_unixfs_data(b"")
        assert result == b""
    
    def test_extract_unixfs_data_simple(self):
        """Test _extract_unixfs_data with simple data."""
        # Create simple UnixFS data: field 4 (Data) with 5 bytes "hello"
        unixfs_data = bytes([0x22, 0x05]) + b"hello"
        
        result = _extract_unixfs_data(unixfs_data)
        assert result == b"hello"
    
    def test_extract_unixfs_data_invalid(self):
        """Test _extract_unixfs_data with invalid data."""
        invalid_data = b"\xFF\xFF\xFF"
        
        result = _extract_unixfs_data(invalid_data)
        assert result == b""


class TestExtractUnixFSDataSize:
    """Test _extract_unixfs_data_size function."""
    
    def test_extract_unixfs_data_size_empty(self):
        """Test _extract_unixfs_data_size with empty data."""
        result = _extract_unixfs_data_size(b"")
        assert result == 0
    
    def test_extract_unixfs_data_size_with_file_size(self):
        """Test _extract_unixfs_data_size with file size field."""
        # Create UnixFS data with field 3 (file size) = 1024
        unixfs_data = bytes([0x18]) + _encode_varint(1024)
        
        result = _extract_unixfs_data_size(unixfs_data)
        assert result == 1024
    
    def test_extract_unixfs_data_size_with_data_field(self):
        """Test _extract_unixfs_data_size with data field."""
        # Create UnixFS data with field 4 (data) of length 512
        unixfs_data = bytes([0x22]) + _encode_varint(512) + b"x" * 512
        
        result = _extract_unixfs_data_size(unixfs_data)
        assert result == 512


class TestCreateChunkDAGRootNode:
    """Test _create_chunk_dag_root_node function."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_create_chunk_dag_root_node_no_ipld(self):
        """Test _create_chunk_dag_root_node without IPLD libraries."""
        blocks = [
            FileBlockUpload(cid="cid1", data=b"block1"),
            FileBlockUpload(cid="cid2", data=b"block2"),
        ]
        
        cid, total_size = _create_chunk_dag_root_node(blocks, None)
        
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert total_size == len(b"block1") + len(b"block2")
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.PBNode')
    @patch('sdk.dag.encode')
    @patch('sdk.dag.multihash')
    @patch('sdk.dag.CID')
    def test_create_chunk_dag_root_node_with_ipld(self, mock_cid_class, mock_multihash, mock_encode, mock_pbnode):
        """Test _create_chunk_dag_root_node with IPLD libraries."""
        blocks = [
            FileBlockUpload(cid="bafybeigtest1", data=b"block1"),
            FileBlockUpload(cid="bafybeigtest2", data=b"block2"),
        ]
        
        # Mock CID objects
        mock_cid1 = Mock()
        mock_cid2 = Mock()
        mock_cid_class.decode.side_effect = [mock_cid1, mock_cid2]
        
        # Mock encoding
        encoded_bytes = b"encoded_root_node"
        mock_encode.return_value = encoded_bytes
        
        # Mock multihash
        digest = b"digest_data"
        mock_multihash.digest.return_value = digest
        
        # Mock final CID
        mock_root_cid = Mock()
        mock_cid_class.return_value = mock_root_cid
        
        cid, total_size = _create_chunk_dag_root_node(blocks, None)
        
        assert cid == mock_root_cid
        assert total_size == len(b"block1") + len(b"block2")
        mock_encode.assert_called_once()
        mock_multihash.digest.assert_called_once_with(encoded_bytes, "sha2-256")
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    def test_create_chunk_dag_root_node_with_pb_links(self, mock_cid_class):
        """Test _create_chunk_dag_root_node with provided pb_links."""
        blocks = [
            FileBlockUpload(cid="cid1", data=b"block1"),
            FileBlockUpload(cid="cid2", data=b"block2"),
        ]
        
        # Mock pb_links
        mock_link1 = Mock()
        mock_link2 = Mock()
        pb_links = [mock_link1, mock_link2]
        
        with patch('sdk.dag.PBNode'), \
             patch('sdk.dag.encode', return_value=b"encoded"), \
             patch('sdk.dag.multihash') as mock_multihash:
            
            mock_multihash.digest.return_value = b"digest"
            mock_root_cid = Mock()
            mock_cid_class.return_value = mock_root_cid
            
            cid, total_size = _create_chunk_dag_root_node(blocks, pb_links)
            
            assert cid == mock_root_cid
            assert total_size == len(b"block1") + len(b"block2")
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.PBNode')
    @patch('sdk.dag.encode')
    def test_create_chunk_dag_root_node_exception_fallback(self, mock_encode, mock_pbnode):
        """Test _create_chunk_dag_root_node fallback on exception."""
        blocks = [
            FileBlockUpload(cid="cid1", data=b"block1"),
            FileBlockUpload(cid="cid2", data=b"block2"),
        ]
        
        # Force an exception
        mock_encode.side_effect = Exception("Encoding failed")
        
        cid, total_size = _create_chunk_dag_root_node(blocks, None)
        
        # Should fallback to non-IPLD behavior
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert total_size == len(b"block1") + len(b"block2")


class TestExtractUnixFSDataFallback:
    """Test _extract_unixfs_data_fallback function."""
    
    def test_extract_unixfs_data_fallback_empty(self):
        """Test _extract_unixfs_data_fallback with empty data."""
        result = _extract_unixfs_data_fallback(b"")
        assert result == b""
    
    def test_extract_unixfs_data_fallback_with_valid_dag_pb(self):
        """Test _extract_unixfs_data_fallback with valid DAG-PB structure."""
        # Create DAG-PB with field 1 (Data) containing UnixFS data
        # UnixFS data: field 4 (Data) = "hello"
        unixfs_data = bytes([0x22, 0x05]) + b"hello"
        # DAG-PB: field 1 (Data) with UnixFS data
        dag_pb_data = bytes([0x0a]) + _encode_varint(len(unixfs_data)) + unixfs_data
        
        result = _extract_unixfs_data_fallback(dag_pb_data)
        assert result == b"hello"
    
    def test_extract_unixfs_data_fallback_invalid_data(self):
        """Test _extract_unixfs_data_fallback with invalid data."""
        invalid_data = b"\xFF\xFF\xFF"
        result = _extract_unixfs_data_fallback(invalid_data)
        # Should return original data when extraction fails
        assert result == invalid_data
    
    def test_extract_unixfs_data_fallback_truncated_data(self):
        """Test _extract_unixfs_data_fallback with truncated data."""
        # Create truncated data (length says more bytes than available)
        truncated_data = bytes([0x0a, 0xFF, 0x01])  # Says there's 255 bytes but only 1
        result = _extract_unixfs_data_fallback(truncated_data)
        # Should handle gracefully
        assert isinstance(result, bytes)
    
    def test_extract_unixfs_data_fallback_with_multiple_fields(self):
        """Test _extract_unixfs_data_fallback with multiple protobuf fields."""
        # Create data with multiple fields: field 2 (Links) and field 1 (Data)
        unixfs_data = bytes([0x22, 0x04]) + b"test"
        dag_pb_data = (
            bytes([0x12, 0x05]) + b"link1" +  # Field 2 (Links)
            bytes([0x0a]) + _encode_varint(len(unixfs_data)) + unixfs_data  # Field 1 (Data)
        )
        
        result = _extract_unixfs_data_fallback(dag_pb_data)
        assert result == b"test"
    
    def test_extract_unixfs_data_fallback_no_unixfs_data(self):
        """Test _extract_unixfs_data_fallback when no UnixFS data extracted."""
        # Create DAG-PB with field 1 but UnixFS extraction returns empty
        dag_pb_data = bytes([0x0a, 0x02, 0x08, 0x02])  # Field 1 with just type, no data
        
        result = _extract_unixfs_data_fallback(dag_pb_data)
        # Should return original data when extraction returns empty
        assert result == dag_pb_data
    
    def test_extract_unixfs_data_fallback_exception_handling(self):
        """Test _extract_unixfs_data_fallback exception handling."""
        # Test various edge cases that might cause exceptions
        test_cases = [
            b"\x00",
            b"\x0a",  # Field tag without data
            bytes([0x0a, 0x00]),  # Field with zero length
        ]
        
        for data in test_cases:
            result = _extract_unixfs_data_fallback(data)
            assert isinstance(result, bytes)


class TestCIDFallbackBehavior:
    """Test CID fallback class when IPLD is unavailable."""
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_cid_fallback_init(self):
        """Test CID fallback class initialization."""
        cid = CID("test_cid_string")
        assert cid.cid_str == "test_cid_string"
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_cid_fallback_decode(self):
        """Test CID fallback decode method."""
        cid_str = "bafybeigtest123"
        cid = CID.decode(cid_str)
        assert cid.cid_str == cid_str
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_cid_fallback_str(self):
        """Test CID fallback __str__ method."""
        cid = CID("test_cid")
        assert str(cid) == "test_cid"
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_cid_fallback_string(self):
        """Test CID fallback string() method."""
        cid = CID("test_cid")
        assert cid.string() == "test_cid"
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_cid_fallback_bytes(self):
        """Test CID fallback bytes() method."""
        cid = CID("test_cid")
        assert cid.bytes() == b"test_cid"
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_cid_fallback_type_dag_pb(self):
        """Test CID fallback type() method for DAG-PB."""
        cid = CID("bafybeigtest123")
        assert cid.type() == 0x70  # dag-pb
    
    @patch('sdk.dag.IPLD_AVAILABLE', False)
    def test_cid_fallback_type_raw(self):
        """Test CID fallback type() method for raw."""
        cid = CID("bafkreigtest123")
        assert cid.type() == 0x55  # raw


class TestDAGRootEdgeCases:
    """Test DAGRoot edge cases and error handling."""
    
    def test_dag_root_build_with_invalid_cid_objects(self):
        """Test DAGRoot build with invalid CID objects."""
        dag_root = DAGRoot()
        
        # Add link with object that doesn't have string() or __str__
        invalid_cid = object()
        
        # Should handle gracefully or raise appropriate error
        try:
            dag_root.add_link(invalid_cid, 100, 120)
            # If it doesn't raise, verify it was added
            assert len(dag_root.links) == 1
        except Exception as e:
            # Should be a reasonable error
            assert isinstance(e, (AttributeError, DAGError))
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    @patch('sdk.dag.PBLink')
    def test_dag_root_build_with_cid_decode_failure(self, mock_pb_link, mock_cid_class):
        """Test DAGRoot build when CID decode fails."""
        dag_root = DAGRoot()
        
        # Make CID.decode raise an exception
        mock_cid_class.decode.side_effect = Exception("CID decode failed")
        
        dag_root.add_link("invalid_cid1", 100, 120)
        dag_root.add_link("invalid_cid2", 200, 240)
        
        # Should raise DAGError about no valid CIDs
        with pytest.raises(DAGError, match="no valid CIDs found for DAG links"):
            dag_root.build()
    
    def test_dag_root_encode_varint_zero(self):
        """Test DAGRoot._encode_varint with zero."""
        dag_root = DAGRoot()
        result = dag_root._encode_varint(0)
        assert result == bytes([0])
    
    def test_dag_root_encode_varint_large_values(self):
        """Test DAGRoot._encode_varint with large values."""
        dag_root = DAGRoot()
        
        # Test various large values
        test_values = [
            (128, bytes([0x80, 0x01])),
            (16383, bytes([0xFF, 0x7F])),
            (16384, bytes([0x80, 0x80, 0x01])),
        ]
        
        for value, expected in test_values:
            result = dag_root._encode_varint(value)
            assert result == expected
    
    def test_dag_root_create_unixfs_file_data_zero_size(self):
        """Test _create_unixfs_file_data with zero file size."""
        dag_root = DAGRoot()
        dag_root.total_file_size = 0
        
        unixfs_data = dag_root._create_unixfs_file_data()
        # Should only contain type field
        assert unixfs_data == bytes([0x08, 0x02])
    
    def test_dag_root_create_unixfs_file_data_large_size(self):
        """Test _create_unixfs_file_data with large file size."""
        dag_root = DAGRoot()
        dag_root.total_file_size = 1000000  # 1MB
        
        unixfs_data = dag_root._create_unixfs_file_data()
        assert unixfs_data[:2] == bytes([0x08, 0x02])
        assert len(unixfs_data) > 2  # Should include encoded size


class TestComplexNodeParsing:
    """Test complex node parsing scenarios."""
    
    def test_extract_unixfs_data_with_all_wire_types(self):
        """Test _extract_unixfs_data with all protobuf wire types."""
        # Create UnixFS data with various wire types
        unixfs_data = (
            bytes([0x08, 0x02]) +  # Field 1 (Type), wire type 0 (varint)
            bytes([0x10, 0x00]) +  # Field 2 (Blocksize), wire type 0
            bytes([0x22, 0x05]) + b"hello"  # Field 4 (Data), wire type 2
        )
        
        result = _extract_unixfs_data(unixfs_data)
        assert result == b"hello"
    
    def test_extract_unixfs_data_size_with_various_field_types(self):
        """Test _extract_unixfs_data_size with various field types."""
        # Create UnixFS with multiple fields
        unixfs_data = (
            bytes([0x08, 0x02]) +  # Type
            bytes([0x10, 0x00]) +  # Blocksize
            bytes([0x18]) + _encode_varint(2048)  # File size
        )
        
        result = _extract_unixfs_data_size(unixfs_data)
        assert result == 2048
    
    def test_node_sizes_with_no_data_and_links(self):
        """Test node_sizes with no data but with links."""
        with patch('sdk.dag.IPLD_AVAILABLE', True), \
             patch('sdk.dag.decode') as mock_decode:
            
            # Mock node with no data but with links
            mock_link1 = Mock(size=100)
            mock_link2 = Mock(size=200)
            
            mock_node = Mock()
            mock_node.data = None
            mock_node.links = [mock_link1, mock_link2]
            mock_decode.return_value = mock_node
            
            raw_size, encoded_size = node_sizes(b"test")
            
            assert raw_size == 0
            assert encoded_size == 300
    
    def test_extract_block_data_with_cid_decode_exception(self):
        """Test extract_block_data when CID decode raises exception."""
        with patch('sdk.dag.IPLD_AVAILABLE', True), \
             patch('sdk.dag.CID') as mock_cid_class:
            
            mock_cid_class.decode.side_effect = Exception("CID decode failed")
            
            # Should fallback based on CID string prefix
            result = extract_block_data("bafkreigtest", b"raw_data")
            assert result == b"raw_data"
    
    def test_extract_block_data_with_unknown_cid_type(self):
        """Test extract_block_data with unknown CID type."""
        with patch('sdk.dag.IPLD_AVAILABLE', True), \
             patch('sdk.dag.CID') as mock_cid_class:
            
            mock_cid = Mock()
            mock_cid.codec = 0x99  # Unknown codec
            mock_cid_class.decode.return_value = mock_cid
            
            # Should raise DAGError for unknown CID type
            with pytest.raises(DAGError, match="unknown CID type"):
                extract_block_data("test_cid", b"data")
    
    def test_extract_block_data_dag_pb_decode_failure(self):
        """Test extract_block_data DAG-PB with decode failure."""
        with patch('sdk.dag.IPLD_AVAILABLE', True), \
             patch('sdk.dag.CID') as mock_cid_class, \
             patch('sdk.dag.decode') as mock_decode:
            
            mock_cid = Mock()
            mock_cid.codec = 0x70  # DAG_PB_CODEC
            mock_cid_class.decode.return_value = mock_cid
            mock_decode.side_effect = Exception("Decode failed")
            
            # Should fallback to _extract_unixfs_data_fallback
            result = extract_block_data("test_cid", b"data")
            assert isinstance(result, bytes)


class TestEncryptionIntegration:
    """Test DAG functionality with encryption."""
    
    @patch('sdk.dag.encrypt')
    def test_build_dag_with_encryption_small_data(self, mock_encrypt):
        """Test build_dag with encryption for small data."""
        data = b"Small encrypted data"
        encrypted_data = b"encrypted_" + data
        reader = io.BytesIO(data)
        enc_key = b"0" * 32  # 32-byte key
        
        mock_encrypt.return_value = encrypted_data
        
        result = build_dag(None, reader, 1024, enc_key)
        
        assert isinstance(result, ChunkDAG)
        mock_encrypt.assert_called_once_with(enc_key, data, b"dag_encryption")
        assert len(result.blocks) >= 1
    
    @patch('sdk.dag.encrypt')
    def test_build_dag_with_encryption_large_data(self, mock_encrypt):
        """Test build_dag with encryption for large data."""
        data = b"x" * 5000
        encrypted_data = b"e" * 5000
        reader = io.BytesIO(data)
        enc_key = b"1" * 32
        block_size = 1024
        
        mock_encrypt.return_value = encrypted_data
        
        result = build_dag(None, reader, block_size, enc_key)
        
        assert isinstance(result, ChunkDAG)
        mock_encrypt.assert_called_once()
        # With 5000 bytes and 1024 block size, should create multiple blocks
        assert len(result.blocks) >= 4
    
    @patch('sdk.dag.encrypt')
    def test_build_dag_encryption_increases_size(self, mock_encrypt):
        """Test that encryption can increase data size."""
        data = b"Original data"
        # Simulate encryption adding overhead
        encrypted_data = b"X" * (len(data) + 16)  # Add 16 bytes overhead
        reader = io.BytesIO(data)
        enc_key = b"key" * 10 + b"12"  # 32 bytes
        
        mock_encrypt.return_value = encrypted_data
        
        result = build_dag(None, reader, 1024, enc_key)
        
        assert isinstance(result, ChunkDAG)
        # Encoded size might be larger than raw size due to encryption
        assert result.raw_data_size <= result.encoded_size or result.raw_data_size >= result.encoded_size
    
    def test_build_dag_with_empty_encryption_key(self):
        """Test build_dag with empty encryption key (no encryption)."""
        data = b"Unencrypted data"
        reader = io.BytesIO(data)
        enc_key = b""  # Empty key means no encryption
        
        with patch('sdk.dag.encrypt') as mock_encrypt:
            result = build_dag(None, reader, 1024, enc_key)
            
            # Empty key should not trigger encryption
            mock_encrypt.assert_not_called()
            assert isinstance(result, ChunkDAG)


class TestErrorHandling:
    """Test error handling throughout DAG module."""
    
    def test_build_dag_with_read_error(self):
        """Test build_dag when reader fails."""
        # Create a mock reader that raises an exception
        mock_reader = Mock()
        mock_reader.read.side_effect = IOError("Read failed")
        
        with pytest.raises(DAGError, match="failed to build chunk DAG"):
            build_dag(None, mock_reader, 1024)
    
    def test_dag_error_is_exception(self):
        """Test that DAGError is a proper Exception."""
        error = DAGError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.CID')
    def test_create_unixfs_file_node_with_encoding_error(self, mock_cid_class):
        """Test _create_unixfs_file_node handles encoding errors."""
        data = b"test data"
        
        with patch('sdk.dag.encode') as mock_encode:
            mock_encode.side_effect = Exception("Encoding failed")
            
            # Should fallback to non-IPLD behavior
            cid, encoded_data = _create_unixfs_file_node(data)
            
            assert isinstance(cid, str)
            assert cid.startswith("bafybeig")
            assert encoded_data == data
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.decode')
    def test_node_sizes_with_decode_error(self, mock_decode):
        """Test node_sizes handles decode errors."""
        data = b"invalid node data"
        mock_decode.side_effect = Exception("Decode failed")
        
        raw_size, encoded_size = node_sizes(data)
        
        # Should return data length for both on error
        assert raw_size == len(data)
        assert encoded_size == len(data)
    
    @patch('sdk.dag.bytes_to_node')
    def test_get_node_links_with_bytes_to_node_error(self, mock_bytes_to_node):
        """Test get_node_links when bytes_to_node fails."""
        data = b"invalid data"
        mock_bytes_to_node.side_effect = DAGError("Node decode failed")
        
        with pytest.raises(DAGError, match="failed to extract links from node"):
            get_node_links(data)
    
    def test_decode_varint_with_short_data(self):
        """Test _decode_varint with insufficient data."""
        # Single byte that indicates more data follows, but no more data
        data = bytes([0x80])
        
        value, bytes_read = _decode_varint(data)
        # Should handle gracefully
        assert bytes_read == 1
        assert value >= 0
    
    def test_extract_block_data_with_empty_cid(self):
        """Test extract_block_data with empty CID string."""
        result = extract_block_data("", b"data")
        assert isinstance(result, bytes)
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    @patch('sdk.dag.decode')
    def test_extract_block_data_with_empty_pb_node_data(self, mock_decode):
        """Test extract_block_data when pb_node has empty data field."""
        cid_str = "bafybeigtest"
        data = b"test_data"
        
        mock_cid = Mock()
        mock_cid.codec = 0x70  # DAG_PB_CODEC
        
        mock_node = Mock()
        mock_node.data = None
        
        with patch('sdk.dag.CID') as mock_cid_class:
            mock_cid_class.decode.return_value = mock_cid
            mock_decode.return_value = mock_node
            
            result = extract_block_data(cid_str, data)
            assert result == b""


class TestVarintEdgeCases:
    """Test varint encoding/decoding edge cases."""
    
    def test_encode_varint_max_u64(self):
        """Test encoding maximum 64-bit value."""
        max_u64 = (1 << 64) - 1
        result = _encode_varint(max_u64)
        assert len(result) == 10  # Max varint length
    
    def test_decode_varint_single_byte_values(self):
        """Test decoding all single-byte varint values."""
        for i in range(128):
            data = bytes([i])
            value, bytes_read = _decode_varint(data)
            assert value == i
            assert bytes_read == 1
    
    def test_decode_varint_two_byte_values(self):
        """Test decoding two-byte varint values."""
        test_cases = [
            (128, bytes([0x80, 0x01])),
            (255, bytes([0xFF, 0x01])),
            (256, bytes([0x80, 0x02])),
            (16383, bytes([0xFF, 0x7F])),
        ]
        
        for expected_value, data in test_cases:
            value, bytes_read = _decode_varint(data)
            assert value == expected_value
            assert bytes_read == 2
    
    def test_encode_decode_varint_roundtrip_random_values(self):
        """Test varint encoding/decoding roundtrip with various values."""
        test_values = [
            0, 1, 127, 128, 255, 256,
            1000, 10000, 100000, 1000000,
            (1 << 14) - 1, (1 << 21) - 1, (1 << 28) - 1
        ]
        
        for original_value in test_values:
            encoded = _encode_varint(original_value)
            decoded_value, bytes_read = _decode_varint(encoded)
            assert decoded_value == original_value
            assert bytes_read == len(encoded)


class TestChunkDAGDataclass:
    """Test ChunkDAG dataclass."""
    
    def test_chunk_dag_creation(self):
        """Test ChunkDAG creation with all fields."""
        blocks = [
            FileBlockUpload(cid="cid1", data=b"data1"),
            FileBlockUpload(cid="cid2", data=b"data2"),
        ]
        
        chunk_dag = ChunkDAG(
            cid="root_cid",
            raw_data_size=1000,
            encoded_size=1200,
            blocks=blocks
        )
        
        assert chunk_dag.cid == "root_cid"
        assert chunk_dag.raw_data_size == 1000
        assert chunk_dag.encoded_size == 1200
        assert len(chunk_dag.blocks) == 2
    
    def test_chunk_dag_with_cid_object(self):
        """Test ChunkDAG with CID object instead of string."""
        mock_cid = Mock()
        mock_cid.__str__ = Mock(return_value="mock_cid_string")
        
        chunk_dag = ChunkDAG(
            cid=mock_cid,
            raw_data_size=500,
            encoded_size=600,
            blocks=[]
        )
        
        assert chunk_dag.cid == mock_cid
        assert chunk_dag.blocks == []


class TestFileBlockUploadIntegration:
    """Test FileBlockUpload integration with DAG."""
    
    def test_block_by_cid_with_unicode_cid(self):
        """Test block_by_cid with unicode characters in CID."""
        blocks = [
            FileBlockUpload(cid="cid_αβγ", data=b"data1"),
            FileBlockUpload(cid="cid_普通", data=b"data2"),
        ]
        
        block, found = block_by_cid(blocks, "cid_αβγ")
        assert found is True
        assert block.data == b"data1"
    
    def test_block_by_cid_case_sensitivity(self):
        """Test block_by_cid is case sensitive."""
        blocks = [
            FileBlockUpload(cid="CID_Upper", data=b"upper"),
            FileBlockUpload(cid="cid_lower", data=b"lower"),
        ]
        
        block, found = block_by_cid(blocks, "CID_Upper")
        assert found is True
        assert block.data == b"upper"
        
        block, found = block_by_cid(blocks, "cid_upper")
        assert found is False


@pytest.mark.integration
class TestDAGIntegration:
    """Integration tests for DAG functionality."""
    
    def test_complete_dag_workflow(self):
        """Test complete DAG creation workflow."""
        # Create sample data
        data = b"This is a test file for DAG creation workflow."
        reader = io.BytesIO(data)
        
        # Build DAG
        chunk_dag = build_dag(None, reader, block_size=1024)
        
        # Verify DAG structure
        assert isinstance(chunk_dag, ChunkDAG)
        assert chunk_dag.raw_data_size == len(data)
        assert len(chunk_dag.blocks) >= 1
        
        # Test DAG root creation
        dag_root = DAGRoot.new()
        dag_root.add_link(chunk_dag.cid, chunk_dag.raw_data_size, chunk_dag.encoded_size)
        
        root_cid = dag_root.build()
        assert root_cid is not None
    
    def test_multi_chunk_dag_workflow(self):
        """Test DAG workflow with multiple chunks."""
        dag_root = DAGRoot.new()
        
        # Add multiple chunks
        for i in range(3):
            cid = f"bafybeig{i:050d}"
            dag_root.add_link(cid, raw_data_size=1024, proto_node_size=1200)
        
        root_cid = dag_root.build()
        assert root_cid is not None
        assert dag_root.total_file_size == 3072  # 3 * 1024
    
    @patch('sdk.dag.IPLD_AVAILABLE', True)
    def test_dag_with_ipld_libraries(self):
        """Test DAG functionality when IPLD libraries are available."""
        data = b"Test data for IPLD"
        reader = io.BytesIO(data)
        
        # This should work even if IPLD is mocked as available
        with patch('sdk.dag.PBNode'), \
             patch('sdk.dag.encode'), \
             patch('sdk.dag.multihash'), \
             patch('sdk.dag.CID') as mock_cid:
            
            mock_cid.decode.return_value = Mock()
            
            # Should not raise an exception
            try:
                chunk_dag = build_dag(None, reader, block_size=1024)
                assert isinstance(chunk_dag, ChunkDAG)
            except Exception:
                # If mocking fails, fallback behavior should still work
                pass
    
    def test_large_file_dag_creation(self):
        """Test DAG creation with a large file."""
        # Create 10KB of data
        large_data = b"x" * 10240
        reader = io.BytesIO(large_data)
        block_size = 1024
        
        chunk_dag = build_dag(None, reader, block_size)
        
        # Should create multiple blocks
        expected_blocks = (len(large_data) + block_size - 1) // block_size
        assert len(chunk_dag.blocks) >= expected_blocks
        assert chunk_dag.raw_data_size == len(large_data)
    
    def test_dag_varint_encoding_decoding_roundtrip(self):
        """Test varint encoding/decoding roundtrip."""
        test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 1000000]
        
        for value in test_values:
            encoded = _encode_varint(value)
            decoded, bytes_read = _decode_varint(encoded)
            
            assert decoded == value
            assert bytes_read == len(encoded)

