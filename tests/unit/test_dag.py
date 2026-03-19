"""Unit tests for sdk/dag.py - DAG creation, CID operations, and block handling."""

import hashlib
import io
from unittest.mock import Mock, patch, MagicMock

import pytest

from sdk.dag import (
    DAGRoot,
    ChunkDAG,
    DAGError,
    build_dag,
    _create_unixfs_file_node,
    _create_chunk_dag_root_node,
    node_sizes,
    _extract_unixfs_data_size,
    _encode_varint,
    _decode_varint,
    extract_block_data,
    _extract_unixfs_data_fallback,
    _extract_unixfs_data,
    bytes_to_node,
    get_node_links,
    block_by_cid,
)
from sdk.model import FileBlockUpload


class TestDAGRoot:
    """Test DAGRoot class functionality."""

    def test_new(self):
        """Test DAGRoot.new() creates a new instance."""
        dag_root = DAGRoot.new()
        assert isinstance(dag_root, DAGRoot)
        assert dag_root.node is None
        assert dag_root.fs_node_data == b""
        assert dag_root.links == []
        assert dag_root.total_file_size == 0

    def test_add_link_with_string_cid(self):
        """Test adding a link with string CID."""
        dag_root = DAGRoot.new()
        cid_str = "bafybeigtest123"
        raw_data_size = 1024
        proto_node_size = 512

        dag_root.add_link(cid_str, raw_data_size, proto_node_size)

        assert len(dag_root.links) == 1
        assert dag_root.links[0]["cid_str"] == cid_str
        assert dag_root.links[0]["size"] == proto_node_size
        assert dag_root.total_file_size == raw_data_size

    def test_add_link_with_cid_object(self):
        """Test adding a link with CID object."""
        dag_root = DAGRoot.new()
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigtest456"
        raw_data_size = 2048
        proto_node_size = 1024

        dag_root.add_link(mock_cid, raw_data_size, proto_node_size)

        assert len(dag_root.links) == 1
        assert dag_root.total_file_size == raw_data_size

    def test_add_multiple_links(self):
        """Test adding multiple links accumulates size."""
        dag_root = DAGRoot.new()

        dag_root.add_link("cid1", 1024, 512)
        dag_root.add_link("cid2", 2048, 1024)
        dag_root.add_link("cid3", 4096, 2048)

        assert len(dag_root.links) == 3
        assert dag_root.total_file_size == 1024 + 2048 + 4096

    def test_build_no_links_raises_error(self):
        """Test build() raises error when no links added."""
        dag_root = DAGRoot.new()

        with pytest.raises(DAGError, match="no chunks added"):
            dag_root.build()

    def test_build_single_link_returns_cid(self):
        """Test build() with single link returns that link's CID."""
        dag_root = DAGRoot.new()
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigsinglelinktest"

        dag_root.add_link(mock_cid, 1024, 512)
        result = dag_root.build()

        assert result == "bafybeigsinglelinktest"

    def test_encode_varint_single_byte(self):
        """Test _encode_varint for values fitting in single byte."""
        dag_root = DAGRoot.new()

        assert dag_root._encode_varint(0) == b"\x00"
        assert dag_root._encode_varint(1) == b"\x01"
        assert dag_root._encode_varint(127) == b"\x7f"

    def test_encode_varint_multi_byte(self):
        """Test _encode_varint for values requiring multiple bytes."""
        dag_root = DAGRoot.new()

        # 128 requires 2 bytes
        assert dag_root._encode_varint(128) == b"\x80\x01"
        assert dag_root._encode_varint(255) == b"\xff\x01"
        assert dag_root._encode_varint(16384) == b"\x80\x80\x01"

    def test_create_unixfs_file_data(self):
        """Test _create_unixfs_file_data creates proper UnixFS data."""
        dag_root = DAGRoot.new()
        dag_root.total_file_size = 1024

        result = dag_root._create_unixfs_file_data()

        # Should start with type = File (0x08, 0x02)
        assert result[:2] == b"\x08\x02"
        # Should contain size field and encoded varint
        assert len(result) > 2

    def test_create_unixfs_file_data_zero_size(self):
        """Test _create_unixfs_file_data with zero size."""
        dag_root = DAGRoot.new()
        dag_root.total_file_size = 0

        result = dag_root._create_unixfs_file_data()

        # Should only have type field, no size
        assert result == b"\x08\x02"


class TestVarintEncoding:
    """Test varint encoding and decoding functions."""

    def test_encode_varint_zero(self):
        """Test encoding zero."""
        assert _encode_varint(0) == b"\x00"

    def test_encode_varint_small_values(self):
        """Test encoding small values."""
        assert _encode_varint(1) == b"\x01"
        assert _encode_varint(127) == b"\x7f"

    def test_encode_varint_medium_values(self):
        """Test encoding medium values requiring multi-byte."""
        assert _encode_varint(128) == b"\x80\x01"
        assert _encode_varint(300) == b"\xac\x02"

    def test_encode_varint_large_values(self):
        """Test encoding large values."""
        assert _encode_varint(16384) == b"\x80\x80\x01"

    def test_decode_varint_single_byte(self):
        """Test decoding single-byte varints."""
        value, bytes_read = _decode_varint(b"\x00")
        assert value == 0
        assert bytes_read == 1

        value, bytes_read = _decode_varint(b"\x7f")
        assert value == 127
        assert bytes_read == 1

    def test_decode_varint_multi_byte(self):
        """Test decoding multi-byte varints."""
        value, bytes_read = _decode_varint(b"\x80\x01")
        assert value == 128
        assert bytes_read == 2

        value, bytes_read = _decode_varint(b"\xac\x02")
        assert value == 300
        assert bytes_read == 2

    def test_decode_varint_with_extra_data(self):
        """Test decoding varint when followed by extra data."""
        value, bytes_read = _decode_varint(b"\x7fextra_data")
        assert value == 127
        assert bytes_read == 1

    def test_decode_varint_too_long_raises_error(self):
        """Test decoding overlong varint raises ValueError."""
        long_varint = b"\x80" * 20
        with pytest.raises(ValueError, match="varint too long"):
            _decode_varint(long_varint)

    def test_encode_decode_roundtrip(self):
        """Test encode-decode roundtrip for various values."""
        test_values = [0, 1, 127, 128, 255, 256, 16384, 1000000]

        for value in test_values:
            encoded = _encode_varint(value)
            decoded, bytes_read = _decode_varint(encoded)
            assert decoded == value, f"Roundtrip failed for {value}"
            assert bytes_read == len(encoded)


class TestUnixFSDataExtraction:
    """Test UnixFS data extraction functions."""

    def test_extract_unixfs_data_size_simple(self):
        """Test extracting size from simple UnixFS data."""
        # Field 3 (fileSize) with value 1024
        unixfs_data = bytes([0x18, 0x80, 0x08])  # 3<<3|0, then varint for 1024
        size = _extract_unixfs_data_size(unixfs_data)
        assert size == 1024

    def test_extract_unixfs_data_size_zero(self):
        """Test extracting zero size."""
        # Empty UnixFS data
        unixfs_data = b""
        size = _extract_unixfs_data_size(unixfs_data)
        assert size == 0

    def test_extract_unixfs_data_with_field_4(self):
        """Test extracting from field 4 (data length)."""
        # Field 4 (data) with length-delimited wire type
        unixfs_data = bytes([0x22, 0x04]) + b"test"  # field 4, length 4, then "test"
        size = _extract_unixfs_data_size(unixfs_data)
        assert size == 4

    def test_extract_unixfs_data_fallback_simple(self):
        """Test fallback extraction with simple data."""
        # Field 1 (data field) with UnixFS inside
        inner_data = bytes([0x08, 0x02])  # Type=File
        node_data = bytes([0x0a, len(inner_data)]) + inner_data

        result = _extract_unixfs_data_fallback(node_data)
        # Should return the node data or inner data
        assert isinstance(result, bytes)

    def test_extract_unixfs_data_empty_inner(self):
        """Test extract with empty inner data."""
        result = _extract_unixfs_data(b"")
        assert result == b""

    def test_extract_unixfs_data_with_field_4(self):
        """Test extracting actual data from field 4."""
        # Field 4 with "hello world"
        data_content = b"hello world"
        unixfs_data = bytes([0x22, len(data_content)]) + data_content

        result = _extract_unixfs_data(unixfs_data)
        assert result == data_content


class TestChunkDAG:
    """Test ChunkDAG dataclass."""

    def test_chunk_dag_creation(self):
        """Test ChunkDAG instantiation."""
        blocks = [FileBlockUpload(cid="cid1", data=b"data1")]
        chunk_dag = ChunkDAG(
            cid="root_cid", raw_data_size=100, encoded_size=200, blocks=blocks
        )

        assert chunk_dag.cid == "root_cid"
        assert chunk_dag.raw_data_size == 100
        assert chunk_dag.encoded_size == 200
        assert chunk_dag.blocks == blocks

    def test_chunk_dag_empty_blocks(self):
        """Test ChunkDAG with empty blocks."""
        chunk_dag = ChunkDAG(
            cid="root_cid", raw_data_size=0, encoded_size=0, blocks=[]
        )

        assert chunk_dag.blocks == []


class TestBuildDAG:
    """Test build_dag function."""

    def test_build_dag_small_data_no_ipld(self):
        """Test build_dag with small data and no IPLD library."""
        data = b"small data"
        reader = io.BytesIO(data)

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = build_dag(None, reader, block_size=1024)

        assert isinstance(result, ChunkDAG)
        assert result.raw_data_size == len(data)
        assert len(result.blocks) == 1
        assert result.blocks[0].data == data

    def test_build_dag_empty_data_raises_error(self):
        """Test build_dag with empty data raises error."""
        reader = io.BytesIO(b"")

        with pytest.raises(DAGError, match="empty data"):
            build_dag(None, reader, block_size=1024)

    def test_build_dag_exact_block_size(self):
        """Test build_dag when data exactly matches block size."""
        data = b"x" * 1024
        reader = io.BytesIO(data)

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = build_dag(None, reader, block_size=1024)

        assert result.raw_data_size == 1024
        assert len(result.blocks) == 1

    def test_build_dag_multiple_blocks(self):
        """Test build_dag with data spanning multiple blocks."""
        data = b"x" * 3000  # 3x1024 bytes
        reader = io.BytesIO(data)

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = build_dag(None, reader, block_size=1024)

        assert result.raw_data_size == 3000
        assert len(result.blocks) == 3

    def test_build_dag_partial_last_block(self):
        """Test build_dag with partial last block."""
        data = b"x" * 2500  # 2.5 blocks of 1024
        reader = io.BytesIO(data)

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = build_dag(None, reader, block_size=1024)

        assert result.raw_data_size == 2500
        assert len(result.blocks) == 3


class TestCreateUnixFSFileNode:
    """Test _create_unixfs_file_node function."""

    def test_create_unixfs_file_node_fallback(self):
        """Test node creation with IPLD unavailable."""
        data = b"test data"

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            cid, encoded = _create_unixfs_file_node(data)

        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert encoded == data

    def test_create_unixfs_file_node_empty_data_fallback(self):
        """Test empty data node creation fallback."""
        data = b""

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            cid, encoded = _create_unixfs_file_node(data)

        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")


class TestCreateChunkDAGRootNode:
    """Test _create_chunk_dag_root_node function."""

    def test_create_chunk_dag_root_no_ipld(self):
        """Test chunk DAG root creation without IPLD."""
        blocks = [
            FileBlockUpload(cid="cid1", data=b"block1"),
            FileBlockUpload(cid="cid2", data=b"block2"),
        ]

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            cid, size = _create_chunk_dag_root_node(blocks)

        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert size == len(b"block1block2")

    def test_create_chunk_dag_root_empty_blocks(self):
        """Test chunk DAG root with empty blocks list."""
        blocks = []

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            cid, size = _create_chunk_dag_root_node(blocks)

        assert isinstance(cid, str)
        assert size == 0


class TestNodeSizes:
    """Test node_sizes function."""

    def test_node_sizes_simple_data_no_ipld(self):
        """Test node size calculation without IPLD."""
        node_data = b"simple node data"

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            raw_size, encoded_size = node_sizes(node_data)

        assert raw_size == len(node_data)
        assert encoded_size == len(node_data)

    def test_node_sizes_empty_node_no_ipld(self):
        """Test node size for empty node."""
        node_data = b""

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            raw_size, encoded_size = node_sizes(node_data)

        assert raw_size == 0
        assert encoded_size == 0


class TestExtractBlockData:
    """Test extract_block_data function."""

    def test_extract_block_data_raw_codec_no_ipld(self):
        """Test extracting block data in raw codec without IPLD."""
        cid_str = "bafkreigtest"
        data = b"raw block data"

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = extract_block_data(cid_str, data)

        # Should return fallback extraction
        assert isinstance(result, bytes)

    def test_extract_block_data_empty_data(self):
        """Test extracting from empty data."""
        cid_str = "bafybeigtest"
        data = b""

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = extract_block_data(cid_str, data)

        assert result == b""


class TestBytesToNode:
    """Test bytes_to_node function."""

    def test_bytes_to_node_invalid_data_raises_error(self):
        """Test bytes_to_node with invalid data raises error."""
        invalid_data = b"\xff\xff\xff"

        with patch("sdk.dag.DAG_PB_AVAILABLE", False):
            with pytest.raises(DAGError, match="failed to decode"):
                bytes_to_node(invalid_data)


class TestGetNodeLinks:
    """Test get_node_links function."""

    def test_get_node_links_empty_node(self):
        """Test getting links from node with no links."""
        # This test would need actual PBNode or mocking
        with patch("sdk.dag.DAG_PB_AVAILABLE", False):
            with pytest.raises(DAGError):
                get_node_links(b"invalid")

    def test_get_node_links_with_mock_node(self):
        """Test getting links from mocked node."""
        mock_link = Mock()
        mock_link.hash = "test_cid"
        mock_link.name = "test"
        mock_link.size = 1024

        mock_node = Mock()
        mock_node.links = [mock_link]

        with patch("sdk.dag.bytes_to_node", return_value=mock_node):
            result = get_node_links(b"dummy_data")

        assert len(result) == 1
        assert result[0]["cid"] == "test_cid"
        assert result[0]["name"] == "test"
        assert result[0]["size"] == 1024


class TestBlockByCID:
    """Test block_by_cid function."""

    def test_block_by_cid_found(self):
        """Test finding block by CID."""
        blocks = [
            FileBlockUpload(cid="cid1", data=b"data1"),
            FileBlockUpload(cid="cid2", data=b"data2"),
        ]

        block, found = block_by_cid(blocks, "cid1")

        assert found is True
        assert block.cid == "cid1"
        assert block.data == b"data1"

    def test_block_by_cid_not_found(self):
        """Test block not found returns false."""
        blocks = [
            FileBlockUpload(cid="cid1", data=b"data1"),
        ]

        block, found = block_by_cid(blocks, "cid999")

        assert found is False
        assert block.cid == ""
        assert block.data == b""

    def test_block_by_cid_empty_list(self):
        """Test searching empty block list."""
        block, found = block_by_cid([], "cid1")

        assert found is False


class TestDAGIntegration:
    """Integration tests for DAG functionality."""

    def test_dag_root_build_workflow_fallback(self):
        """Test complete DAGRoot workflow with fallback (no IPLD)."""
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            dag_root = DAGRoot.new()

            dag_root.add_link("cid1", 1024, 512)
            dag_root.add_link("cid2", 1024, 512)

            result = dag_root.build()

            assert isinstance(result, str)
            assert result.startswith("bafybeig")

    def test_build_dag_workflow_small_data(self):
        """Test complete build_dag workflow with small data."""
        data = b"Hello, World!"
        reader = io.BytesIO(data)

        with patch("sdk.dag.IPLD_AVAILABLE", False):
            chunk_dag = build_dag(None, reader, block_size=1024)

        assert chunk_dag.raw_data_size == len(data)
        assert len(chunk_dag.blocks) >= 1
        assert chunk_dag.blocks[0].data == data

    def test_varint_roundtrip_with_sizes(self):
        """Test varint encoding/decoding with file sizes."""
        sizes = [1024, 65536, 1048576, 1024 * 1024 * 100]

        for size in sizes:
            encoded = _encode_varint(size)
            decoded, _ = _decode_varint(encoded)
            assert decoded == size
