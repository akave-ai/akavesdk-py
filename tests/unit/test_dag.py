import io
from unittest.mock import Mock, patch

import pytest

from sdk.dag import (
    ChunkDAG,
    DAGError,
    DAGRoot,
    _create_chunk_dag_root_node,
    _create_unixfs_file_node,
    _decode_varint,
    _encode_varint,
    _extract_unixfs_data,
    _extract_unixfs_data_fallback,
    _extract_unixfs_data_size,
    block_by_cid,
    build_dag,
    extract_block_data,
    get_node_links,
    node_sizes,
)
from sdk.model import FileBlockUpload


# ---------------------------------------------------------------------------
# DAGRoot
# ---------------------------------------------------------------------------


class TestDAGRoot:
    def test_new(self):
        dag = DAGRoot.new()
        assert isinstance(dag, DAGRoot)
        assert dag.links == []
        assert dag.total_file_size == 0

    def test_add_link_string_cid(self):
        dag = DAGRoot.new()
        dag.add_link("bafybeigtest123", 1024, 512)
        assert len(dag.links) == 1
        assert dag.links[0]["cid_str"] == "bafybeigtest123"
        assert dag.links[0]["size"] == 512
        assert dag.total_file_size == 1024

    def test_add_link_cid_object(self):
        dag = DAGRoot.new()
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigtest456"
        dag.add_link(mock_cid, 2048, 1024)
        assert len(dag.links) == 1
        assert dag.total_file_size == 2048

    def test_add_multiple_links_accumulates_size(self):
        dag = DAGRoot.new()
        dag.add_link("cid1", 1024, 512)
        dag.add_link("cid2", 2048, 1024)
        assert len(dag.links) == 2
        assert dag.total_file_size == 3072

    def test_build_no_links_raises(self):
        with pytest.raises(DAGError, match="no chunks added"):
            DAGRoot.new().build()

    def test_build_single_link_returns_cid_directly(self):
        dag = DAGRoot.new()
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigsinglelinktest"
        dag.add_link(mock_cid, 1024, 512)
        assert dag.build() == "bafybeigsinglelinktest"

    def test_build_multiple_links_fallback(self):
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            dag = DAGRoot.new()
            dag.add_link("cid1", 1024, 512)
            dag.add_link("cid2", 1024, 512)
            result = dag.build()
        assert isinstance(result, str)
        assert result.startswith("bafybeig")

    def test_create_unixfs_file_data_type_field(self):
        dag = DAGRoot.new()
        dag.total_file_size = 1024
        result = dag._create_unixfs_file_data()
        assert result[:2] == b"\x08\x02"
        assert len(result) > 2

    def test_create_unixfs_file_data_zero_size(self):
        dag = DAGRoot.new()
        dag.total_file_size = 0
        assert dag._create_unixfs_file_data() == b"\x08\x02"


# ---------------------------------------------------------------------------
# Varint
# ---------------------------------------------------------------------------


class TestVarint:
    def test_encode_zero(self):
        assert _encode_varint(0) == b"\x00"

    def test_encode_single_byte(self):
        assert _encode_varint(1) == b"\x01"
        assert _encode_varint(127) == b"\x7f"

    def test_encode_multi_byte(self):
        assert _encode_varint(128) == b"\x80\x01"
        assert _encode_varint(300) == b"\xac\x02"

    def test_decode_single_byte(self):
        assert _decode_varint(b"\x00") == (0, 1)
        assert _decode_varint(b"\x7f") == (127, 1)

    def test_decode_multi_byte(self):
        assert _decode_varint(b"\x80\x01") == (128, 2)
        assert _decode_varint(b"\xac\x02") == (300, 2)

    def test_decode_with_trailing_bytes(self):
        value, n = _decode_varint(b"\x7fextra")
        assert value == 127
        assert n == 1

    def test_decode_too_long_raises(self):
        with pytest.raises(ValueError, match="varint too long"):
            _decode_varint(b"\x80" * 20)

    def test_roundtrip(self):
        for v in [0, 1, 127, 128, 255, 16384, 1_000_000]:
            encoded = _encode_varint(v)
            decoded, n = _decode_varint(encoded)
            assert decoded == v
            assert n == len(encoded)


# ---------------------------------------------------------------------------
# UnixFS data extraction
# ---------------------------------------------------------------------------


class TestUnixFSExtraction:
    def test_extract_size_field_3(self):
        # field 3 (fileSize), varint for 1024: 0x80 0x08
        unixfs = bytes([0x18, 0x80, 0x08])
        assert _extract_unixfs_data_size(unixfs) == 1024

    def test_extract_size_empty(self):
        assert _extract_unixfs_data_size(b"") == 0

    def test_extract_size_field_4_length(self):
        # field 4, length-delimited, length = 4
        unixfs = bytes([0x22, 0x04]) + b"test"
        assert _extract_unixfs_data_size(unixfs) == 4

    def test_extract_unixfs_data_field_4(self):
        content = b"hello world"
        unixfs = bytes([0x22, len(content)]) + content
        assert _extract_unixfs_data(unixfs) == content

    def test_extract_unixfs_data_empty(self):
        assert _extract_unixfs_data(b"") == b""

    def test_extract_unixfs_data_fallback_returns_bytes(self):
        inner = bytes([0x08, 0x02])
        node = bytes([0x0A, len(inner)]) + inner
        result = _extract_unixfs_data_fallback(node)
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# ChunkDAG
# ---------------------------------------------------------------------------


class TestChunkDAG:
    def test_creation(self):
        blocks = [FileBlockUpload(cid="cid1", data=b"data1")]
        dag = ChunkDAG(cid="root", raw_data_size=100, encoded_size=200, blocks=blocks)
        assert dag.cid == "root"
        assert dag.raw_data_size == 100
        assert dag.encoded_size == 200
        assert dag.blocks == blocks


# ---------------------------------------------------------------------------
# build_dag
# ---------------------------------------------------------------------------


class TestBuildDAG:
    def test_small_data_single_block(self):
        data = b"small data"
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = build_dag(None, io.BytesIO(data), block_size=1024)
        assert isinstance(result, ChunkDAG)
        assert result.raw_data_size == len(data)
        assert len(result.blocks) == 1
        assert result.blocks[0].data == data

    def test_empty_data_raises(self):
        with pytest.raises(DAGError, match="empty data"):
            build_dag(None, io.BytesIO(b""), block_size=1024)

    def test_multiple_blocks(self):
        data = b"x" * 3000
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = build_dag(None, io.BytesIO(data), block_size=1024)
        assert result.raw_data_size == 3000
        assert len(result.blocks) == 3

    def test_partial_last_block(self):
        data = b"x" * 2500
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = build_dag(None, io.BytesIO(data), block_size=1024)
        assert result.raw_data_size == 2500
        assert len(result.blocks) == 3


# ---------------------------------------------------------------------------
# _create_unixfs_file_node / _create_chunk_dag_root_node
# ---------------------------------------------------------------------------


class TestNodeCreation:
    def test_unixfs_file_node_fallback(self):
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            cid, encoded = _create_unixfs_file_node(b"test data")
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert encoded == b"test data"

    def test_chunk_dag_root_fallback(self):
        blocks = [
            FileBlockUpload(cid="cid1", data=b"block1"),
            FileBlockUpload(cid="cid2", data=b"block2"),
        ]
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            cid, size = _create_chunk_dag_root_node(blocks)
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert size == len(b"block1block2")


# ---------------------------------------------------------------------------
# node_sizes
# ---------------------------------------------------------------------------


class TestNodeSizes:
    def test_fallback_returns_len(self):
        data = b"some node data"
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            raw, encoded = node_sizes(data)
        assert raw == len(data)
        assert encoded == len(data)


# ---------------------------------------------------------------------------
# extract_block_data
# ---------------------------------------------------------------------------


class TestExtractBlockData:
    def test_fallback_returns_bytes(self):
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = extract_block_data("bafkreigtest", b"raw data")
        assert isinstance(result, bytes)

    def test_empty_data_fallback(self):
        with patch("sdk.dag.IPLD_AVAILABLE", False):
            result = extract_block_data("bafybeigtest", b"")
        assert result == b""


# ---------------------------------------------------------------------------
# get_node_links
# ---------------------------------------------------------------------------


class TestGetNodeLinks:
    def test_with_mocked_node(self):
        mock_link = Mock()
        mock_link.hash = "test_cid"
        mock_link.name = "test"
        mock_link.size = 1024
        mock_node = Mock()
        mock_node.links = [mock_link]

        with patch("sdk.dag.bytes_to_node", return_value=mock_node):
            result = get_node_links(b"dummy")

        assert len(result) == 1
        assert result[0]["cid"] == "test_cid"
        assert result[0]["name"] == "test"
        assert result[0]["size"] == 1024

    def test_invalid_data_raises(self):
        with patch("sdk.dag.decode", side_effect=Exception("bad proto")):
            with pytest.raises(DAGError):
                get_node_links(b"\xff\xff\xff")


# ---------------------------------------------------------------------------
# block_by_cid
# ---------------------------------------------------------------------------


class TestBlockByCID:
    def test_found(self):
        blocks = [
            FileBlockUpload(cid="cid1", data=b"data1"),
            FileBlockUpload(cid="cid2", data=b"data2"),
        ]
        block, found = block_by_cid(blocks, "cid1")
        assert found is True
        assert block.cid == "cid1"

    def test_not_found(self):
        blocks = [FileBlockUpload(cid="cid1", data=b"data1")]
        block, found = block_by_cid(blocks, "cid999")
        assert found is False
        assert block.cid == ""
