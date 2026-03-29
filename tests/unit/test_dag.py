"""
Unit tests for the DAG module (sdk/dag.py).

Issue: https://github.com/akave-ai/akavesdk-py/issues/102
"""

import io
from unittest.mock import patch

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
    _extract_unixfs_data_size,
    block_by_cid,
    build_dag,
    extract_block_data,
    node_sizes,
)
from sdk.model import FileBlockUpload


class TestVarintEncodingDecoding:
    """Tests for varint encode/decode roundtrip."""

    def test_encode_single_byte(self):
        assert _encode_varint(0) == b"\x00"
        assert _encode_varint(127) == b"\x7f"

    def test_encode_multi_byte(self):
        assert _encode_varint(128) == b"\x80\x01"
        assert _encode_varint(300) == b"\xac\x02"

    def test_decode_varint(self):
        value, bytes_read = _decode_varint(b"\xac\x02")
        assert value == 300
        assert bytes_read == 2

    def test_roundtrip(self):
        for val in [0, 1, 127, 128, 16384, 1_000_000]:
            decoded, _ = _decode_varint(_encode_varint(val))
            assert decoded == val

    def test_decode_too_long_raises(self):
        with pytest.raises(ValueError, match="varint too long"):
            _decode_varint(b"\x80" * 10 + b"\x01")


class TestDAGRoot:
    """Tests for DAGRoot creation, add_link, and build."""

    def test_new_initial_state(self):
        root = DAGRoot.new()
        assert root.links == []
        assert root.total_file_size == 0

    def test_add_link(self):
        root = DAGRoot.new()
        root.add_link("bafybeigtest", 1024, 1100)
        assert len(root.links) == 1
        assert root.total_file_size == 1024

    def test_build_no_chunks_raises(self):
        with pytest.raises(DAGError, match="no chunks added"):
            DAGRoot.new().build()

    def test_build_single_chunk_returns_cid(self):
        root = DAGRoot.new()
        root.add_link("bafybeigplainstring", 512, 550)
        assert root.build() == "bafybeigplainstring"

    def test_build_multiple_chunks(self):
        root = DAGRoot.new()
        cid1, _ = _create_unixfs_file_node(b"chunk1")
        cid2, _ = _create_unixfs_file_node(b"chunk2")
        root.add_link(str(cid1), 1024, 1100)
        root.add_link(str(cid2), 1024, 1100)
        assert len(str(root.build())) > 0

    def test_unixfs_file_data_type_field(self):
        root = DAGRoot.new()
        root.total_file_size = 1024
        data = root._create_unixfs_file_data()
        assert data[:2] == bytes([0x08, 0x02])


class TestBuildDag:
    """Tests for build_dag()."""

    def test_empty_data_raises(self):
        with pytest.raises(DAGError):
            build_dag(None, io.BytesIO(b""), 1024)

    def test_single_block(self):
        result = build_dag(None, io.BytesIO(b"hello world"), 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) == 1

    def test_multi_block(self):
        result = build_dag(None, io.BytesIO(b"x" * 3000), 1024)
        assert len(result.blocks) >= 3

    def test_boundary_one_over_block_size(self):
        result = build_dag(None, io.BytesIO(b"a" * 1025), 1024)
        assert len(result.blocks) == 2


class TestUnixfsNodeCreation:
    """Tests for _create_unixfs_file_node and _create_chunk_dag_root_node."""

    def test_create_unixfs_file_node(self):
        cid, encoded = _create_unixfs_file_node(b"hello")
        assert cid is not None
        assert len(encoded) > 0

    def test_same_data_same_cid(self):
        cid1, _ = _create_unixfs_file_node(b"deterministic")
        cid2, _ = _create_unixfs_file_node(b"deterministic")
        assert str(cid1) == str(cid2)

    def test_create_chunk_dag_root_node(self):
        blocks = [
            FileBlockUpload(cid="bafybeigblock1abc", data=b"a" * 100),
            FileBlockUpload(cid="bafybeigblock2def", data=b"b" * 200),
        ]
        cid, size = _create_chunk_dag_root_node(blocks)
        assert cid is not None
        assert size > 0


class TestNodeSizesAndExtraction:
    """Tests for node_sizes, extract_block_data, and UnixFS parsing."""

    def test_node_sizes(self):
        _, encoded = _create_unixfs_file_node(b"hello world")
        raw_size, encoded_size = node_sizes(encoded)
        assert raw_size > 0
        assert encoded_size > 0

    def test_extract_block_data_roundtrip(self):
        original = b"extract me please"
        cid, encoded = _create_unixfs_file_node(original)
        assert extract_block_data(str(cid), encoded) == original

    def test_extract_unixfs_data(self):
        payload = b"inner_data"
        length = _encode_varint(len(payload))
        unixfs = bytes([0x08, 0x02, 0x22]) + length + payload
        assert _extract_unixfs_data(unixfs) == payload

    def test_extract_unixfs_data_size(self):
        size_bytes = _encode_varint(1024)
        unixfs = bytes([0x08, 0x02, 0x18]) + size_bytes
        assert _extract_unixfs_data_size(unixfs) == 1024


class TestBlockByCid:
    """Tests for block_by_cid()."""

    def test_found(self):
        blocks = [
            FileBlockUpload(cid="cid_a", data=b"a"),
            FileBlockUpload(cid="cid_b", data=b"b"),
        ]
        block, found = block_by_cid(blocks, "cid_b")
        assert found is True
        assert block.data == b"b"

    def test_not_found(self):
        _, found = block_by_cid([FileBlockUpload(cid="x", data=b"x")], "missing")
        assert found is False


class TestFallbackBehavior:
    """Tests for fallback when IPLD is unavailable."""

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_create_unixfs_file_node_fallback(self):
        cid, encoded = _create_unixfs_file_node(b"fallback data")
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert encoded == b"fallback data"

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_build_dag_fallback(self):
        result = build_dag(None, io.BytesIO(b"small data"), 1024)
        assert isinstance(result, ChunkDAG)
        assert str(result.cid).startswith("bafybeig")

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_dag_root_build_fallback(self):
        root = DAGRoot.new()
        root.add_link("bafybeigchunk1", 500, 520)
        root.add_link("bafybeigchunk2", 500, 520)
        result = root.build()
        assert isinstance(result, str)
        assert result.startswith("bafybeig")
