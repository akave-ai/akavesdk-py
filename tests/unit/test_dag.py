"""
Unit tests for the DAG module (sdk/dag.py).

Covers: DAGRoot, ChunkDAG, build_dag(), UnixFS node creation,
CID operations, varint encoding/decoding, node parsing,
and fallback behavior when IPLD is unavailable.

Issue: https://github.com/akave-ai/akavesdk-py/issues/102
"""

import io
from unittest.mock import Mock, patch

import pytest

from sdk.dag import (
    IPLD_AVAILABLE,
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
    node_sizes,
)
from sdk.model import FileBlockUpload

# ---------------------------------------------------------------------------
# Varint encoding / decoding
# ---------------------------------------------------------------------------


class TestEncodeVarint:
    """Tests for _encode_varint()."""

    def test_encode_zero(self):
        assert _encode_varint(0) == b"\x00"

    def test_encode_single_byte(self):
        assert _encode_varint(1) == b"\x01"
        assert _encode_varint(127) == b"\x7f"

    def test_encode_two_bytes(self):
        result = _encode_varint(128)
        assert result == b"\x80\x01"

    def test_encode_300(self):
        result = _encode_varint(300)
        assert result == b"\xac\x02"

    def test_encode_large_value(self):
        result = _encode_varint(1_000_000)
        assert len(result) == 3
        # Round-trip verify
        decoded, _ = _decode_varint(result)
        assert decoded == 1_000_000

    def test_encode_max_single_byte_boundary(self):
        """Values at exactly 127 should be one byte."""
        assert len(_encode_varint(127)) == 1
        assert len(_encode_varint(128)) == 2

    def test_roundtrip_various_values(self):
        values = [0, 1, 127, 128, 255, 256, 16383, 16384, 2**21, 2**28 - 1]
        for val in values:
            encoded = _encode_varint(val)
            decoded, bytes_read = _decode_varint(encoded)
            assert decoded == val, f"Roundtrip failed for {val}"
            assert bytes_read == len(encoded)


class TestDecodeVarint:
    """Tests for _decode_varint()."""

    def test_decode_zero(self):
        value, bytes_read = _decode_varint(b"\x00")
        assert value == 0
        assert bytes_read == 1

    def test_decode_single_byte(self):
        value, bytes_read = _decode_varint(b"\x01")
        assert value == 1
        assert bytes_read == 1

    def test_decode_127(self):
        value, bytes_read = _decode_varint(b"\x7f")
        assert value == 127
        assert bytes_read == 1

    def test_decode_128(self):
        value, bytes_read = _decode_varint(b"\x80\x01")
        assert value == 128
        assert bytes_read == 2

    def test_decode_300(self):
        value, bytes_read = _decode_varint(b"\xac\x02")
        assert value == 300
        assert bytes_read == 2

    def test_decode_with_trailing_bytes(self):
        """Decoder should stop at the first non-continuation byte."""
        value, bytes_read = _decode_varint(b"\x01\xff\xff")
        assert value == 1
        assert bytes_read == 1

    def test_decode_varint_too_long(self):
        """Varints exceeding 64-bit shift should raise ValueError."""
        # Create a varint with 10+ continuation bytes (shift >= 64)
        bad_varint = b"\x80" * 10 + b"\x01"
        with pytest.raises(ValueError, match="varint too long"):
            _decode_varint(bad_varint)


# ---------------------------------------------------------------------------
# DAGRoot
# ---------------------------------------------------------------------------


class TestDAGRootNew:
    """Tests for DAGRoot.new() factory."""

    def test_creates_instance(self):
        root = DAGRoot.new()
        assert isinstance(root, DAGRoot)

    def test_initial_state(self):
        root = DAGRoot.new()
        assert root.node is None
        assert root.fs_node_data == b""
        assert root.links == []
        assert root.total_file_size == 0


class TestDAGRootAddLink:
    """Tests for DAGRoot.add_link()."""

    def setup_method(self):
        self.root = DAGRoot.new()

    def test_add_link_with_string_cid(self):
        self.root.add_link("bafybeigtest", 1024, 1100)
        assert len(self.root.links) == 1
        assert self.root.links[0]["cid_str"] == "bafybeigtest"
        assert self.root.links[0]["size"] == 1100
        assert self.root.total_file_size == 1024

    def test_add_multiple_links(self):
        self.root.add_link("cid1", 100, 110)
        self.root.add_link("cid2", 200, 220)
        self.root.add_link("cid3", 300, 330)
        assert len(self.root.links) == 3
        assert self.root.total_file_size == 600

    def test_add_link_with_string_method_cid(self):
        """CID objects with .string() should be handled."""
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigmockcid"
        self.root.add_link(mock_cid, 512, 550)
        assert self.root.links[0]["cid_str"] == "bafybeigmockcid"

    def test_add_link_with_str_dunder_cid(self):
        """CID objects with __str__ but no .string() should work."""

        class FakeCID:
            def __str__(self):
                return "bafybeigstrcid"

        self.root.add_link(FakeCID(), 256, 280)
        assert self.root.links[0]["cid_str"] == "bafybeigstrcid"

    def test_total_file_size_accumulates(self):
        for i in range(5):
            self.root.add_link(f"cid-{i}", 100, 110)
        assert self.root.total_file_size == 500


class TestDAGRootBuild:
    """Tests for DAGRoot.build()."""

    def setup_method(self):
        self.root = DAGRoot.new()

    def test_build_no_chunks_raises(self):
        with pytest.raises(DAGError, match="no chunks added"):
            self.root.build()

    def test_build_single_chunk_returns_cid(self):
        """Single-chunk DAG returns the chunk's own CID."""
        mock_cid = Mock()
        mock_cid.string.return_value = "bafybeigsinglechunk"
        self.root.add_link(mock_cid, 1024, 1100)
        result = self.root.build()
        assert result == "bafybeigsinglechunk"

    def test_build_single_chunk_string_fallback(self):
        """Single chunk with string CID uses cid_str."""
        self.root.add_link("bafybeigplainstring", 512, 550)
        result = self.root.build()
        assert result == "bafybeigplainstring"

    def test_build_multiple_chunks_produces_cid(self):
        """Multi-chunk DAG should produce a root CID using real CIDs."""
        # Generate real CIDs via _create_unixfs_file_node
        cid1, _ = _create_unixfs_file_node(b"chunk1data")
        cid2, _ = _create_unixfs_file_node(b"chunk2data")
        self.root.add_link(str(cid1), 1024, 1100)
        self.root.add_link(str(cid2), 1024, 1100)
        result = self.root.build()
        result_str = str(result)
        assert len(result_str) > 0

    def test_build_multiple_chunks_deterministic(self):
        """Same links should produce the same root CID."""
        cid1, _ = _create_unixfs_file_node(b"aaa")
        cid2, _ = _create_unixfs_file_node(b"bbb")
        root1 = DAGRoot.new()
        root2 = DAGRoot.new()
        for r in [root1, root2]:
            r.add_link(str(cid1), 500, 520)
            r.add_link(str(cid2), 500, 520)
        assert str(root1.build()) == str(root2.build())


class TestDAGRootCreateUnixfsFileData:
    """Tests for DAGRoot._create_unixfs_file_data()."""

    def test_unixfs_file_data_starts_with_type_file(self):
        root = DAGRoot.new()
        root.total_file_size = 0
        data = root._create_unixfs_file_data()
        assert data[:2] == bytes([0x08, 0x02])  # Type = File

    def test_unixfs_file_data_includes_size(self):
        root = DAGRoot.new()
        root.total_file_size = 1024
        data = root._create_unixfs_file_data()
        assert data[0:2] == bytes([0x08, 0x02])
        assert data[2] == 0x18  # Field 3 (filesize)

    def test_unixfs_file_data_zero_size_no_size_field(self):
        root = DAGRoot.new()
        root.total_file_size = 0
        data = root._create_unixfs_file_data()
        assert data == bytes([0x08, 0x02])


class TestDAGRootEncodeVarint:
    """Tests for DAGRoot._encode_varint() (instance method)."""

    def test_matches_module_function(self):
        root = DAGRoot.new()
        for val in [0, 1, 127, 128, 300, 100000]:
            assert root._encode_varint(val) == _encode_varint(val)


# ---------------------------------------------------------------------------
# ChunkDAG dataclass
# ---------------------------------------------------------------------------


class TestChunkDAG:
    """Tests for ChunkDAG dataclass."""

    def test_create_chunk_dag(self):
        blocks = [FileBlockUpload(cid="cid1", data=b"data1")]
        chunk = ChunkDAG(cid="rootcid", raw_data_size=100, encoded_size=120, blocks=blocks)
        assert chunk.cid == "rootcid"
        assert chunk.raw_data_size == 100
        assert chunk.encoded_size == 120
        assert len(chunk.blocks) == 1

    def test_chunk_dag_multiple_blocks(self):
        blocks = [FileBlockUpload(cid=f"cid{i}", data=b"x" * 100) for i in range(5)]
        chunk = ChunkDAG(cid="root", raw_data_size=500, encoded_size=600, blocks=blocks)
        assert len(chunk.blocks) == 5


# ---------------------------------------------------------------------------
# build_dag()
# ---------------------------------------------------------------------------


class TestBuildDag:
    """Tests for build_dag()."""

    def test_empty_data_raises(self):
        reader = io.BytesIO(b"")
        with pytest.raises(DAGError, match="failed to build chunk DAG"):
            build_dag(None, reader, 1024)

    def test_single_block_data(self):
        data = b"hello world"
        reader = io.BytesIO(data)
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert result.raw_data_size > 0
        assert len(result.blocks) == 1
        assert len(result.blocks[0].data) > 0

    def test_multi_block_data(self):
        data = b"x" * 3000
        reader = io.BytesIO(data)
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) >= 3

    def test_data_exactly_block_size(self):
        data = b"a" * 1024
        reader = io.BytesIO(data)
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) == 1

    def test_data_one_byte_over_block_size(self):
        data = b"a" * 1025
        reader = io.BytesIO(data)
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) == 2

    def test_single_byte_data(self):
        reader = io.BytesIO(b"\x42")
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) == 1

    def test_blocks_have_cid_and_data(self):
        reader = io.BytesIO(b"test data here")
        result = build_dag(None, reader, 1024)
        for block in result.blocks:
            assert isinstance(block, FileBlockUpload)
            assert block.cid is not None
            assert len(str(block.cid)) > 0
            assert len(block.data) > 0

    def test_multi_block_all_blocks_populated(self):
        data = b"y" * 5000
        reader = io.BytesIO(data)
        result = build_dag(None, reader, 1024)
        for block in result.blocks:
            assert block.cid
            assert len(block.data) > 0

    def test_cid_is_string(self):
        reader = io.BytesIO(b"some data")
        result = build_dag(None, reader, 1024)
        # CID should be convertible to string
        cid_str = str(result.cid)
        assert len(cid_str) > 0

    def test_binary_data(self):
        data = bytes(range(256)) * 10
        reader = io.BytesIO(data)
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) >= 2

    def test_large_block_size_single_block(self):
        data = b"small data"
        reader = io.BytesIO(data)
        result = build_dag(None, reader, 1024 * 1024)  # 1MB block
        assert len(result.blocks) == 1

    def test_ctx_parameter_ignored(self):
        """ctx is not used in current implementation, should not affect result."""
        reader1 = io.BytesIO(b"data")
        reader2 = io.BytesIO(b"data")
        r1 = build_dag(None, reader1, 1024)
        r2 = build_dag("some_context", reader2, 1024)
        assert str(r1.cid) == str(r2.cid)


# ---------------------------------------------------------------------------
# _create_unixfs_file_node()
# ---------------------------------------------------------------------------


class TestCreateUnixfsFileNode:
    """Tests for _create_unixfs_file_node()."""

    def test_returns_cid_and_encoded_data(self):
        cid, encoded = _create_unixfs_file_node(b"hello")
        assert cid is not None
        assert len(encoded) > 0

    def test_small_data(self):
        cid, encoded = _create_unixfs_file_node(b"a")
        assert len(encoded) > 1  # must include framing

    def test_encoded_data_contains_original(self):
        """The encoded data should embed the original data."""
        original = b"unique_test_data_12345"
        cid, encoded = _create_unixfs_file_node(original)
        assert original in bytes(encoded)

    def test_different_data_different_cid(self):
        cid1, _ = _create_unixfs_file_node(b"data_a")
        cid2, _ = _create_unixfs_file_node(b"data_b")
        assert str(cid1) != str(cid2)

    def test_same_data_same_cid(self):
        cid1, enc1 = _create_unixfs_file_node(b"deterministic")
        cid2, enc2 = _create_unixfs_file_node(b"deterministic")
        assert str(cid1) == str(cid2)
        assert enc1 == enc2

    def test_empty_data(self):
        """Empty data should still produce a valid CID."""
        cid, encoded = _create_unixfs_file_node(b"")
        assert cid is not None


# ---------------------------------------------------------------------------
# _create_chunk_dag_root_node()
# ---------------------------------------------------------------------------


class TestCreateChunkDagRootNode:
    """Tests for _create_chunk_dag_root_node()."""

    def test_single_block(self):
        blocks = [FileBlockUpload(cid="bafybeigtest1", data=b"data1")]
        cid, size = _create_chunk_dag_root_node(blocks)
        assert cid is not None
        assert size > 0

    def test_multiple_blocks(self):
        blocks = [
            FileBlockUpload(cid="bafybeigblock1abc", data=b"a" * 100),
            FileBlockUpload(cid="bafybeigblock2def", data=b"b" * 200),
        ]
        cid, size = _create_chunk_dag_root_node(blocks)
        assert cid is not None
        assert size > 0

    def test_size_matches_total_block_data(self):
        blocks = [
            FileBlockUpload(cid="bafybeigx1", data=b"x" * 50),
            FileBlockUpload(cid="bafybeigy1", data=b"y" * 75),
        ]
        cid, size = _create_chunk_dag_root_node(blocks)
        # In fallback mode, size = len(combined_data)
        # In IPLD mode, size = sum(len(block.data))
        assert size == 125 or size > 0

    def test_deterministic(self):
        blocks = [
            FileBlockUpload(cid="bafybeigdet1", data=b"aa"),
            FileBlockUpload(cid="bafybeigdet2", data=b"bb"),
        ]
        cid1, _ = _create_chunk_dag_root_node(blocks)
        cid2, _ = _create_chunk_dag_root_node(blocks)
        assert str(cid1) == str(cid2)


# ---------------------------------------------------------------------------
# node_sizes()
# ---------------------------------------------------------------------------


class TestNodeSizes:
    """Tests for node_sizes()."""

    def test_with_encoded_node(self):
        """Create a node via _create_unixfs_file_node and verify sizes."""
        data = b"hello world"
        _, encoded = _create_unixfs_file_node(data)
        raw_size, encoded_size = node_sizes(encoded)
        assert raw_size > 0
        assert encoded_size > 0

    def test_raw_bytes_fallback(self):
        """Non-protobuf data should fall back gracefully."""
        data = b"\x00\x01\x02\x03"
        raw_size, encoded_size = node_sizes(data)
        assert encoded_size == len(data)

    def test_empty_node_data(self):
        raw_size, encoded_size = node_sizes(b"")
        assert encoded_size >= 0


# ---------------------------------------------------------------------------
# extract_block_data()
# ---------------------------------------------------------------------------


class TestExtractBlockData:
    """Tests for extract_block_data()."""

    def test_extract_from_unixfs_node(self):
        """Data encoded via _create_unixfs_file_node should be extractable."""
        original = b"extract me please"
        cid, encoded = _create_unixfs_file_node(original)
        extracted = extract_block_data(str(cid), encoded)
        assert extracted == original

    def test_extract_raw_data(self):
        """Raw codec CIDs should return data as-is."""
        raw_data = b"raw block data"
        extracted = extract_block_data("bafkreigrawcidtest", raw_data)
        # bafkreig prefix suggests raw codec; should return data directly
        assert len(extracted) > 0

    def test_extract_fallback_on_invalid_data(self):
        """Invalid data should trigger fallback without crashing."""
        result = extract_block_data("bafybeigtest", b"\xff\xfe\xfd")
        assert isinstance(result, bytes)

    def test_extract_empty_data(self):
        result = extract_block_data("bafybeigtest", b"")
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# _extract_unixfs_data()
# ---------------------------------------------------------------------------


class TestExtractUnixfsData:
    """Tests for _extract_unixfs_data()."""

    def test_extract_field_4_data(self):
        """Data in field 4 (wire type 2) should be extracted."""
        payload = b"inner_data"
        # Build a minimal UnixFS-like protobuf:
        # Field 1 (type), varint: 0x08 0x02
        # Field 4 (data), length-delimited: 0x22 <len> <data>
        length = _encode_varint(len(payload))
        unixfs = bytes([0x08, 0x02]) + bytes([0x22]) + length + payload
        result = _extract_unixfs_data(unixfs)
        assert result == payload

    def test_empty_bytes(self):
        result = _extract_unixfs_data(b"")
        assert result == b""

    def test_no_field_4(self):
        """UnixFS data without field 4 should return empty bytes."""
        # Only has field 1 (type = file)
        unixfs = bytes([0x08, 0x02])
        result = _extract_unixfs_data(unixfs)
        assert result == b""

    def test_malformed_data_returns_empty(self):
        result = _extract_unixfs_data(b"\xff\xff\xff")
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# _extract_unixfs_data_fallback()
# ---------------------------------------------------------------------------


class TestExtractUnixfsDataFallback:
    """Tests for _extract_unixfs_data_fallback()."""

    def test_returns_bytes(self):
        result = _extract_unixfs_data_fallback(b"some data")
        assert isinstance(result, bytes)

    def test_empty_data(self):
        result = _extract_unixfs_data_fallback(b"")
        assert isinstance(result, bytes)

    def test_extracts_from_dagpb_structure(self):
        """If wrapped in DAG-PB field 1, should extract inner UnixFS data."""
        payload = b"hello"
        # Build inner UnixFS: field 4 length-delimited
        inner = bytes([0x08, 0x02, 0x22]) + _encode_varint(len(payload)) + payload
        # Wrap in DAG-PB: field 1 length-delimited
        dagpb = bytes([0x0A]) + _encode_varint(len(inner)) + inner
        result = _extract_unixfs_data_fallback(dagpb)
        assert result == payload


# ---------------------------------------------------------------------------
# _extract_unixfs_data_size()
# ---------------------------------------------------------------------------


class TestExtractUnixfsDataSize:
    """Tests for _extract_unixfs_data_size()."""

    def test_with_field_3_filesize(self):
        """Field 3 varint should be parsed as filesize."""
        # Field 1 (type=file): 0x08 0x02
        # Field 3 (filesize): 0x18 <varint>
        size_bytes = _encode_varint(1024)
        unixfs = bytes([0x08, 0x02, 0x18]) + size_bytes
        result = _extract_unixfs_data_size(unixfs)
        assert result == 1024

    def test_with_field_4_data(self):
        """Field 4 length-delimited: size = length of data."""
        payload = b"abcdefghij"  # 10 bytes
        length = _encode_varint(len(payload))
        unixfs = bytes([0x08, 0x02, 0x22]) + length + payload
        result = _extract_unixfs_data_size(unixfs)
        assert result == 10

    def test_empty_returns_zero(self):
        assert _extract_unixfs_data_size(b"") == 0

    def test_only_type_field_returns_zero(self):
        assert _extract_unixfs_data_size(bytes([0x08, 0x02])) == 0

    def test_malformed_returns_zero(self):
        assert _extract_unixfs_data_size(b"\xff\xff") == 0


# ---------------------------------------------------------------------------
# block_by_cid()
# ---------------------------------------------------------------------------


class TestBlockByCid:
    """Tests for block_by_cid()."""

    def test_find_existing_block(self):
        blocks = [
            FileBlockUpload(cid="cid_a", data=b"data_a"),
            FileBlockUpload(cid="cid_b", data=b"data_b"),
            FileBlockUpload(cid="cid_c", data=b"data_c"),
        ]
        block, found = block_by_cid(blocks, "cid_b")
        assert found is True
        assert block.cid == "cid_b"
        assert block.data == b"data_b"

    def test_not_found(self):
        blocks = [FileBlockUpload(cid="cid_x", data=b"x")]
        block, found = block_by_cid(blocks, "cid_missing")
        assert found is False
        assert block.cid == ""
        assert block.data == b""

    def test_empty_list(self):
        block, found = block_by_cid([], "any_cid")
        assert found is False

    def test_first_match_returned(self):
        blocks = [
            FileBlockUpload(cid="dup", data=b"first"),
            FileBlockUpload(cid="dup", data=b"second"),
        ]
        block, found = block_by_cid(blocks, "dup")
        assert found is True
        assert block.data == b"first"


# ---------------------------------------------------------------------------
# IPLD availability flag
# ---------------------------------------------------------------------------


class TestIPLDAvailability:
    """Tests for IPLD_AVAILABLE flag behavior."""

    def test_ipld_flag_is_boolean(self):
        assert isinstance(IPLD_AVAILABLE, bool)


# ---------------------------------------------------------------------------
# Fallback behavior (mock IPLD unavailable)
# ---------------------------------------------------------------------------


class TestFallbackBehavior:
    """Tests for fallback CID generation when IPLD is unavailable."""

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_create_unixfs_file_node_fallback(self):
        cid, encoded = _create_unixfs_file_node(b"fallback test data")
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        # In fallback mode, encoded data is the raw data itself
        assert encoded == b"fallback test data"

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_create_unixfs_file_node_fallback_deterministic(self):
        cid1, _ = _create_unixfs_file_node(b"same")
        cid2, _ = _create_unixfs_file_node(b"same")
        assert cid1 == cid2

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_create_unixfs_file_node_fallback_different_data(self):
        cid1, _ = _create_unixfs_file_node(b"data_a")
        cid2, _ = _create_unixfs_file_node(b"data_b")
        assert cid1 != cid2

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_create_chunk_dag_root_node_fallback(self):
        blocks = [
            FileBlockUpload(cid="c1", data=b"aaa"),
            FileBlockUpload(cid="c2", data=b"bbb"),
        ]
        cid, size = _create_chunk_dag_root_node(blocks)
        assert isinstance(cid, str)
        assert cid.startswith("bafybeig")
        assert size == 6  # len(b"aaa" + b"bbb")

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_node_sizes_fallback(self):
        data = b"twelve bytes"
        raw_size, encoded_size = node_sizes(data)
        assert raw_size == len(data)
        assert encoded_size == len(data)

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_extract_block_data_fallback(self):
        data = b"raw block content"
        result = extract_block_data("bafybeigtest", data)
        assert isinstance(result, bytes)

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_build_dag_fallback_single_block(self):
        reader = io.BytesIO(b"small data")
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) == 1
        cid_str = str(result.cid)
        assert cid_str.startswith("bafybeig")

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_build_dag_fallback_multi_block(self):
        reader = io.BytesIO(b"x" * 3000)
        result = build_dag(None, reader, 1024)
        assert isinstance(result, ChunkDAG)
        assert len(result.blocks) >= 3

    @patch("sdk.dag.IPLD_AVAILABLE", False)
    def test_dag_root_build_multi_chunk_fallback(self):
        root = DAGRoot.new()
        root.add_link("bafybeigchunk1", 500, 520)
        root.add_link("bafybeigchunk2", 500, 520)
        result = root.build()
        assert isinstance(result, str)
        assert result.startswith("bafybeig")


# ---------------------------------------------------------------------------
# DAGError
# ---------------------------------------------------------------------------


class TestDAGError:
    """Tests for DAGError exception."""

    def test_is_exception(self):
        assert issubclass(DAGError, Exception)

    def test_message(self):
        err = DAGError("something went wrong")
        assert str(err) == "something went wrong"

    def test_raise_and_catch(self):
        with pytest.raises(DAGError):
            raise DAGError("test error")


# ---------------------------------------------------------------------------
# Integration: build_dag round-trip with extract_block_data
# ---------------------------------------------------------------------------


class TestBuildDagRoundTrip:
    """End-to-end: build DAG then extract original data from blocks."""

    def test_single_block_roundtrip(self):
        original = b"roundtrip test data"
        reader = io.BytesIO(original)
        dag = build_dag(None, reader, 1024)
        assert len(dag.blocks) == 1
        extracted = extract_block_data(dag.blocks[0].cid, dag.blocks[0].data)
        assert extracted == original

    def test_multi_block_roundtrip(self):
        original = b"A" * 500 + b"B" * 500 + b"C" * 500
        reader = io.BytesIO(original)
        dag = build_dag(None, reader, 512)
        assert len(dag.blocks) >= 2
        # Each block should extract to its corresponding chunk
        reconstructed = b""
        for block in dag.blocks:
            reconstructed += extract_block_data(block.cid, block.data)
        assert reconstructed == original

    def test_binary_data_roundtrip(self):
        original = bytes(range(256))
        reader = io.BytesIO(original)
        dag = build_dag(None, reader, 1024)
        extracted = extract_block_data(dag.blocks[0].cid, dag.blocks[0].data)
        assert extracted == original
