import struct
from typing import List, Optional

import zfec

_MAGIC_SUFFIX = bytes([0xDE, 0xAD, 0xBE, 0xEF])


class ErasureCodeError(Exception):
    pass


class ErasureCode:
    def __init__(self, data_blocks: int, parity_blocks: int):
        if data_blocks <= 0 or parity_blocks <= 0:
            raise ErasureCodeError("data and parity shards must be > 0")
        self.data_blocks = data_blocks
        self.parity_blocks = parity_blocks
        self._total = data_blocks + parity_blocks
        self._encoder = zfec.Encoder(data_blocks, self._total)
        self._decoder = zfec.Decoder(data_blocks, self._total)

    @classmethod
    def new(cls, data_blocks: int, parity_blocks: int) -> "ErasureCode":
        return cls(data_blocks, parity_blocks)

    def encode(self, data: bytes) -> bytes:
        wrapped = _wrap_data(data)
        pieces = _split(wrapped, self.data_blocks)
        all_pieces = self._encoder.encode(pieces)
        return b"".join(all_pieces)

    def reconstruct_all(self, blocks: List[Optional[bytes]]) -> None:
        if len(blocks) != self._total:
            raise ErasureCodeError(f"expected {self._total} blocks, got {len(blocks)}")

        available = [(i, b) for i, b in enumerate(blocks) if b is not None]
        if len(available) < self.data_blocks:
            raise ErasureCodeError(f"too many missing blocks: have {len(available)}, need at least {self.data_blocks}")

        indices = [i for i, _ in available[: self.data_blocks]]
        pieces = [b for _, b in available[: self.data_blocks]]

        try:
            data_pieces = self._decoder.decode(pieces, indices)
            all_pieces = self._encoder.encode(list(data_pieces))
        except zfec.easyfec.Error as e:
            raise ErasureCodeError(f"reconstruction failed: {e}") from e

        for i, piece in enumerate(all_pieces):
            blocks[i] = bytes(piece)

    def extract_data(self, blocks: List[Optional[bytes]], original_size: int) -> bytes:
        if len(blocks) != self._total:
            raise ErasureCodeError(f"expected {self._total} blocks, got {len(blocks)}")

        available = [(i, b) for i, b in enumerate(blocks) if b is not None]
        if len(available) < self.data_blocks:
            raise ErasureCodeError(f"too many missing blocks: have {len(available)}, need at least {self.data_blocks}")

        indices = [i for i, _ in available[: self.data_blocks]]
        pieces = [b for _, b in available[: self.data_blocks]]

        try:
            data_pieces = self._decoder.decode(pieces, indices)
        except zfec.easyfec.Error as e:
            raise ErasureCodeError(f"decode failed: {e}") from e

        joined = b"".join(bytes(p) for p in data_pieces)

        if original_size == 0:
            return _unwrap_data(joined)
        return joined[:original_size]


def _wrap_data(data: bytes) -> bytes:
    size = struct.pack(">Q", len(data))
    return size + data + _MAGIC_SUFFIX


def _unwrap_data(buf: bytes) -> bytes:
    min_len = 8 + len(_MAGIC_SUFFIX)
    if len(buf) < min_len:
        raise ErasureCodeError("buffer too short")

    (size,) = struct.unpack(">Q", buf[:8])
    data_start = 8
    data_end = data_start + size
    n = data_end + len(_MAGIC_SUFFIX)

    if n > len(buf):
        raise ErasureCodeError("buffer too short")

    if buf[data_end:n] != _MAGIC_SUFFIX:
        raise ErasureCodeError("missing suffix or corrupted data")

    return buf[data_start:data_end]


def _split(data: bytes, k: int) -> List[bytes]:
    remainder = len(data) % k
    if remainder:
        data = data + b"\x00" * (k - remainder)
    piece_size = len(data) // k
    return [data[i * piece_size : (i + 1) * piece_size] for i in range(k)]
