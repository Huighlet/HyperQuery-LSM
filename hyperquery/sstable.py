"""
HyperQuery-LSM: Immutable Sorted String Table (SSTable) Writer & Reader.
Includes Data Blocks, Sparse Index Binary Search, Bloom Filters, and 48-byte Footers.
"""

from __future__ import annotations

import bisect
from pathlib import Path
import struct
from typing import Iterator, List, Optional, Tuple

from hyperquery.bloom import BloomFilter
from hyperquery.types import CorruptedSSTableError, Entry, SSTableMetadata

# Footer format:
# data_offset (Q), data_len (Q), index_offset (Q), index_len (Q), filter_offset (Q), filter_len (Q), entry_count (I), magic (8s)
# 8*6 + 4 + 8 = 60 bytes
_MAGIC = b"HYPERLSM"
_FOOTER_FORMAT = ">QQQQQQI8s"
_FOOTER_SIZE = struct.calcsize(_FOOTER_FORMAT)


class SSTableWriter:
    """Writes sorted entries to an immutable on-disk SSTable file."""

    @staticmethod
    def write(
        file_path: Path | str,
        entries: List[Entry],
        level: int = 0,
        index_interval: int = 16,
    ) -> SSTableMetadata:
        """
        Serialize sorted entries into SSTable layout:
        [Data Section] -> [Sparse Index] -> [Bloom Filter] -> [Footer]
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if not entries:
            raise ValueError("Cannot write an empty SSTable.")

        bloom = BloomFilter(expected_elements=len(entries), false_positive_rate=0.01)
        sparse_index: List[Tuple[bytes, int]] = []

        data_buffer = bytearray()
        for idx, entry in enumerate(entries):
            bloom.add(entry.key)
            data_offset = len(data_buffer)

            if idx % index_interval == 0:
                sparse_index.append((entry.key, data_offset))

            key_len = len(entry.key)
            val_bytes = entry.value if entry.value is not None else b""
            val_len = len(val_bytes)
            tomb = 1 if entry.is_tombstone else 0

            # Entry frame: ts (Q), tomb (B), key_len (I), key, val_len (I), val
            header = struct.pack(">QBI", entry.timestamp_ns, tomb, key_len)
            val_header = struct.pack(">I", val_len)
            data_buffer.extend(header + entry.key + val_header + val_bytes)

        data_offset = 0
        data_len = len(data_buffer)

        # Build Sparse Index Buffer: count (I) -> [key_len (I), key, offset (Q)]...
        index_buffer = bytearray()
        index_buffer.extend(struct.pack(">I", len(sparse_index)))
        for k, off in sparse_index:
            index_buffer.extend(struct.pack(">IQ", len(k), off) + k)

        index_offset = data_len
        index_len = len(index_buffer)

        # Build Bloom Filter Buffer
        filter_bytes = bloom.serialize()
        filter_offset = index_offset + index_len
        filter_len = len(filter_bytes)

        # Build Footer
        footer = struct.pack(
            _FOOTER_FORMAT,
            data_offset,
            data_len,
            index_offset,
            index_len,
            filter_offset,
            filter_len,
            len(entries),
            _MAGIC,
        )

        with path.open("wb") as f:
            f.write(data_buffer)
            f.write(index_buffer)
            f.write(filter_bytes)
            f.write(footer)

        min_k = entries[0].key
        max_k = entries[-1].key
        total_size = len(data_buffer) + len(index_buffer) + len(filter_bytes) + len(footer)

        return SSTableMetadata(
            table_id=path.stem,
            level=level,
            min_key=min_k,
            max_key=max_k,
            entry_count=len(entries),
            file_size_bytes=total_size,
            file_path=str(path.resolve()),
        )


class SSTableReader:
    """Reads and queries an immutable SSTable file."""

    def __init__(self, file_path: Path | str) -> None:
        self.path = Path(file_path)
        if not self.path.is_file():
            raise FileNotFoundError(f"SSTable not found: {self.path}")

        self._data = self.path.read_bytes()
        total_len = len(self._data)
        if total_len < _FOOTER_SIZE:
            raise CorruptedSSTableError("SSTable too short to contain footer.")

        # Read Footer
        footer_bytes = self._data[-_FOOTER_SIZE:]
        (
            self.data_offset,
            self.data_len,
            self.index_offset,
            self.index_len,
            self.filter_offset,
            self.filter_len,
            self.entry_count,
            magic,
        ) = struct.unpack(_FOOTER_FORMAT, footer_bytes)

        if magic != _MAGIC:
            raise CorruptedSSTableError(f"Invalid SSTable magic bytes: {magic}")

        # Load Bloom Filter
        filter_slice = self._data[self.filter_offset : self.filter_offset + self.filter_len]
        self.bloom = BloomFilter.deserialize(filter_slice)

        # Load Sparse Index
        index_slice = self._data[self.index_offset : self.index_offset + self.index_len]
        num_index_entries = struct.unpack(">I", index_slice[:4])[0]
        pos = 4
        self._index_keys: List[bytes] = []
        self._index_offsets: List[int] = []

        for _ in range(num_index_entries):
            k_len, off = struct.unpack(">IQ", index_slice[pos : pos + 12])
            pos += 12
            k = index_slice[pos : pos + k_len]
            pos += k_len
            self._index_keys.append(k)
            self._index_offsets.append(off)

    def get(self, key: bytes) -> Optional[Entry]:
        """
        Point lookup:
        1. Fast Bloom Filter rejection (0 disk seeks)
        2. Binary search on Sparse Index
        3. Scan target data block
        """
        # Step 1: Bloom Filter check
        if not self.bloom.contains(key):
            return None

        # Step 2: Binary search on Sparse Index
        idx = bisect.bisect_right(self._index_keys, key) - 1
        if idx < 0:
            idx = 0

        start_offset = self.data_offset + self._index_offsets[idx]
        end_offset = (
            self.data_offset + self._index_offsets[idx + 1]
            if idx + 1 < len(self._index_offsets)
            else self.data_offset + self.data_len
        )

        # Step 3: Scan candidate data block
        pos = start_offset
        while pos < end_offset:
            ts, tomb, k_len = struct.unpack(">QBI", self._data[pos : pos + 13])
            pos += 13
            entry_key = self._data[pos : pos + k_len]
            pos += k_len

            v_len = struct.unpack(">I", self._data[pos : pos + 4])[0]
            pos += 4
            entry_val = self._data[pos : pos + v_len] if v_len > 0 else None
            pos += v_len

            if entry_key == key:
                return Entry(
                    key=entry_key,
                    value=entry_val if not tomb else None,
                    timestamp_ns=ts,
                    is_tombstone=bool(tomb),
                )
            elif entry_key > key:
                # Keys are strictly sorted, so key is not present
                break

        return None

    def scan(
        self,
        start_key: Optional[bytes] = None,
        end_key: Optional[bytes] = None,
    ) -> Iterator[Entry]:
        """Yield entries within [start_key, end_key)."""
        pos = self.data_offset
        end_data = self.data_offset + self.data_len

        while pos < end_data:
            ts, tomb, k_len = struct.unpack(">QBI", self._data[pos : pos + 13])
            pos += 13
            entry_key = self._data[pos : pos + k_len]
            pos += k_len

            v_len = struct.unpack(">I", self._data[pos : pos + 4])[0]
            pos += 4
            entry_val = self._data[pos : pos + v_len] if v_len > 0 else None
            pos += v_len

            if start_key is not None and entry_key < start_key:
                continue
            if end_key is not None and entry_key >= end_key:
                break

            yield Entry(
                key=entry_key,
                value=entry_val if not tomb else None,
                timestamp_ns=ts,
                is_tombstone=bool(tomb),
            )
