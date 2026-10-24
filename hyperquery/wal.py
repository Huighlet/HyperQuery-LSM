"""
HyperQuery-LSM: Write-Ahead Log (WAL) with CRC32 Verification & Crash Recovery.
"""

from __future__ import annotations

import io
from pathlib import Path
import struct
from typing import List, Optional
import zlib

from hyperquery.types import CorruptedWALError, Entry

# WAL Header: crc32 (uint32), timestamp_ns (uint64), is_tombstone (uint8), key_len (uint32), val_len (uint32)
# Size: 4 + 8 + 1 + 4 + 4 = 21 bytes
_WAL_HEADER_FORMAT = ">IQBII"
_WAL_HEADER_SIZE = struct.calcsize(_WAL_HEADER_FORMAT)


class WriteAheadLog:
    """
    Append-only binary Write-Ahead Log.
    Guarantees durability by persisting entries before MemTable insertion.
    """

    def __init__(self, file_path: Path | str) -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.file_path.open("a+b")

    def append(self, entry: Entry) -> None:
        """Write an entry frame with CRC32 checksum."""
        key_bytes = entry.key
        val_bytes = entry.value if entry.value is not None else b""
        key_len = len(key_bytes)
        val_len = len(val_bytes)
        tombstone_flag = 1 if entry.is_tombstone else 0

        # Compute CRC32 over payload
        payload_header = struct.pack(">QBII", entry.timestamp_ns, tombstone_flag, key_len, val_len)
        hasher = zlib.crc32(payload_header)
        hasher = zlib.crc32(key_bytes, hasher)
        if val_len > 0:
            hasher = zlib.crc32(val_bytes, hasher)
        checksum = hasher & 0xFFFFFFFF

        frame = struct.pack(_WAL_HEADER_FORMAT, checksum, entry.timestamp_ns, tombstone_flag, key_len, val_len)
        self._file.write(frame + key_bytes + val_bytes)

    def flush(self) -> None:
        """Flush buffer to storage."""
        self._file.flush()

    def close(self) -> None:
        """Close log file."""
        if not self._file.closed:
            self._file.flush()
            self._file.close()

    @classmethod
    def replay(cls, file_path: Path | str) -> List[Entry]:
        """
        Replay all valid log entries from a WAL file during crash recovery.

        Raises:
            CorruptedWALError: If CRC32 checksum or framing bounds fail.
        """
        path = Path(file_path)
        if not path.is_file():
            return []

        entries: List[Entry] = []
        data = path.read_bytes()
        pos = 0
        total_len = len(data)

        while pos < total_len:
            if pos + _WAL_HEADER_SIZE > total_len:
                raise CorruptedWALError(f"Incomplete WAL header at byte offset {pos}")

            checksum, ts, is_tomb, key_len, val_len = struct.unpack(
                _WAL_HEADER_FORMAT, data[pos : pos + _WAL_HEADER_SIZE]
            )

            expected_frame_len = _WAL_HEADER_SIZE + key_len + val_len
            if pos + expected_frame_len > total_len:
                raise CorruptedWALError(f"Truncated WAL payload at byte offset {pos}")

            payload_header = struct.pack(">QBII", ts, is_tomb, key_len, val_len)
            k_start = pos + _WAL_HEADER_SIZE
            k_end = k_start + key_len
            key = data[k_start:k_end]

            v_start = k_end
            v_end = v_start + val_len
            val = data[v_start:v_end] if val_len > 0 else None

            # Verify CRC32
            hasher = zlib.crc32(payload_header)
            hasher = zlib.crc32(key, hasher)
            if val_len > 0 and val is not None:
                hasher = zlib.crc32(val, hasher)
            computed_crc = hasher & 0xFFFFFFFF

            if computed_crc != checksum:
                raise CorruptedWALError(
                    f"CRC32 mismatch in WAL at offset {pos}. Expected {checksum}, computed {computed_crc}"
                )

            entry = Entry(
                key=key,
                value=val if not is_tomb else None,
                timestamp_ns=ts,
                is_tombstone=bool(is_tomb),
            )
            entries.append(entry)
            pos += expected_frame_len

        return entries
