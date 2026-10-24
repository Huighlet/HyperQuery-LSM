"""
HyperQuery-LSM: Core Storage Types, SSTable Metadata, and Exception Definitions.
"""

from __future__ import annotations

import dataclasses
import time
from typing import Optional


@dataclasses.dataclass(slots=True, frozen=True)
class Entry:
    """
    Log-Structured Merge-Tree key-value entry.
    Supports binary keys, binary values, soft-delete tombstones, and nanosecond timestamps.
    """
    key: bytes
    value: Optional[bytes]
    timestamp_ns: int
    is_tombstone: bool = False

    @classmethod
    def put(cls, key: bytes, value: bytes, timestamp_ns: Optional[int] = None) -> Entry:
        """Create a write entry."""
        ts = timestamp_ns if timestamp_ns is not None else time.time_ns()
        return cls(key=key, value=value, timestamp_ns=ts, is_tombstone=False)

    @classmethod
    def delete(cls, key: bytes, timestamp_ns: Optional[int] = None) -> Entry:
        """Create a tombstone deletion marker."""
        ts = timestamp_ns if timestamp_ns is not None else time.time_ns()
        return cls(key=key, value=None, timestamp_ns=ts, is_tombstone=True)


@dataclasses.dataclass(slots=True, frozen=True)
class SSTableMetadata:
    """Metadata descriptor for an immutable on-disk Sorted String Table (SSTable)."""
    table_id: str
    level: int
    min_key: bytes
    max_key: bytes
    entry_count: int
    file_size_bytes: int
    file_path: str


@dataclasses.dataclass(slots=True, frozen=True)
class CompactionStats:
    """Metrics recorded during a multi-way SSTable merge compaction run."""
    level_from: int
    level_to: int
    input_tables: int
    output_tables: int
    entries_processed: int
    tombstones_purged: int
    duration_sec: float


# -------------------------------------------------------------------------
# Exceptions
# -------------------------------------------------------------------------

class HyperQueryError(Exception):
    """Base exception for all HyperQuery-LSM storage engine errors."""


class CorruptedWALError(HyperQueryError):
    """Raised when Write-Ahead Log frame checksum verification fails."""


class CorruptedSSTableError(HyperQueryError):
    """Raised when an on-disk SSTable fails magic byte or index validation."""


class CompactionError(HyperQueryError):
    """Raised when an error occurs during SSTable compaction."""
