"""
Tests for SSTable binary serialization, Sparse Index binary search, and Bloom filter rejection.
"""

from pathlib import Path
import pytest
from hyperquery.sstable import SSTableReader, SSTableWriter
from hyperquery.types import Entry


def test_sstable_write_and_read(tmp_path: Path):
    sst_file = tmp_path / "table0.sst"
    entries = [
        Entry.put(f"key-{i:04d}".encode("ascii"), f"val-{i}".encode("ascii"))
        for i in range(100)
    ]
    meta = SSTableWriter.write(sst_file, entries, index_interval=8)

    assert meta.entry_count == 100
    assert meta.min_key == b"key-0000"
    assert meta.max_key == b"key-0099"

    reader = SSTableReader(sst_file)
    assert reader.entry_count == 100

    # Test point reads
    found = reader.get(b"key-0042")
    assert found is not None
    assert found.value == b"val-42"

    # Test Bloom filter early rejection on absent key
    assert reader.get(b"non-existent-key") is None


def test_sstable_scan_range(tmp_path: Path):
    sst_file = tmp_path / "table_scan.sst"
    entries = [
        Entry.put(f"acc-{i:03d}".encode("ascii"), f"bal-{i}".encode("ascii"))
        for i in range(50)
    ]
    SSTableWriter.write(sst_file, entries, index_interval=5)

    reader = SSTableReader(sst_file)
    scanned = list(reader.scan(start_key=b"acc-010", end_key=b"acc-020"))
    assert len(scanned) == 10
    assert scanned[0].key == b"acc-010"
    assert scanned[-1].key == b"acc-019"
