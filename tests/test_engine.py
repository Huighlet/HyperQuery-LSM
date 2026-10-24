"""
Tests for unified LSMTreeEngine operations, automatic flushing, and multi-level queries.
"""

from pathlib import Path
import pytest
from hyperquery.engine import LSMTreeEngine


def test_engine_put_get_delete(tmp_path: Path):
    engine = LSMTreeEngine(tmp_path, memtable_max_entries=10)

    # 1. Put and Get
    engine.put(b"user:101", b'{"name":"Alice"}')
    assert engine.get(b"user:101") == b'{"name":"Alice"}'

    # 2. Update
    engine.put(b"user:101", b'{"name":"Alice","role":"Admin"}')
    assert engine.get(b"user:101") == b'{"name":"Alice","role":"Admin"}'

    # 3. Delete
    engine.delete(b"user:101")
    assert engine.get(b"user:101") is None
    engine.close()


def test_engine_automatic_flush_to_sstable(tmp_path: Path):
    # max_entries = 5 so it triggers flush after 5 writes
    engine = LSMTreeEngine(tmp_path, memtable_max_entries=5)

    for i in range(12):
        engine.put(f"key-{i:02d}".encode(), f"val-{i}".encode())

    # Should have flushed at least twice into SSTables
    l0_tables = engine._sstables.get(0, [])
    assert len(l0_tables) >= 2

    # Verify all keys readable across both MemTable and SSTables
    for i in range(12):
        assert engine.get(f"key-{i:02d}".encode()) == f"val-{i}".encode()

    engine.close()


def test_engine_range_scan(tmp_path: Path):
    engine = LSMTreeEngine(tmp_path, memtable_max_entries=4)

    for i in range(10):
        engine.put(f"item-{i:02d}".encode(), f"val-{i}".encode())

    # Range scan [item-03..item-07)
    scanned = engine.scan(start_key=b"item-03", end_key=b"item-07")
    assert len(scanned) == 4
    assert [k for k, v in scanned] == [b"item-03", b"item-04", b"item-05", b"item-06"]

    engine.close()


def test_engine_compaction(tmp_path: Path):
    # level0_compaction_threshold = 2
    engine = LSMTreeEngine(tmp_path, memtable_max_entries=3, level0_compaction_threshold=2)

    # 8 writes with threshold=3 causes multiple flushes and auto-compaction
    for i in range(8):
        engine.put(f"k-{i}".encode(), f"v-{i}".encode())

    # Verify reads work seamlessly after compaction
    for i in range(8):
        assert engine.get(f"k-{i}".encode()) == f"v-{i}".encode()

    engine.close()
