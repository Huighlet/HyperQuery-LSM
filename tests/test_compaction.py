"""
Tests for Compactor multi-way merge, key deduplication, and tombstone purging.
"""

from pathlib import Path
import pytest
from hyperquery.compaction import Compactor
from hyperquery.sstable import SSTableReader, SSTableWriter
from hyperquery.types import Entry


def test_compaction_multiway_merge_and_deduplication(tmp_path: Path):
    out_dir = tmp_path / "compact"

    # SSTable 1: older versions of keys
    e1 = [
        Entry.put(b"k1", b"v1_old", timestamp_ns=100),
        Entry.put(b"k2", b"v2_old", timestamp_ns=100),
        Entry.put(b"k3", b"v3_only", timestamp_ns=100),
    ]
    t1_path = tmp_path / "t1.sst"
    m1 = SSTableWriter.write(t1_path, e1, level=0)

    # SSTable 2: newer versions of keys + tombstone
    e2 = [
        Entry.put(b"k1", b"v1_new", timestamp_ns=200),  # updated
        Entry.delete(b"k2", timestamp_ns=200),          # deleted
        Entry.put(b"k4", b"v4_only", timestamp_ns=200), # new
    ]
    t2_path = tmp_path / "t2.sst"
    m2 = SSTableWriter.write(t2_path, e2, level=0)

    # Compact to bottom level (Level 1)
    new_tables, stats = Compactor.compact_level(
        input_tables=[m1, m2],
        output_dir=out_dir,
        target_level=1,
        is_bottom_level=True,
    )

    assert len(new_tables) == 1
    assert stats.tombstones_purged == 1  # k2 tombstone purged!

    reader = SSTableReader(new_tables[0].file_path)
    entries = list(reader.scan())
    keys = [e.key for e in entries]

    # k2 must be completely absent (purged)
    assert keys == [b"k1", b"k3", b"k4"]
    # k1 must have newest value
    assert reader.get(b"k1").value == b"v1_new"
