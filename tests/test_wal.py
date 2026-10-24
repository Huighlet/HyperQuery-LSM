"""
Tests for Write-Ahead Log (WAL) durability, CRC32 checks, and crash replay.
"""

from pathlib import Path
import pytest
from hyperquery.types import CorruptedWALError, Entry
from hyperquery.wal import WriteAheadLog


def test_wal_append_and_replay(tmp_path: Path):
    wal_file = tmp_path / "test.wal"
    wal = WriteAheadLog(wal_file)

    e1 = Entry.put(b"k1", b"val1", timestamp_ns=100)
    e2 = Entry.put(b"k2", b"val2", timestamp_ns=200)
    e3 = Entry.delete(b"k3", timestamp_ns=300)

    wal.append(e1)
    wal.append(e2)
    wal.append(e3)
    wal.close()

    # Replay
    recovered = WriteAheadLog.replay(wal_file)
    assert len(recovered) == 3
    assert recovered[0].key == b"k1"
    assert recovered[0].value == b"val1"
    assert recovered[1].key == b"k2"
    assert recovered[2].is_tombstone is True


def test_wal_corrupted_frame_detected(tmp_path: Path):
    wal_file = tmp_path / "corrupted.wal"
    wal = WriteAheadLog(wal_file)
    wal.append(Entry.put(b"key-valid", b"val-valid"))
    wal.close()

    # Tamper with byte payload
    data = bytearray(wal_file.read_bytes())
    data[-1] ^= 0xFF  # Flip bit in value
    wal_file.write_bytes(data)

    with pytest.raises(CorruptedWALError, match="CRC32 mismatch"):
        WriteAheadLog.replay(wal_file)
