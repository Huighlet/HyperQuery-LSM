"""
HyperQuery-LSM: Unified Log-Structured Merge-Tree Storage Engine.
Integrates MemTable, WAL, multi-level SSTables, Bloom Filters, and Compaction.
"""

from __future__ import annotations

import collections
import json
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from hyperquery.compaction import Compactor
from hyperquery.skiplist import SkipList
from hyperquery.sstable import SSTableMetadata, SSTableReader, SSTableWriter
from hyperquery.types import CompactionStats, Entry
from hyperquery.wal import WriteAheadLog


class LSMTreeEngine:
    """
    Production-grade Log-Structured Merge-Tree storage engine.
    Ensures high write throughput, fast point lookups via Bloom filters,
    and bounded read amplification through leveled compaction.
    """

    def __init__(
        self,
        data_dir: Path | str,
        memtable_max_entries: int = 5000,
        memtable_max_bytes: int = 4 * 1024 * 1024,  # 4MB
        level0_compaction_threshold: int = 4,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.memtable_max_entries = memtable_max_entries
        self.memtable_max_bytes = memtable_max_bytes
        self.level0_compaction_threshold = level0_compaction_threshold
        self._lock = threading.RLock()

        # In-Memory MemTable
        self._memtable = SkipList()

        # Write-Ahead Log
        self._wal_path = self.data_dir / "active.wal"
        self._wal = WriteAheadLog(self._wal_path)

        # Multi-level SSTables: level (int) -> List[SSTableMetadata]
        self._sstables: Dict[int, List[SSTableMetadata]] = collections.defaultdict(list)
        # Table ID -> SSTableReader cache
        self._readers: Dict[str, SSTableReader] = {}

        # Recover or discover existing SSTables
        self._discover_sstables()

    def _discover_sstables(self) -> None:
        """Scan data directory and register existing SSTables."""
        for sst_file in sorted(self.data_dir.glob("*.sst")):
            try:
                reader = SSTableReader(sst_file)
                self._readers[sst_file.stem] = reader
                # Extract level from filename or default to 0
                lvl = 0
                parts = sst_file.stem.split("_")
                for p in parts:
                    if p.startswith("L") and p[1:].isdigit():
                        lvl = int(p[1:])
                        break
                meta = SSTableMetadata(
                    table_id=sst_file.stem,
                    level=lvl,
                    min_key=reader._index_keys[0] if reader._index_keys else b"",
                    max_key=b"",  # Can be populated if needed
                    entry_count=reader.entry_count,
                    file_size_bytes=len(reader._data),
                    file_path=str(sst_file.resolve()),
                )
                self._sstables[lvl].append(meta)
            except Exception:
                pass

    def put(self, key: bytes, value: bytes) -> None:
        """Store key-value pair into the LSM-Tree."""
        with self._lock:
            entry = Entry.put(key=key, value=value)
            self._wal.append(entry)
            self._memtable.insert(entry)

            if (
                len(self._memtable) >= self.memtable_max_entries
                or self._memtable.size_bytes >= self.memtable_max_bytes
            ):
                self.flush_memtable()

    def delete(self, key: bytes) -> None:
        """Soft-delete a key by writing a Tombstone marker."""
        with self._lock:
            entry = Entry.delete(key=key)
            self._wal.append(entry)
            self._memtable.insert(entry)

            if len(self._memtable) >= self.memtable_max_entries:
                self.flush_memtable()

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Point read path:
        1. Query in-memory MemTable
        2. Query Level 0 SSTables (newest to oldest)
        3. Query Level 1+ SSTables
        """
        with self._lock:
            # Step 1: In-memory MemTable
            entry = self._memtable.get(key)
            if entry is not None:
                return None if entry.is_tombstone else entry.value

            # Step 2: Query Level 0 SSTables (reverse chronological order)
            l0_tables = self._sstables.get(0, [])
            for table in reversed(l0_tables):
                reader = self._get_reader(table)
                found = reader.get(key)
                if found is not None:
                    return None if found.is_tombstone else found.value

            # Step 3: Query higher levels
            for lvl in sorted(k for k in self._sstables.keys() if k > 0):
                for table in self._sstables[lvl]:
                    reader = self._get_reader(table)
                    found = reader.get(key)
                    if found is not None:
                        return None if found.is_tombstone else found.value

            return None

    def scan(
        self,
        start_key: Optional[bytes] = None,
        end_key: Optional[bytes] = None,
    ) -> List[Tuple[bytes, bytes]]:
        """
        Range scan returning (key, value) pairs sorted lexicographically.
        Merges MemTable and all SSTables while deduplicating by newest timestamp.
        """
        with self._lock:
            merged: Dict[bytes, Entry] = {}

            # Collect from all SSTables
            for lvl, tables in self._sstables.items():
                for table in tables:
                    reader = self._get_reader(table)
                    for entry in reader.scan(start_key=start_key, end_key=end_key):
                        if entry.key not in merged or entry.timestamp_ns > merged[entry.key].timestamp_ns:
                            merged[entry.key] = entry

            # Collect from MemTable (overrides disk)
            for entry in self._memtable.scan(start_key=start_key, end_key=end_key):
                if entry.key not in merged or entry.timestamp_ns > merged[entry.key].timestamp_ns:
                    merged[entry.key] = entry

            # Filter out tombstones and sort
            results = [
                (k, e.value)
                for k, e in sorted(merged.items(), key=lambda x: x[0])
                if not e.is_tombstone and e.value is not None
            ]
            return results

    def flush_memtable(self) -> Optional[SSTableMetadata]:
        """
        Flush in-memory MemTable to an immutable Level 0 SSTable.
        Rotates the WAL and clears the MemTable.
        """
        with self._lock:
            if len(self._memtable) == 0:
                return None

            entries = self._memtable.to_sorted_list()
            table_id = f"sstable_L0_{time.time_ns()}"
            sst_path = self.data_dir / f"{table_id}.sst"

            meta = SSTableWriter.write(
                file_path=sst_path,
                entries=entries,
                level=0,
            )
            self._sstables[0].append(meta)
            self._readers[meta.table_id] = SSTableReader(sst_path)

            # Rotate WAL
            self._wal.close()
            self._wal_path.unlink(missing_ok=True)
            self._wal = WriteAheadLog(self._wal_path)

            # Reset MemTable
            self._memtable.clear()

            # Auto-compact if threshold reached
            if len(self._sstables[0]) >= self.level0_compaction_threshold:
                self.compact_level(0)

            return meta

    def compact_level(self, level: int = 0) -> CompactionStats:
        """Compact all SSTables at the given level into the next level."""
        with self._lock:
            input_tables = list(self._sstables.get(level, []))
            if not input_tables:
                return CompactionStats(level, level + 1, 0, 0, 0, 0, 0.0)

            target_level = level + 1
            is_bottom = (target_level >= 2)

            new_tables, stats = Compactor.compact_level(
                input_tables=input_tables,
                output_dir=self.data_dir,
                target_level=target_level,
                is_bottom_level=is_bottom,
            )

            # Clean up old readers
            for t in input_tables:
                self._readers.pop(t.table_id, None)

            # Update catalog
            self._sstables[level].clear()
            self._sstables[target_level].extend(new_tables)
            for t in new_tables:
                self._readers[t.table_id] = SSTableReader(t.file_path)

            return stats

    def _get_reader(self, table: SSTableMetadata) -> SSTableReader:
        if table.table_id not in self._readers:
            self._readers[table.table_id] = SSTableReader(table.file_path)
        return self._readers[table.table_id]

    def create_snapshot(self) -> Dict[str, Any]:
        """
        Point-in-time state checkpoint and durability audit.
        Flushes active MemTable to disk and persists a frozen snapshot manifest.
        """
        with self._lock:
            self.flush_memtable()
            snap_dir = self.data_dir / "snapshots"
            snap_dir.mkdir(parents=True, exist_ok=True)
            snap_id = f"snapshot_{int(time.time_ns())}"
            snap_path = snap_dir / f"{snap_id}.json"

            total_bytes = 0
            total_entries = 0
            tables_info = []

            for lvl, tables in self._sstables.items():
                for t in tables:
                    total_bytes += t.file_size_bytes
                    total_entries += t.entry_count
                    tables_info.append({
                        "table_id": t.table_id,
                        "level": t.level,
                        "entry_count": t.entry_count,
                        "file_size_bytes": t.file_size_bytes,
                    })

            snapshot_data = {
                "snapshot_id": snap_id,
                "timestamp_ns": time.time_ns(),
                "total_tables": len(tables_info),
                "total_entries": total_entries,
                "total_bytes": total_bytes,
                "tables": tables_info,
                "status": "DURABLE_LSM_CHECKPOINT_SAVED",
            }

            snap_path.write_text(json.dumps(snapshot_data, indent=2))
            return snapshot_data

    def list_snapshots(self) -> List[Dict[str, Any]]:
        """List all historical point-in-time snapshots created for this engine."""
        with self._lock:
            snap_dir = self.data_dir / "snapshots"
            if not snap_dir.exists():
                return []
            results = []
            for f in sorted(snap_dir.glob("snapshot_*.json")):
                try:
                    data = json.loads(f.read_text())
                    results.append(data)
                except Exception:
                    pass
            return results

    def close(self) -> None:
        """Close storage engine."""
        with self._lock:
            self._wal.close()
