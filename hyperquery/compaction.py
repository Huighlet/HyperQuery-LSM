"""
HyperQuery-LSM: Multi-Way Leveled Compaction & Tombstone Garbage Collection.
Merges overlapping SSTable runs to bound read amplification and reclaim disk space.
"""

from __future__ import annotations

import heapq
from pathlib import Path
import time
from typing import Dict, Iterator, List, Tuple

from hyperquery.sstable import SSTableMetadata, SSTableReader, SSTableWriter
from hyperquery.types import CompactionStats, Entry


class Compactor:
    """
    Executes K-way sorted merge compaction across SSTable files.
    Deduplicates keys by preserving newest timestamp and purges dead tombstones.
    """

    @staticmethod
    def compact_level(
        input_tables: List[SSTableMetadata],
        output_dir: Path | str,
        target_level: int,
        is_bottom_level: bool = False,
    ) -> Tuple[List[SSTableMetadata], CompactionStats]:
        """
        Merge input SSTables into one or more new SSTables at target_level.
        """
        t0 = time.perf_counter()
        if not input_tables:
            return [], CompactionStats(
                level_from=0,
                level_to=target_level,
                input_tables=0,
                output_tables=0,
                entries_processed=0,
                tombstones_purged=0,
                duration_sec=0.0,
            )

        readers = [SSTableReader(t.file_path) for t in input_tables]

        # Multi-way merge: group entries by key
        # Priority queue item: (key, -timestamp_ns, reader_idx, entry)
        heap: List[Tuple[bytes, int, int, Entry]] = []
        iterators: List[Iterator[Entry]] = [r.scan() for r in readers]

        for idx, it in enumerate(iterators):
            try:
                first_entry = next(it)
                heapq.heappush(heap, (first_entry.key, -first_entry.timestamp_ns, idx, first_entry))
            except StopIteration:
                pass

        merged_entries: List[Entry] = []
        last_key: bytes | None = None
        tombstones_purged = 0
        total_processed = 0

        while heap:
            k, neg_ts, idx, entry = heapq.heappop(heap)
            total_processed += 1

            # Advance iterator that yielded this entry
            try:
                nxt = next(iterators[idx])
                heapq.heappush(heap, (nxt.key, -nxt.timestamp_ns, idx, nxt))
            except StopIteration:
                pass

            # If we've already selected the latest version of this key, discard older versions
            if k == last_key:
                continue

            last_key = k

            # If it's a tombstone and this is bottom level, purge it!
            if entry.is_tombstone and is_bottom_level:
                tombstones_purged += 1
                continue

            merged_entries.append(entry)

        # Write output SSTables
        out_path_dir = Path(output_dir)
        out_path_dir.mkdir(parents=True, exist_ok=True)
        new_tables: List[SSTableMetadata] = []

        if merged_entries:
            table_filename = f"sstable_L{target_level}_{time.time_ns()}.sst"
            out_file = out_path_dir / table_filename
            meta = SSTableWriter.write(
                file_path=out_file,
                entries=merged_entries,
                level=target_level,
            )
            new_tables.append(meta)

        # Delete old compacted input files
        for t in input_tables:
            try:
                Path(t.file_path).unlink(missing_ok=True)
            except Exception:
                pass

        dur = time.perf_counter() - t0
        stats = CompactionStats(
            level_from=input_tables[0].level if input_tables else 0,
            level_to=target_level,
            input_tables=len(input_tables),
            output_tables=len(new_tables),
            entries_processed=total_processed,
            tombstones_purged=tombstones_purged,
            duration_sec=dur,
        )

        return new_tables, stats
