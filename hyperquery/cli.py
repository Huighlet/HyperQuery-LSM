"""
HyperQuery-LSM: Command Line Interface & Microsecond Storage Engine Benchmark Suite.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from pathlib import Path

from hyperquery.bloom import BloomFilter
from hyperquery.engine import LSMTreeEngine
from hyperquery.skiplist import SkipList
from hyperquery.sstable import SSTableReader, SSTableWriter
from hyperquery.types import Entry


def run_benchmark(iterations: int = 50000) -> None:
    """Execute microsecond benchmarks across LSM storage components."""
    print("=" * 76)
    print("HyperQuery-LSM: Microsecond Storage Engine Benchmark Suite")
    print(f"Iterations: {iterations:,} operations per test")
    print("=" * 76)

    # 1. SkipList In-Memory Ordered Insertions
    skiplist = SkipList()
    sample_entries = [
        Entry.put(f"key-{i:08d}".encode("ascii"), f"val-{i}".encode("ascii"))
        for i in range(iterations)
    ]

    t0 = time.perf_counter()
    for entry in sample_entries:
        skiplist.insert(entry)
    t1 = time.perf_counter()
    dur_skip_put = t1 - t0
    ops_skip_put = iterations / dur_skip_put
    lat_skip_put_us = (dur_skip_put / iterations) * 1_000_000

    print(f"[1] SkipList In-Memory Ordered Insertion:")
    print(f"    Throughput: {ops_skip_put:12,.1f} inserts/sec")
    print(f"    Latency:    {lat_skip_put_us:12.3f} us/insert\n")

    # 2. SkipList Point Read
    t0 = time.perf_counter()
    for i in range(iterations):
        skiplist.get(f"key-{i:08d}".encode("ascii"))
    t1 = time.perf_counter()
    dur_skip_get = t1 - t0
    ops_skip_get = iterations / dur_skip_get
    lat_skip_get_us = (dur_skip_get / iterations) * 1_000_000

    print(f"[2] SkipList In-Memory Point Lookup:")
    print(f"    Throughput: {ops_skip_get:12,.1f} lookups/sec")
    print(f"    Latency:    {lat_skip_get_us:12.3f} us/lookup\n")

    # 3. Bloom Filter Membership Verification
    bloom = BloomFilter(expected_elements=iterations, false_positive_rate=0.01)
    for entry in sample_entries:
        bloom.add(entry.key)

    t0 = time.perf_counter()
    for entry in sample_entries:
        bloom.contains(entry.key)
    t1 = time.perf_counter()
    dur_bloom = t1 - t0
    ops_bloom = iterations / dur_bloom
    lat_bloom_us = (dur_bloom / iterations) * 1_000_000

    print(f"[3] Bloom Filter Double-Hashing Query:")
    print(f"    Throughput: {ops_bloom:12,.1f} checks/sec")
    print(f"    Latency:    {lat_bloom_us:12.3f} us/check\n")

    # 4. SSTable Point Read with Sparse Index & Bloom Filter
    temp_dir = Path(tempfile.mkdtemp())
    try:
        sst_path = temp_dir / "bench.sst"
        SSTableWriter.write(sst_path, sample_entries, index_interval=16)
        reader = SSTableReader(sst_path)

        # Existing key reads
        t0 = time.perf_counter()
        for i in range(min(iterations, 25000)):
            reader.get(f"key-{i:08d}".encode("ascii"))
        t1 = time.perf_counter()
        dur_sst_get = t1 - t0
        ops_sst_get = min(iterations, 25000) / dur_sst_get
        lat_sst_get_us = (dur_sst_get / min(iterations, 25000)) * 1_000_000

        print(f"[4] SSTable Disk Read (Existing Keys via Sparse Index):")
        print(f"    Throughput: {ops_sst_get:12,.1f} lookups/sec")
        print(f"    Latency:    {lat_sst_get_us:12.3f} us/lookup\n")

        # Negative key reads (Bloom filter rejection)
        t0 = time.perf_counter()
        for i in range(iterations):
            reader.get(f"absent-{i}".encode("ascii"))
        t1 = time.perf_counter()
        dur_bloom_neg = t1 - t0
        ops_bloom_neg = iterations / dur_bloom_neg
        lat_bloom_neg_us = (dur_bloom_neg / iterations) * 1_000_000

        print(f"[5] SSTable Negative Read (100% Bloom Filter Bypass):")
        print(f"    Throughput: {ops_bloom_neg:12,.1f} lookups/sec")
        print(f"    Latency:    {lat_bloom_neg_us:12.3f} us/lookup\n")

        # 5. Full LSMTreeEngine Write Path (WAL + MemTable + Auto-Flush)
        lsm_dir = temp_dir / "lsm_bench"
        engine = LSMTreeEngine(lsm_dir, memtable_max_entries=5000)

        t0 = time.perf_counter()
        for i in range(min(iterations, 20000)):
            engine.put(f"user-{i}".encode("ascii"), b"profile-data-blob")
        t1 = time.perf_counter()
        dur_lsm_put = t1 - t0
        ops_lsm_put = min(iterations, 20000) / dur_lsm_put
        lat_lsm_put_us = (dur_lsm_put / min(iterations, 20000)) * 1_000_000

        print(f"[6] Full LSM Engine Write (WAL + MemTable + Flushing):")
        print(f"    Throughput: {ops_lsm_put:12,.1f} writes/sec")
        print(f"    Latency:    {lat_lsm_put_us:12.3f} us/write")
        print("=" * 76)

        engine.close()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def cmd_server(host: str, port: int, data_dir: str, web_port: int | None = None) -> None:
    """Run Redis-compatible (RESP) TCP socket server with optional embedded Web Dashboard."""
    import asyncio
    import threading
    from hyperquery.server import HyperQueryServer
    from hyperquery.web import run_web_dashboard

    engine = LSMTreeEngine(data_dir=data_dir)

    if web_port:
        th = threading.Thread(target=run_web_dashboard, args=(engine, web_port, host), daemon=True)
        th.start()

    server = HyperQueryServer(engine=engine, host=host, port=port)
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\nShutting down HyperQuery server...")
        server.stop()
        engine.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="hyperquery",
        description="HyperQuery-LSM: Embedded Log-Structured Merge-Tree Storage Engine",
    )
    subparsers = parser.add_subparsers(dest="command")

    server_parser = subparsers.add_parser("server", help="Run Redis-compatible (RESP) TCP server")
    server_parser.add_argument("--host", default="0.0.0.0", help="Binding host address")
    server_parser.add_argument("--port", type=int, default=6379, help="Binding port (default: 6379)")
    server_parser.add_argument("--web-port", type=int, default=6380, help="Optional embedded HTTP dashboard port (default: 6380)")
    server_parser.add_argument("--data", default="./lsm_data", help="Directory for WAL and SSTables")

    bench_parser = subparsers.add_parser("bench", help="Run LSM engine microsecond benchmark suite")
    bench_parser.add_argument("-n", "--iterations", type=int, default=50000, help="Number of benchmark iterations")

    dashboard_parser = subparsers.add_parser("dashboard", help="Run standalone web monitoring dashboard")
    dashboard_parser.add_argument("--host", default="0.0.0.0", help="Binding host address")
    dashboard_parser.add_argument("--port", type=int, default=6380, help="Binding port (default: 6380)")
    dashboard_parser.add_argument("--data", default="./lsm_data", help="Directory for WAL and SSTables")

    args = parser.parse_args()
    if args.command == "server":
        cmd_server(args.host, args.port, args.data, args.web_port)
    elif args.command == "dashboard":
        from hyperquery.web import run_web_dashboard
        eng = LSMTreeEngine(data_dir=args.data)
        run_web_dashboard(eng, port=args.port, host=args.host)
    elif args.command == "bench":
        run_benchmark(iterations=args.iterations)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
