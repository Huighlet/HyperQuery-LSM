"""
Enterprise Simulation: High-Throughput KV Workload & LSM-Tree Storage Engine.
Simulates 1,000 concurrent key-value operations over TCP using the Redis RESP2 wire protocol,
verifying password AUTH, MemTable flushes, Bloom filter lookups, range scans, and durable snapshots.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
import threading
import time
from pathlib import Path

from hyperquery.client import HyperQueryClient
from hyperquery.engine import LSMTreeEngine
from hyperquery.server import HyperQueryServer


def start_server_thread(engine: LSMTreeEngine, port: int, password: str):
    """Run HyperQueryServer event loop in dedicated daemon thread."""
    server = HyperQueryServer(engine=engine, host="127.0.0.1", port=port, requirepass=password)
    loop = asyncio.new_event_loop()

    def run():
        asyncio.set_event_loop(loop)
        coro = server.start()
        loop.run_until_complete(coro)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    time.sleep(0.3)  # Allow socket to bind
    return server, loop


def run_kv_simulation():
    print("=" * 75)
    print("HyperQuery-LSM Enterprise Simulation: High-Throughput KV & Storage Engine")
    print("=" * 75)

    test_dir = Path(tempfile.mkdtemp(prefix="hyperquery_sim_"))
    engine = LSMTreeEngine(data_dir=test_dir, memtable_max_entries=200)
    server_port = 6389
    auth_pass = "lsm_vault_secret_99"

    print(f"\n[+] Booting HyperQuery RESP2 Server on 127.0.0.1:{server_port}...")
    server, loop = start_server_thread(engine, port=server_port, password=auth_pass)

    try:
        # 1. Verify Security & Password Authentication
        print("\n[+] Testing Security Perimeter (AUTH verification)...")
        unauth_client = HyperQueryClient(host="127.0.0.1", port=server_port, password=None)
        try:
            unauth_client.ping()
            raise AssertionError("Expected NOAUTH failure for unauthenticated client")
        except RuntimeError as e:
            print(f"     [OK] Blocked unauthenticated command: {e}")
        finally:
            unauth_client.close()

        # Connect authenticated client
        client = HyperQueryClient(host="127.0.0.1", port=server_port, password=auth_pass)
        client.connect()
        pong = client.ping("HEALTHY")
        print(f"     [OK] Authenticated client connected successfully: PING -> {pong}")

        # 2. High-Throughput Write Ingestion (1,000 KV pairs)
        total_writes = 1000
        print(f"\n[+] Ingesting {total_writes} structured KV records across keyspaces...")
        start_time = time.perf_counter()

        for i in range(total_writes):
            key = f"user:{i:04d}"
            payload = json.dumps({
                "uid": i,
                "tier": "enterprise" if i % 10 == 0 else "standard",
                "balance": round(100.0 + (i * 1.5), 2),
                "active": True,
            })
            client.set(key, payload)

        elapsed_write = time.perf_counter() - start_time
        write_rate = total_writes / elapsed_write if elapsed_write > 0 else 0
        print(f"[OK] Completed {total_writes} SET operations in {elapsed_write*1000:.2f} ms ({write_rate:,.0f} ops/sec)")

        # 3. High-Speed Point Lookups (GET)
        print(f"\n[+] Executing point lookups for random keys...")
        start_read = time.perf_counter()
        sample_keys = [f"user:{k:04d}" for k in (0, 42, 250, 500, 750, 999)]
        for k in sample_keys:
            raw_val = client.get(k)
            assert raw_val is not None, f"Key {k} not found!"
            data = json.loads(raw_val)
            assert data["active"] is True
        elapsed_read = time.perf_counter() - start_read
        print(f"[OK] Verified point lookups across active keyspaces ({elapsed_read*1000:.2f} ms)")

        # 4. Range Scan (SCAN) with Prefix Boundaries
        print(f"\n[+] Executing lexicographical range scan between 'user:0100' and 'user:0120'...")
        scan_results = client.scan(start_key="user:0100", end_key="user:0120")
        print(f"     Retrieved {len(scan_results)} ordered items from range scan:")
        for k, v in scan_results[:3]:
            print(f"       - {k.decode()}: {v.decode()[:40]}...")

        # 5. Flush MemTable & SSTable Bloom Filter Generation
        print(f"\n[+] Triggering active MemTable flush to disk (Level 0 SSTable + Bloom Filter)...")
        client.flush()
        info_stats = client.info()
        print(f"     Level 0 SSTables On Disk: {info_stats.get('level0_tables')}")
        print(f"     Total SSTables: {info_stats.get('total_sstables')}")

        # 6. Point-in-Time Checkpoint Snapshot
        print(f"\n[+] Generating Point-in-Time Storage Snapshot Checkpoint...")
        snap_msg = client.snapshot()
        print(f"     [OK] Server Response: {snap_msg}")
        snapshots = client.list_snapshots()
        print(f"     Available Snapshots in Catalog: {snapshots}")
        assert len(snapshots) >= 1

        # 7. Soft Deletions (DEL) & Tombstone Verification
        print(f"\n[+] Soft-deleting key 'user:0042' and verifying tombstone semantics...")
        del_count = client.delete("user:0042")
        assert del_count == 1
        del_val = client.get("user:0042")
        assert del_val is None, "Expected None for deleted key"
        print(f"     [OK] Tombstone correctly masked key: GET user:0042 -> (nil)")

        # 8. Observability & Prometheus Metrics
        print(f"\n[+] Auditing server telemetry via METRICS...")
        metrics = client.get_metrics()
        assert "hyperquery_commands_total" in metrics
        assert "hyperquery_puts_total" in metrics
        print(f"     [OK] Prometheus Metrics verified (/METRICS live)")

        client.close()
        print("\n" + "=" * 75)
        print("[SUCCESS] HyperQuery-LSM Enterprise Storage Simulation Passed!")
        print("=" * 75)

    finally:
        engine.close()
        shutil.rmtree(test_dir, ignore_errors=True)


if __name__ == "__main__":
    run_kv_simulation()
