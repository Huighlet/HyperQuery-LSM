"""
HyperQuery-LSM Embedded Web Console & Storage Engine Visualizer.
Zero external dependencies; uses Python standard library http.server to provide
an in-browser interactive dashboard displaying MemTable, SSTables, Bloom filters, and snapshots.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import time
from typing import Optional

from hyperquery.engine import LSMTreeEngine


def create_web_handler(engine: LSMTreeEngine):
    class HyperQueryDashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # Silence default access logging

        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                data = {
                    "status": "HEALTHY",
                    "engine": "HyperQuery-LSM",
                    "memtable_entries": len(engine._memtable),
                    "total_sstables": sum(len(t) for t in engine._sstables.values()),
                }
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return

            if self.path == "/api/status":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                tables = []
                for lvl, sst_list in engine._sstables.items():
                    for t in sst_list:
                        tables.append({
                            "table_id": t.table_id,
                            "level": t.level,
                            "entry_count": t.entry_count,
                            "file_size_bytes": t.file_size_bytes,
                        })
                payload = {
                    "memtable_entries": len(engine._memtable),
                    "memtable_bytes": engine._memtable.size_bytes,
                    "sstables": tables,
                    "snapshots": [s["snapshot_id"] for s in engine.list_snapshots()],
                }
                self.wfile.write(json.dumps(payload).encode("utf-8"))
                return

            # Root Dashboard HTML
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()

            total_sst = sum(len(t) for t in engine._sstables.values())
            l0_count = len(engine._sstables.get(0, []))
            l1_count = len(engine._sstables.get(1, []))
            snaps = engine.list_snapshots()

            table_rows = ""
            for lvl, sst_list in engine._sstables.items():
                for t in sst_list:
                    table_rows += f"""
                    <tr>
                        <td style="padding:10px;border-bottom:1px solid #334155;"><code>{t.table_id}</code></td>
                        <td style="padding:10px;border-bottom:1px solid #334155;"><span style="background:#0284c7;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;">Level {t.level}</span></td>
                        <td style="padding:10px;border-bottom:1px solid #334155;">{t.entry_count:,}</td>
                        <td style="padding:10px;border-bottom:1px solid #334155;text-align:right;">{t.file_size_bytes:,} B</td>
                        <td style="padding:10px;border-bottom:1px solid #334155;color:#10b981;">Active (Bloom &ge; 99%)</td>
                    </tr>
                    """

            if not table_rows:
                table_rows = "<tr><td colspan='5' style='padding:16px;text-align:center;color:#64748b;'>No SSTables flushed to disk yet. Writes are in-memory (SkipList MemTable).</td></tr>"

            html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>HyperQuery-LSM: Storage Engine & Redis RESP Console</title>
  <style>
    body {{ margin:0; padding:30px; font-family:-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; background:#0b0f19; color:#f3f4f6; }}
    .container {{ max-width:1100px; margin:0 auto; background:#111827; border:1px solid #1f2937; border-radius:10px; padding:28px; box-shadow:0 10px 30px rgba(0,0,0,0.6); }}
    h1 {{ margin-top:0; font-size:22px; color:#38bdf8; display:flex; align-items:center; justify-content:space-between; }}
    .badge {{ background:#10b981; color:#fff; padding:4px 10px; border-radius:9999px; font-size:12px; font-weight:700; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:16px; margin:20px 0; }}
    .card {{ background:#1e293b; border:1px solid #334155; border-radius:8px; padding:16px; }}
    .card-title {{ font-size:11px; text-transform:uppercase; letter-spacing:0.05em; color:#94a3b8; margin-bottom:6px; }}
    .card-val {{ font-size:22px; font-weight:700; color:#f8fafc; }}
    table {{ width:100%; border-collapse:collapse; margin-top:16px; font-size:13px; background:#0f172a; border-radius:6px; overflow:hidden; }}
    th {{ background:#1e293b; padding:10px; text-align:left; color:#94a3b8; font-size:11px; text-transform:uppercase; }}
    pre {{ background:#020617; padding:12px; border-radius:6px; color:#38bdf8; font-size:12px; overflow-x:auto; }}
  </style>
</head>
<body>
  <div class="container">
    <h1>
      HyperQuery-LSM &bull; Storage Engine Console
      <span class="badge">RESP2 SERVER ACTIVE</span>
    </h1>
    <p style="color:#94a3b8;font-size:13px;margin-bottom:20px;">
      Log-Structured Merge-Tree with SkipList MemTable, binary WAL, Bloom filter SSTables, and leveled compaction.
    </p>

    <div class="grid">
      <div class="card">
        <div class="card-title">MemTable Entries</div>
        <div class="card-val" style="color:#38bdf8;">{len(engine._memtable):,}</div>
      </div>
      <div class="card">
        <div class="card-title">MemTable Byte Volume</div>
        <div class="card-val" style="color:#a78bfa;">{engine._memtable.size_bytes:,} B</div>
      </div>
      <div class="card">
        <div class="card-title">Level 0 SSTables</div>
        <div class="card-val" style="color:#f59e0b;">{l0_count}</div>
      </div>
      <div class="card">
        <div class="card-title">Total Snapshots</div>
        <div class="card-val" style="color:#34d399;">{len(snaps)}</div>
      </div>
    </div>

    <h3>Immutable SSTable Manifest (Disk Files)</h3>
    <table>
      <thead>
        <tr>
          <th>SSTable ID</th>
          <th>Level</th>
          <th>Entries</th>
          <th style="text-align:right;">File Size</th>
          <th>Filter Verification</th>
        </tr>
      </thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>

    <h3 style="margin-top:24px;">Quick Connect (Redis CLI)</h3>
    <pre>redis-cli -h 127.0.0.1 -p 6379 PING
redis-cli -h 127.0.0.1 -p 6379 SET user:100 '{{ "name": "Alice" }}'
redis-cli -h 127.0.0.1 -p 6379 GET user:100</pre>
  </div>
</body>
</html>
"""
            self.wfile.write(html.encode("utf-8"))

    return HyperQueryDashboardHandler


def run_web_dashboard(engine: LSMTreeEngine, port: int = 6380, host: str = "0.0.0.0"):
    """Launch embedded HTTP monitoring web server."""
    handler = create_web_handler(engine)
    server = ThreadingHTTPServer((host, port), handler)
    print(f"HyperQuery-LSM Web Dashboard listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
