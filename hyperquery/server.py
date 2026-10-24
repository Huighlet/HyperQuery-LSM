"""
HyperQuery-LSM: Redis-Compatible (RESP) High-Performance TCP Server.
Allows any standard Redis client (redis-cli, redis-py, ioredis, go-redis) to interact with the LSM storage engine.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional

from hyperquery.engine import LSMTreeEngine
from hyperquery.resp import RESPParser


class HyperQueryServer:
    """
    Asynchronous TCP server implementing the Redis RESP wire protocol.
    Backs all key-value operations with the HyperQuery Log-Structured Merge-Tree.
    """

    def __init__(
        self,
        engine: LSMTreeEngine,
        host: str = "0.0.0.0",
        port: int = 6379,
        requirepass: Optional[str] = None,
    ) -> None:
        self.engine = engine
        self.host = host
        self.port = port
        self.requirepass = requirepass or os.getenv("HYPERQUERY_AUTH_PASSWORD")
        self._server: Optional[asyncio.Server] = None
        self._start_time = time.time()

        # Telemetry metrics
        self.total_commands = 0
        self.total_puts = 0
        self.total_gets = 0
        self.total_dels = 0
        self.total_scans = 0

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle continuous client connection stream."""
        buffer = bytearray()
        client_state = {"authenticated": (self.requirepass is None)}
        try:
            while not reader.at_eof():
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buffer.extend(chunk)

                # Parse and execute all complete commands in buffer
                while True:
                    tokens, remaining = RESPParser.parse_request(bytes(buffer))
                    if tokens is None:
                        break
                    buffer = bytearray(remaining)

                    response = self.execute_command(tokens, client_state=client_state)
                    writer.write(response)
                    await writer.drain()
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def execute_command(
        self,
        tokens: list[bytes],
        client_state: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """Dispatch command tokens to LSM storage engine."""
        if not tokens:
            return RESPParser.serialize_error("Empty command")

        self.total_commands += 1
        cmd = tokens[0].decode("ascii", errors="replace").upper()
        args = tokens[1:]

        # Authentication check
        if self.requirepass is not None and client_state is not None:
            if not client_state.get("authenticated", False):
                if cmd == "AUTH":
                    if len(args) >= 1 and args[0].decode("utf-8", errors="replace") == self.requirepass:
                        client_state["authenticated"] = True
                        return RESPParser.serialize_simple_string("OK")
                    return RESPParser.serialize_error("invalid password")
                elif cmd in ("QUIT",):
                    return RESPParser.serialize_simple_string("OK")
                else:
                    return RESPParser.serialize_error("NOAUTH Authentication required.")

        if cmd == "AUTH":
            if self.requirepass is not None and len(args) >= 1:
                if args[0].decode("utf-8", errors="replace") == self.requirepass:
                    if client_state is not None:
                        client_state["authenticated"] = True
                    return RESPParser.serialize_simple_string("OK")
                return RESPParser.serialize_error("invalid password")
            return RESPParser.serialize_simple_string("OK")

        elif cmd == "PING":
            msg = args[0].decode("utf-8", errors="replace") if args else "PONG"
            return RESPParser.serialize_simple_string(msg)

        elif cmd == "SET":
            if len(args) < 2:
                return RESPParser.serialize_error("wrong number of arguments for 'SET'")
            self.total_puts += 1
            key = args[0]
            val = args[1]
            self.engine.put(key, val)
            return RESPParser.serialize_simple_string("OK")

        elif cmd == "GET":
            if len(args) < 1:
                return RESPParser.serialize_error("wrong number of arguments for 'GET'")
            self.total_gets += 1
            key = args[0]
            val = self.engine.get(key)
            return RESPParser.serialize_bulk_string(val)

        elif cmd == "DEL":
            if len(args) < 1:
                return RESPParser.serialize_error("wrong number of arguments for 'DEL'")
            self.total_dels += len(args)
            for k in args:
                self.engine.delete(k)
            return RESPParser.serialize_integer(len(args))

        elif cmd == "SCAN":
            self.total_scans += 1
            start_k = args[0] if len(args) > 0 else None
            end_k = args[1] if len(args) > 1 else None
            results = self.engine.scan(start_key=start_k, end_key=end_k)
            # Serialize as alternating key, value array
            items = []
            for k, v in results:
                items.append(RESPParser.serialize_bulk_string(k))
                items.append(RESPParser.serialize_bulk_string(v))
            return RESPParser.serialize_array(items)

        elif cmd in ("FLUSHDB", "FLUSHALL"):
            self.engine.flush_memtable()
            return RESPParser.serialize_simple_string("OK")

        elif cmd in ("SNAPSHOT", "BGSAVE"):
            snap = self.engine.create_snapshot()
            return RESPParser.serialize_simple_string(f"OK SNAPSHOT CREATED {snap['snapshot_id']}")

        elif cmd == "SNAPSHOTS":
            snaps = self.engine.list_snapshots()
            items = [RESPParser.serialize_bulk_string(s["snapshot_id"].encode("utf-8")) for s in snaps]
            return RESPParser.serialize_array(items)

        elif cmd == "METRICS":
            total_sstables = sum(len(tables) for tables in self.engine._sstables.values())
            metrics_lines = [
                "# HELP hyperquery_commands_total Total commands processed",
                "# TYPE hyperquery_commands_total counter",
                f"hyperquery_commands_total {self.total_commands}",
                "# HELP hyperquery_puts_total Total put operations",
                "# TYPE hyperquery_puts_total counter",
                f"hyperquery_puts_total {self.total_puts}",
                "# HELP hyperquery_gets_total Total get operations",
                "# TYPE hyperquery_gets_total counter",
                f"hyperquery_gets_total {self.total_gets}",
                "# HELP hyperquery_dels_total Total delete operations",
                "# TYPE hyperquery_dels_total counter",
                f"hyperquery_dels_total {self.total_dels}",
                "# HELP hyperquery_scans_total Total range scan operations",
                "# TYPE hyperquery_scans_total counter",
                f"hyperquery_scans_total {self.total_scans}",
                "# HELP hyperquery_memtable_entries In-memory skip list entries",
                "# TYPE hyperquery_memtable_entries gauge",
                f"hyperquery_memtable_entries {len(self.engine._memtable)}",
                "# HELP hyperquery_memtable_bytes In-memory skip list byte size",
                "# TYPE hyperquery_memtable_bytes gauge",
                f"hyperquery_memtable_bytes {self.engine._memtable.size_bytes}",
                "# HELP hyperquery_sstables_total Total immutable on-disk SSTables",
                "# TYPE hyperquery_sstables_total gauge",
                f"hyperquery_sstables_total {total_sstables}",
            ]
            metrics_payload = "\n".join(metrics_lines) + "\n"
            return RESPParser.serialize_bulk_string(metrics_payload.encode("utf-8"))

        elif cmd == "INFO":
            total_sstables = sum(len(tables) for tables in self.engine._sstables.values())
            uptime = int(time.time() - self._start_time)
            info_text = (
                "# HyperQuery-LSM Storage Engine\r\n"
                "engine:hyperquery-lsm\r\n"
                "version:1.0.0\r\n"
                f"uptime_seconds:{uptime}\r\n"
                f"total_commands:{self.total_commands}\r\n"
                f"total_puts:{self.total_puts}\r\n"
                f"total_gets:{self.total_gets}\r\n"
                f"total_dels:{self.total_dels}\r\n"
                f"total_scans:{self.total_scans}\r\n"
                f"memtable_entries:{len(self.engine._memtable)}\r\n"
                f"memtable_bytes:{self.engine._memtable.size_bytes}\r\n"
                f"level0_tables:{len(self.engine._sstables.get(0, []))}\r\n"
                f"level1_tables:{len(self.engine._sstables.get(1, []))}\r\n"
                f"total_sstables:{total_sstables}\r\n"
            )
            return RESPParser.serialize_bulk_string(info_text.encode("utf-8"))

        elif cmd == "COMMAND":
            # Standard Redis handshake command from modern GUI clients
            return RESPParser.serialize_array([])

        else:
            return RESPParser.serialize_error(f"unknown command '{cmd}'")

    async def start(self) -> None:
        """Start TCP socket server."""
        self._server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print("=" * 60)
        print(f"HyperQuery-LSM Redis Protocol Server listening on {self.host}:{self.port}")
        print("Compatible with standard redis-cli, redis-py, ioredis, and go-redis")
        print(f"Data directory: {self.engine.data_dir}")
        print("=" * 60)
        async with self._server:
            await self._server.serve_forever()

    def stop(self) -> None:
        """Close server socket."""
        if self._server:
            self._server.close()
