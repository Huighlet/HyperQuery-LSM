"""
HyperQuery-LSM: Redis-Compatible (RESP) High-Performance TCP Server.
Allows any standard Redis client (redis-cli, redis-py, ioredis, go-redis) to interact with the LSM storage engine.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from hyperquery.engine import LSMTreeEngine
from hyperquery.resp import RESPParser


class HyperQueryServer:
    """
    Asynchronous TCP server implementing the Redis RESP wire protocol.
    Backs all key-value operations with the HyperQuery Log-Structured Merge-Tree.
    """

    def __init__(self, engine: LSMTreeEngine, host: str = "0.0.0.0", port: int = 6379) -> None:
        self.engine = engine
        self.host = host
        self.port = port
        self._server: Optional[asyncio.Server] = None

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle continuous client connection stream."""
        buffer = bytearray()
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

                    response = self.execute_command(tokens)
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

    def execute_command(self, tokens: list[bytes]) -> bytes:
        """Dispatch command tokens to LSM storage engine."""
        if not tokens:
            return RESPParser.serialize_error("Empty command")

        cmd = tokens[0].decode("ascii", errors="replace").upper()
        args = tokens[1:]

        if cmd == "PING":
            msg = args[0].decode("utf-8", errors="replace") if args else "PONG"
            return RESPParser.serialize_simple_string(msg)

        elif cmd == "SET":
            if len(args) < 2:
                return RESPParser.serialize_error("wrong number of arguments for 'SET'")
            key = args[0]
            val = args[1]
            self.engine.put(key, val)
            return RESPParser.serialize_simple_string("OK")

        elif cmd == "GET":
            if len(args) < 1:
                return RESPParser.serialize_error("wrong number of arguments for 'GET'")
            key = args[0]
            val = self.engine.get(key)
            return RESPParser.serialize_bulk_string(val)

        elif cmd == "DEL":
            if len(args) < 1:
                return RESPParser.serialize_error("wrong number of arguments for 'DEL'")
            for k in args:
                self.engine.delete(k)
            return RESPParser.serialize_integer(len(args))

        elif cmd == "SCAN":
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

        elif cmd == "INFO":
            info_text = (
                "# HyperQuery-LSM Storage Engine\r\n"
                "engine:hyperquery-lsm\r\n"
                f"memtable_entries:{len(self.engine._memtable)}\r\n"
                f"memtable_bytes:{self.engine._memtable.size_bytes}\r\n"
                f"level0_tables:{len(self.engine._sstables.get(0, []))}\r\n"
                f"level1_tables:{len(self.engine._sstables.get(1, []))}\r\n"
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
