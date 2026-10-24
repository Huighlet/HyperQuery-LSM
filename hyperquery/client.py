"""
HyperQuery Client SDK: Idiomatic high-performance client for HyperQuery-LSM Storage Engine.
Communicates natively with the server over TCP using the Redis Serialization Protocol (RESP2).
"""

from __future__ import annotations

import socket
from typing import Any, Dict, List, Optional, Tuple, Union


class HyperQueryClient:
    """
    Client for interacting with HyperQuery-LSM TCP server via RESP2.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        password: Optional[str] = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.timeout_seconds = timeout_seconds
        self._sock: Optional[socket.socket] = None
        self._rfile: Optional[Any] = None

    def connect(self) -> HyperQueryClient:
        """Establish TCP connection and authenticate if password is configured."""
        if self._sock is not None:
            return self

        sock = socket.create_connection((self.host, self.port), timeout=self.timeout_seconds)
        self._sock = sock
        self._rfile = sock.makefile("rb", buffering=65536)

        if self.password:
            self.auth(self.password)

        return self

    def close(self) -> None:
        """Close socket connection."""
        if self._rfile:
            try:
                self._rfile.close()
            except Exception:
                pass
            self._rfile = None

        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def __enter__(self) -> HyperQueryClient:
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _ensure_connected(self) -> None:
        if self._sock is None or self._rfile is None:
            self.connect()

    def _send_command(self, *tokens: Union[str, bytes]) -> Any:
        """Serialize command to RESP2 array, send over wire, and parse response."""
        self._ensure_connected()

        # Serialize to *N\r\n$len\r\ntoken\r\n...
        payload = bytearray(f"*{len(tokens)}\r\n".encode("ascii"))
        for t in tokens:
            b_token = t.encode("utf-8") if isinstance(t, str) else t
            payload.extend(f"${len(b_token)}\r\n".encode("ascii"))
            payload.extend(b_token)
            payload.extend(b"\r\n")

        self._sock.sendall(payload)
        return self._read_response()

    def _read_response(self) -> Any:
        """Read and parse single RESP2 frame from socket stream."""
        line = self._rfile.readline()
        if not line:
            raise ConnectionResetError("Server closed connection unexpectedly")

        prefix = line[:1]
        content = line[1:-2]  # Strip prefix and trailing \r\n

        if prefix == b"+":
            # Simple string
            return content.decode("utf-8", errors="replace")

        elif prefix == b"-":
            # Error
            err_msg = content.decode("utf-8", errors="replace")
            raise RuntimeError(f"HyperQuery Server Error: {err_msg}")

        elif prefix == b":":
            # Integer
            return int(content.decode("ascii"))

        elif prefix == b"$":
            # Bulk string
            length = int(content.decode("ascii"))
            if length == -1:
                return None
            data = self._rfile.read(length)
            self._rfile.read(2)  # Discard \r\n
            return data

        elif prefix == b"*":
            # Array
            num_elements = int(content.decode("ascii"))
            if num_elements == -1:
                return None
            return [self._read_response() for _ in range(num_elements)]

        else:
            raise ValueError(f"Unknown RESP frame prefix: {prefix!r}")

    # -------------------------------------------------------------------------
    # Public Key-Value Operations
    # -------------------------------------------------------------------------

    def ping(self, message: Optional[str] = None) -> str:
        """Health check ping."""
        if message:
            return self._send_command("PING", message)
        return self._send_command("PING")

    def auth(self, password: str) -> str:
        """Authenticate with server password."""
        return self._send_command("AUTH", password)

    def set(self, key: Union[str, bytes], value: Union[str, bytes]) -> bool:
        """Put key-value pair into LSM-Tree."""
        res = self._send_command("SET", key, value)
        return res == "OK"

    def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """Fetch value for key from LSM-Tree (MemTable or SSTables)."""
        return self._send_command("GET", key)

    def delete(self, *keys: Union[str, bytes]) -> int:
        """Soft-delete keys using tombstone records."""
        if not keys:
            return 0
        return self._send_command("DEL", *keys)

    def scan(
        self,
        start_key: Optional[Union[str, bytes]] = None,
        end_key: Optional[Union[str, bytes]] = None,
    ) -> List[Tuple[bytes, bytes]]:
        """Range scan returning lexicographically sorted (key, value) pairs."""
        args: List[Union[str, bytes]] = ["SCAN"]
        if start_key is not None:
            args.append(start_key)
        if end_key is not None:
            args.append(end_key)

        raw = self._send_command(*args)
        if not raw:
            return []

        # Alternating (key, value) elements
        results: List[Tuple[bytes, bytes]] = []
        for i in range(0, len(raw), 2):
            k = raw[i]
            v = raw[i + 1] if i + 1 < len(raw) else b""
            results.append((k, v))
        return results

    def flush(self) -> bool:
        """Flush active MemTable to Level 0 SSTable on disk."""
        res = self._send_command("FLUSHDB")
        return res == "OK"

    def snapshot(self) -> str:
        """Create point-in-time state checkpoint and snapshot manifest."""
        return self._send_command("SNAPSHOT")

    def list_snapshots(self) -> List[str]:
        """Retrieve list of historical snapshot identifiers."""
        raw = self._send_command("SNAPSHOTS")
        if not raw:
            return []
        return [item.decode("utf-8") if isinstance(item, bytes) else str(item) for item in raw]

    def info(self) -> Dict[str, str]:
        """Retrieve server and storage engine metadata dictionary."""
        raw = self._send_command("INFO")
        info_text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        stats = {}
        for line in info_text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                k, v = line.split(":", 1)
                stats[k.strip()] = v.strip()
        return stats

    def get_metrics(self) -> str:
        """Retrieve Prometheus text metrics format."""
        raw = self._send_command("METRICS")
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
