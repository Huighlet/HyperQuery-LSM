"""
HyperQuery-LSM: Redis Serialization Protocol (RESP2) Wire Parser & Serializer.
Zero external dependencies; pure standard library bitwise frame parsing.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple


class RESPParser:
    """
    Parser for Redis Serialization Protocol (RESP2).
    Types handled:
      + Simple Strings: "+OK\\r\\n"
      : Integers: ":1000\\r\\n"
      $ Bulk Strings: "$6\\r\\nfoobar\\r\\n" (or "$-1\\r\\n" for nil)
      * Arrays: "*2\\r\\n$3\\r\\nGET\\r\\n$4\\r\\nuser\\r\\n"
    """

    @staticmethod
    def serialize_simple_string(val: str) -> bytes:
        return f"+{val}\r\n".encode("utf-8")

    @staticmethod
    def serialize_error(msg: str) -> bytes:
        return f"-ERR {msg}\r\n".encode("utf-8")

    @staticmethod
    def serialize_integer(val: int) -> bytes:
        return f":{val}\r\n".encode("ascii")

    @staticmethod
    def serialize_bulk_string(val: Optional[bytes]) -> bytes:
        if val is None:
            return b"$-1\r\n"
        return f"${len(val)}\r\n".encode("ascii") + val + b"\r\n"

    @staticmethod
    def serialize_array(items: List[bytes]) -> bytes:
        header = f"*{len(items)}\r\n".encode("ascii")
        return header + b"".join(items)

    @classmethod
    def parse_request(cls, buffer: bytes) -> Tuple[Optional[List[bytes]], bytes]:
        """
        Parse a command array from incoming network buffer.
        Returns (list of command tokens or None if incomplete, remaining buffer).
        """
        if not buffer or b"\r\n" not in buffer:
            return None, buffer

        # Redis commands are typically arrays of bulk strings: *<count>\r\n
        if buffer.startswith(b"*"):
            crlf_pos = buffer.find(b"\r\n")
            try:
                num_elements = int(buffer[1:crlf_pos].decode("ascii"))
            except ValueError:
                return None, buffer

            pos = crlf_pos + 2
            tokens: List[bytes] = []

            for _ in range(num_elements):
                if pos >= len(buffer):
                    return None, buffer

                # Expect bulk string: $<len>\r\n<data>\r\n
                if buffer[pos : pos + 1] != b"$":
                    # Fallback for inline commands
                    end_line = buffer.find(b"\r\n", pos)
                    if end_line == -1:
                        return None, buffer
                    tokens.append(buffer[pos:end_line])
                    pos = end_line + 2
                    continue

                crlf_next = buffer.find(b"\r\n", pos)
                if crlf_next == -1:
                    return None, buffer

                str_len = int(buffer[pos + 1 : crlf_next].decode("ascii"))
                data_start = crlf_next + 2
                data_end = data_start + str_len

                if data_end + 2 > len(buffer):
                    return None, buffer  # Incomplete payload

                tokens.append(buffer[data_start:data_end])
                pos = data_end + 2  # Skip trailing \r\n

            return tokens, buffer[pos:]

        # Inline command (e.g. "PING\r\n")
        crlf_pos = buffer.find(b"\r\n")
        line = buffer[:crlf_pos]
        parts = [p for p in line.split(b" ") if p]
        return parts, buffer[crlf_pos + 2 :]
