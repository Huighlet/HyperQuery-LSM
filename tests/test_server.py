"""
Tests for HyperQuery-LSM Redis Protocol (RESP) wire parser and server.
"""

from pathlib import Path
import pytest
from hyperquery.engine import LSMTreeEngine
from hyperquery.resp import RESPParser
from hyperquery.server import HyperQueryServer


def test_resp_parser_serialization():
    assert RESPParser.serialize_simple_string("OK") == b"+OK\r\n"
    assert RESPParser.serialize_integer(42) == b":42\r\n"
    assert RESPParser.serialize_bulk_string(b"hello") == b"$5\r\nhello\r\n"
    assert RESPParser.serialize_bulk_string(None) == b"$-1\r\n"


def test_resp_parser_request():
    # Array: *2\r\n$3\r\nGET\r\n$4\r\nuser\r\n
    raw = b"*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n"
    tokens, remaining = RESPParser.parse_request(raw)
    assert remaining == b""
    assert tokens == [b"GET", b"user"]

    # Inline: PING\r\n
    tokens_inline, _ = RESPParser.parse_request(b"PING\r\n")
    assert tokens_inline == [b"PING"]


def test_hyperquery_server_commands(tmp_path: Path):
    engine = LSMTreeEngine(tmp_path)
    server = HyperQueryServer(engine)

    # 1. PING
    assert server.execute_command([b"PING"]) == b"+PONG\r\n"
    assert server.execute_command([b"PING", b"custom"]) == b"+custom\r\n"

    # 2. SET
    set_resp = server.execute_command([b"SET", b"session:user_1", b"alice_payload"])
    assert set_resp == b"+OK\r\n"

    # 3. GET
    get_resp = server.execute_command([b"GET", b"session:user_1"])
    assert get_resp == b"$13\r\nalice_payload\r\n"

    # 4. GET missing
    get_miss = server.execute_command([b"GET", b"session:user_unknown"])
    assert get_miss == b"$-1\r\n"

    # 5. DEL
    del_resp = server.execute_command([b"DEL", b"session:user_1"])
    assert del_resp == b":1\r\n"
    assert server.execute_command([b"GET", b"session:user_1"]) == b"$-1\r\n"

    # 6. INFO
    info_resp = server.execute_command([b"INFO"])
    assert b"hyperquery-lsm" in info_resp

    engine.close()
