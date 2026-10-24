"""
Enterprise Platform Tests for HyperQuery-LSM.
Tests RESP2 TCP Server, Password AUTH, Prometheus Metrics, Checkpointing Snapshots, and Client SDK.
"""

import asyncio
from pathlib import Path
import shutil
import tempfile
import threading
import time
import pytest

from hyperquery.client import HyperQueryClient
from hyperquery.engine import LSMTreeEngine
from hyperquery.resp import RESPParser
from hyperquery.server import HyperQueryServer


@pytest.fixture
def test_dir():
    d = Path(tempfile.mkdtemp(prefix="hyperquery_test_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def engine(test_dir):
    eng = LSMTreeEngine(data_dir=test_dir, memtable_max_entries=50)
    yield eng
    eng.close()


def test_command_metrics_and_info(engine):
    server = HyperQueryServer(engine=engine)
    server.execute_command([b"SET", b"metric_key", b"metric_val"])
    server.execute_command([b"GET", b"metric_key"])

    info_resp = server.execute_command([b"INFO"])
    assert b"total_puts:1" in info_resp
    assert b"total_gets:1" in info_resp
    assert b"engine:hyperquery-lsm" in info_resp

    metrics_resp = server.execute_command([b"METRICS"])
    assert b"hyperquery_puts_total 1" in metrics_resp
    assert b"hyperquery_gets_total 1" in metrics_resp


def test_password_auth_protection(engine):
    server = HyperQueryServer(engine=engine, requirepass="secret123")
    client_state = {"authenticated": False}

    # Unauthenticated GET should fail
    resp = server.execute_command([b"GET", b"some_key"], client_state=client_state)
    assert b"NOAUTH" in resp

    # Wrong password should fail
    auth_fail = server.execute_command([b"AUTH", b"wrong_password"], client_state=client_state)
    assert b"invalid password" in auth_fail
    assert client_state["authenticated"] is False

    # Correct password should succeed
    auth_ok = server.execute_command([b"AUTH", b"secret123"], client_state=client_state)
    assert auth_ok == b"+OK\r\n"
    assert client_state["authenticated"] is True

    # Now GET should proceed
    get_resp = server.execute_command([b"GET", b"some_key"], client_state=client_state)
    assert get_resp == b"$-1\r\n"


def test_snapshot_creation_and_listing(engine):
    server = HyperQueryServer(engine=engine)
    server.execute_command([b"SET", b"snap_k1", b"snap_v1"])
    server.execute_command([b"SET", b"snap_k2", b"snap_v2"])

    snap_resp = server.execute_command([b"SNAPSHOT"])
    assert b"OK SNAPSHOT CREATED" in snap_resp

    list_resp = server.execute_command([b"SNAPSHOTS"])
    tokens, _ = RESPParser.parse_request(list_resp)
    assert tokens is not None
    assert len(tokens) >= 1
    assert tokens[0].startswith(b"snapshot_")


def test_client_sdk_over_tcp(engine):
    port = 6398
    auth_pass = "sdk_pass_456"
    server = HyperQueryServer(engine=engine, host="127.0.0.1", port=port, requirepass=auth_pass)

    loop = asyncio.new_event_loop()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.start())

    th = threading.Thread(target=run, daemon=True)
    th.start()
    time.sleep(0.3)

    try:
        # 1. Unauthenticated connection fails
        with pytest.raises(RuntimeError) as exc_info:
            bad_client = HyperQueryClient(host="127.0.0.1", port=port, password="wrong")
            bad_client.connect()
        assert "invalid password" in str(exc_info.value)

        # 2. Authenticated client connects
        with HyperQueryClient(host="127.0.0.1", port=port, password=auth_pass) as client:
            assert client.ping("ALIVE") == "ALIVE"

            # 3. SET & GET
            assert client.set("item:100", "laptop") is True
            assert client.get("item:100") == b"laptop"

            # 4. SCAN
            client.set("item:101", "mouse")
            client.set("item:102", "keyboard")
            items = client.scan("item:100", "item:103")
            assert len(items) == 3
            assert items[0] == (b"item:100", b"laptop")

            # 5. SNAPSHOT
            snap_res = client.snapshot()
            assert "SNAPSHOT CREATED" in snap_res
            snaps = client.list_snapshots()
            assert len(snaps) >= 1

            # 6. DEL
            del_count = client.delete("item:100")
            assert del_count == 1
            assert client.get("item:100") is None

            # 7. METRICS
            metrics = client.get_metrics()
            assert "hyperquery_commands_total" in metrics
    finally:
        server.stop()
