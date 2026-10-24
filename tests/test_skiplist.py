"""
Tests for SkipList ordered MemTable operations and range scans.
"""

import pytest
from hyperquery.skiplist import SkipList
from hyperquery.types import Entry


def test_skiplist_insert_and_get():
    sl = SkipList()
    assert len(sl) == 0

    e1 = Entry.put(b"beta", b"val-beta")
    e2 = Entry.put(b"alpha", b"val-alpha")
    e3 = Entry.put(b"gamma", b"val-gamma")

    sl.insert(e1)
    sl.insert(e2)
    sl.insert(e3)

    assert len(sl) == 3
    assert sl.get(b"alpha").value == b"val-alpha"
    assert sl.get(b"beta").value == b"val-beta"
    assert sl.get(b"gamma").value == b"val-gamma"
    assert sl.get(b"delta") is None


def test_skiplist_update_in_place():
    sl = SkipList()
    sl.insert(Entry.put(b"user-1", b"version-1"))
    assert sl.get(b"user-1").value == b"version-1"
    assert len(sl) == 1

    sl.insert(Entry.put(b"user-1", b"version-2"))
    assert sl.get(b"user-1").value == b"version-2"
    assert len(sl) == 1


def test_skiplist_sorted_order_and_scan():
    sl = SkipList()
    keys = [b"k05", b"k02", b"k09", b"k01", b"k07", b"k03"]
    for k in keys:
        sl.insert(Entry.put(k, k))

    # Strict lexicographical ordering
    sorted_entries = sl.to_sorted_list()
    assert [e.key for e in sorted_entries] == [b"k01", b"k02", b"k03", b"k05", b"k07", b"k09"]

    # Range scan [k02, k07)
    scanned = sl.scan(start_key=b"k02", end_key=b"k07")
    assert [e.key for e in scanned] == [b"k02", b"k03", b"k05"]


def test_skiplist_tombstone():
    sl = SkipList()
    sl.insert(Entry.put(b"account", b"100"))
    assert sl.get(b"account").is_tombstone is False

    sl.insert(Entry.delete(b"account"))
    assert sl.get(b"account").is_tombstone is True
    assert sl.get(b"account").value is None
