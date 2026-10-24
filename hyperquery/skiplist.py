"""
HyperQuery-LSM: Probabilistic SkipList MemTable.
Provides O(log N) ordered key insertions, lookups, and sequential range scans.
"""

from __future__ import annotations

import random
import threading
from typing import Iterator, List, Optional

from hyperquery.types import Entry


class SkipListNode:
    """Internal node of a SkipList with multiple forward pointers."""

    __slots__ = ("entry", "forward")

    def __init__(self, entry: Optional[Entry], level: int) -> None:
        self.entry = entry
        self.forward: List[Optional[SkipListNode]] = [None] * level

    @property
    def key(self) -> bytes:
        assert self.entry is not None
        return self.entry.key


class SkipList:
    """
    Thread-safe probabilistic SkipList MemTable.
    Maintains keys in strict lexicographical byte order.
    """

    MAX_LEVEL: int = 16
    P: float = 0.5

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._head = SkipListNode(entry=None, level=self.MAX_LEVEL)
        self._level: int = 1
        self._count: int = 0
        self._size_bytes: int = 0

    def __len__(self) -> int:
        with self._lock:
            return self._count

    @property
    def size_bytes(self) -> int:
        with self._lock:
            return self._size_bytes

    def _random_level(self) -> int:
        """Geometric distribution random level generator: P(h = k) = p^(k-1) * (1-p)."""
        lvl = 1
        while lvl < self.MAX_LEVEL and random.random() < self.P:
            lvl += 1
        return lvl

    def insert(self, entry: Entry) -> None:
        """
        Insert or update a key-value entry in the SkipList.
        O(log N) expected time complexity.
        """
        with self._lock:
            update: List[Optional[SkipListNode]] = [None] * self.MAX_LEVEL
            current: SkipListNode = self._head

            for i in range(self._level - 1, -1, -1):
                while current.forward[i] is not None and current.forward[i].key < entry.key:
                    current = current.forward[i]
                update[i] = current

            target = current.forward[0]

            # In-place update if key exists
            if target is not None and target.key == entry.key:
                old_val_size = len(target.entry.value) if target.entry.value else 0
                new_val_size = len(entry.value) if entry.value else 0
                self._size_bytes += (new_val_size - old_val_size)
                target.entry = entry
                return

            # Insert new node
            new_level = self._random_level()
            if new_level > self._level:
                for i in range(self._level, new_level):
                    update[i] = self._head
                self._level = new_level

            new_node = SkipListNode(entry=entry, level=new_level)
            for i in range(new_level):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node

            self._count += 1
            key_size = len(entry.key)
            val_size = len(entry.value) if entry.value else 0
            self._size_bytes += (key_size + val_size)

    def get(self, key: bytes) -> Optional[Entry]:
        """
        Retrieve entry by key.
        Returns None if key is absent from the SkipList.
        """
        with self._lock:
            current: SkipListNode = self._head
            for i in range(self._level - 1, -1, -1):
                while current.forward[i] is not None and current.forward[i].key < key:
                    current = current.forward[i]

            target = current.forward[0]
            if target is not None and target.key == key:
                return target.entry
            return None

    def scan(
        self,
        start_key: Optional[bytes] = None,
        end_key: Optional[bytes] = None,
    ) -> List[Entry]:
        """
        Ordered range scan from start_key (inclusive) to end_key (exclusive).
        """
        with self._lock:
            results: List[Entry] = []
            current: SkipListNode = self._head

            if start_key is not None:
                for i in range(self._level - 1, -1, -1):
                    while current.forward[i] is not None and current.forward[i].key < start_key:
                        current = current.forward[i]
                node = current.forward[0]
            else:
                node = self._head.forward[0]

            while node is not None:
                if end_key is not None and node.key >= end_key:
                    break
                results.append(node.entry)
                node = node.forward[0]

            return results

    def to_sorted_list(self) -> List[Entry]:
        """Return all entries in strict lexicographical order."""
        return self.scan()

    def clear(self) -> None:
        """Reset the SkipList to an empty state."""
        with self._lock:
            self._head = SkipListNode(entry=None, level=self.MAX_LEVEL)
            self._level = 1
            self._count = 0
            self._size_bytes = 0
