"""
HyperQuery-LSM: Bloom Filter with Kirsch-Mitzenmacher Double Hashing.
Guarantees zero false negatives to bypass disk I/O on negative SSTable lookups.
"""

from __future__ import annotations

import math
import struct
import zlib
from typing import Tuple


class BloomFilter:
    """
    Space-efficient probabilistic filter using double-hashing.
    Formula:
        m = -(n * ln(p)) / (ln(2)^2)   [Bit array size]
        k = (m / n) * ln(2)            [Number of hash functions]
    """

    def __init__(
        self,
        expected_elements: int = 1000,
        false_positive_rate: float = 0.01,
        bit_array: bytearray | None = None,
        bit_size: int | None = None,
        num_hashes: int | None = None,
    ) -> None:
        if bit_array is not None and bit_size is not None and num_hashes is not None:
            self.bit_size = bit_size
            self.num_hashes = num_hashes
            self.bit_array = bit_array
            return

        n = max(1, expected_elements)
        p = max(1e-6, min(0.5, false_positive_rate))

        # Optimal bits: m = - (n * ln(p)) / (ln(2)^2)
        m = int(- (n * math.log(p)) / (math.log(2) ** 2))
        self.bit_size = max(64, m)

        # Optimal hashes: k = (m / n) * ln(2)
        k = int((self.bit_size / n) * math.log(2))
        self.num_hashes = max(1, min(30, k))

        num_bytes = (self.bit_size + 7) // 8
        self.bit_array = bytearray(num_bytes)

    @staticmethod
    def _hash_pair(key: bytes) -> Tuple[int, int]:
        """Compute two 32-bit hashes for Kirsch-Mitzenmacher generation."""
        h1 = zlib.crc32(key) & 0xFFFFFFFF
        # FNV-1a 32-bit hash
        h2 = 2166136261
        for b in key:
            h2 = ((h2 ^ b) * 16777619) & 0xFFFFFFFF
        if h2 == 0:
            h2 = 1
        return h1, h2

    def add(self, key: bytes) -> None:
        """Add a key to the filter."""
        h1, h2 = self._hash_pair(key)
        for i in range(self.num_hashes):
            idx = (h1 + (i * h2)) % self.bit_size
            byte_idx = idx // 8
            bit_idx = idx % 8
            self.bit_array[byte_idx] |= (1 << bit_idx)

    def contains(self, key: bytes) -> bool:
        """
        Check if key might be in the set.
        Returns False: 100% Guaranteed NOT in set (Zero False Negatives).
        Returns True: Likely in set with false positive probability p.
        """
        h1, h2 = self._hash_pair(key)
        for i in range(self.num_hashes):
            idx = (h1 + (i * h2)) % self.bit_size
            byte_idx = idx // 8
            bit_idx = idx % 8
            if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                return False
        return True

    def serialize(self) -> bytes:
        """
        Binary format: [4B bit_size][2B num_hashes][N bytes bit_array]
        """
        header = struct.pack(">IH", self.bit_size, self.num_hashes)
        return header + bytes(self.bit_array)

    @classmethod
    def deserialize(cls, data: bytes) -> BloomFilter:
        """Deserialize raw binary buffer into BloomFilter instance."""
        bit_size, num_hashes = struct.unpack(">IH", data[:6])
        bit_bytes = bytearray(data[6:])
        return cls(
            bit_array=bit_bytes,
            bit_size=bit_size,
            num_hashes=num_hashes,
        )
