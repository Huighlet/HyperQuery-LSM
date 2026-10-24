"""
Tests for BloomFilter double-hashing, zero false negative guarantee, and serialization.
"""

import pytest
from hyperquery.bloom import BloomFilter


def test_bloom_filter_zero_false_negatives():
    bloom = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
    keys = [f"item-key-{i}".encode("ascii") for i in range(500)]

    for k in keys:
        bloom.add(k)

    # Every inserted key MUST return True
    for k in keys:
        assert bloom.contains(k) is True


def test_bloom_filter_false_positive_bound():
    bloom = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
    for i in range(500):
        bloom.add(f"member-{i}".encode("ascii"))

    # Test 1000 absent keys
    false_positives = 0
    for i in range(1000):
        if bloom.contains(f"absent-{i}".encode("ascii")):
            false_positives += 1

    fp_rate = false_positives / 1000.0
    # Expected ~1% false positive, allow slight variance up to 3%
    assert fp_rate < 0.03


def test_bloom_serialization_roundtrip():
    bloom = BloomFilter(expected_elements=200, false_positive_rate=0.01)
    for i in range(50):
        bloom.add(f"key-{i}".encode("ascii"))

    serialized = bloom.serialize()
    restored = BloomFilter.deserialize(serialized)

    assert restored.bit_size == bloom.bit_size
    assert restored.num_hashes == bloom.num_hashes

    for i in range(50):
        assert restored.contains(f"key-{i}".encode("ascii")) is True
    assert restored.contains(b"definitely-absent") is False
