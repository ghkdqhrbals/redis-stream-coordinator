from __future__ import annotations

HASH_SPACE_SIZE = 1 << 32
C1 = 0xCC9E2D51
C2 = 0x1B873593


def murmur3_32(data: bytes, seed: int = 0) -> int:
    """Returns the same signed 32-bit Murmur3 hash used by the JVM starter."""
    h1 = seed & 0xFFFFFFFF
    rounded_end = len(data) & -4

    for index in range(0, rounded_end, 4):
        k1 = (
            data[index]
            | (data[index + 1] << 8)
            | (data[index + 2] << 16)
            | (data[index + 3] << 24)
        )
        k1 = (k1 * C1) & 0xFFFFFFFF
        k1 = _rotl32(k1, 15)
        k1 = (k1 * C2) & 0xFFFFFFFF

        h1 ^= k1
        h1 = _rotl32(h1, 13)
        h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF

    k1 = 0
    tail = len(data) & 3
    if tail == 3:
        k1 ^= data[rounded_end + 2] << 16
    if tail >= 2:
        k1 ^= data[rounded_end + 1] << 8
    if tail >= 1:
        k1 ^= data[rounded_end]
        k1 = (k1 * C1) & 0xFFFFFFFF
        k1 = _rotl32(k1, 15)
        k1 = (k1 * C2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= len(data)
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    return _to_signed32(h1)


def unbiased_shard_index(partition_key: bytes, shard_count: int) -> int:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    limit = HASH_SPACE_SIZE - (HASH_SPACE_SIZE % shard_count)
    attempt = 0
    while True:
        seed = 0 if attempt == 0 else murmur3_32(attempt.to_bytes(4, "little", signed=False))
        unsigned_hash = murmur3_32(partition_key, seed) & 0xFFFFFFFF
        if unsigned_hash < limit:
            return unsigned_hash % shard_count
        attempt += 1


def _rotl32(value: int, bits: int) -> int:
    return ((value << bits) | (value >> (32 - bits))) & 0xFFFFFFFF


def _to_signed32(value: int) -> int:
    return value - 0x100000000 if value & 0x80000000 else value

