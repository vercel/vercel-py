"""
Minimal monotonic ULID implementation in Python.

Based on the JavaScript ULID library: https://github.com/ulid/javascript
This implementation ensures that ULIDs are monotonically increasing even when
timestamps go backwards or are the same.
"""

import os
import time
from collections.abc import Callable

# Crockford's Base32 alphabet
ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
ENCODING_LEN = len(ENCODING)

# ULID structure: 48 bits timestamp + 80 bits randomness = 128 bits total
TIME_MAX = 281474976710655  # 2^48 - 1
TIME_LEN = 10  # characters for timestamp part
RANDOM_LEN = 16  # characters for random part


def _encode_time(timestamp_ms: int, length: int) -> str:
    """Encode a timestamp (in milliseconds) as a Crockford Base32 string."""
    if timestamp_ms > TIME_MAX:
        raise ValueError(f"Timestamp must be <= {TIME_MAX}")
    if timestamp_ms < 0:
        raise ValueError("Timestamp must be >= 0")

    result = ""
    for _ in range(length):
        mod = timestamp_ms % ENCODING_LEN
        result = ENCODING[mod] + result
        timestamp_ms = (timestamp_ms - mod) // ENCODING_LEN

    return result


def _encode_random(length: int, prng: Callable[[], float]) -> str:
    """Generate a random string of specified length using Crockford Base32.

    Args:
        length: Number of characters to generate
        prng: Pseudo-random number generator function that returns float in [0, 1)
    """
    result = ""
    for _ in range(length):
        random_value = prng()
        # Match JavaScript implementation: Math.floor(prng() * ENCODING_LEN) % ENCODING_LEN
        index = int(random_value * ENCODING_LEN) % ENCODING_LEN
        result += ENCODING[index]
    return result


def _detect_prng() -> Callable[[], float]:
    """Create a default PRNG using os.urandom() for cryptographically secure randomness.

    Matches JavaScript's detectPRNG behavior: returns a function that generates
    a random float in [0, 1) by converting bytes to float like buffer[0] / 256.
    """

    def crypto_prng() -> float:
        # Match JavaScript: buffer[0] / 256 to get float in [0, 1)
        return os.urandom(1)[0] / 256.0

    return crypto_prng


def _increment_base32(base32_str: str) -> str | None:
    """
    Increment a Base32 string by 1.
    Returns None if overflow occurs (all characters are at maximum value).
    """
    chars = list(base32_str)
    for i in range(len(chars) - 1, -1, -1):
        char_value = ENCODING.index(chars[i])
        if char_value < ENCODING_LEN - 1:
            chars[i] = ENCODING[char_value + 1]
            return "".join(chars)
        # Carry over - set this position to 0 and continue
        chars[i] = ENCODING[0]

    # Overflow - all characters wrapped around
    return None


def monotonic_factory(prng: Callable[[], float] | None = None) -> Callable[[int | None], str]:
    """
    Create a monotonic ULID generator function.

    Args:
        prng: Optional pseudo-random number generator function that returns float in [0, 1).
              If None, uses os.urandom() for cryptographically secure randomness.
              Example: lambda: 0.96 for testing

    Returns:
        A function that accepts an optional timestamp in milliseconds
        and generates ULIDs that are guaranteed to be monotonically increasing.

    Usage:
        ulid_gen = monotonic_factory()
        ulid1 = ulid_gen(1234567890000)  # with specific timestamp
        ulid2 = ulid_gen(None)            # with current timestamp
        ulid3 = ulid_gen(1234567890000)  # even with older timestamp, still monotonic

        # Or with custom PRNG for testing:
        ulid_gen = monotonic_factory(lambda: 0.96)
    """
    # Match JavaScript: prng ?? detectPRNG()
    current_prng = prng if prng is not None else _detect_prng()

    last_timestamp = 0
    last_random = None  # Match JavaScript: initially undefined/None

    def generate(timestamp_ms: int | None = None) -> str:
        nonlocal last_timestamp, last_random

        # Use current time if not provided or if NaN/invalid
        # Match JavaScript: !seedTime || isNaN(seedTime) ? Date.now() : seedTime
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)
        elif not isinstance(timestamp_ms, (int, float)):
            timestamp_ms = int(time.time() * 1000)

        # Ensure timestamp is valid
        if timestamp_ms < 0:
            raise ValueError("Timestamp must be >= 0")
        if timestamp_ms > TIME_MAX:
            raise ValueError(f"Timestamp must be <= {TIME_MAX}")

        # If timestamp is same or goes backwards, increment the random part
        if timestamp_ms <= last_timestamp:
            # Keep using the last timestamp and increment random part
            if last_random is None:
                # First call with backwards/same time - shouldn't happen but handle it
                last_random = _encode_random(RANDOM_LEN, current_prng)
            else:
                incremented = _increment_base32(last_random)

                if incremented is None:
                    # Random part overflowed, need to increment timestamp
                    last_timestamp += 1
                    if last_timestamp > TIME_MAX:
                        raise ValueError("ULID overflow: timestamp exceeded maximum value")
                    last_random = _encode_random(RANDOM_LEN, current_prng)
                else:
                    last_random = incremented
        else:
            # New timestamp is greater, use it and generate new random part
            last_timestamp = timestamp_ms
            last_random = _encode_random(RANDOM_LEN, current_prng)

        # Encode and return the ULID
        time_part = _encode_time(last_timestamp, TIME_LEN)
        return time_part + last_random

    return generate
