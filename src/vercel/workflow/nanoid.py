"""
Minimal Nano ID implementation in Python.

Based on the JavaScript Nano ID library: https://github.com/ai/nanoid
This implementation provides URL-friendly unique string IDs with customizable
alphabet and size, with support for custom PRNG functions.

Reference: https://github.com/ai/nanoid/blob/main/index.js
"""

import os
from collections.abc import Callable
from math import ceil

# This alphabet uses `A-Za-z0-9_-` symbols.
# The order of characters is optimized for better gzip and brotli compression.
# Same as JS: https://github.com/ai/nanoid/blob/main/url-alphabet/index.js
# References to the same file (works both for gzip and brotli):
# `'use`, `andom`, and `rict'`
# References to the brotli default dictionary:
# `-26T`, `1983`, `40px`, `75px`, `bush`, `jack`, `mind`, `very`, and `wolf`
URL_ALPHABET = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"
DEFAULT_SIZE = 21


def _detect_prng() -> Callable[[int], bytes]:
    """
    Create a default random bytes generator using os.urandom() for
    cryptographically secure randomness.

    Matches JavaScript's crypto.getRandomValues() behavior.
    """
    return os.urandom


def _custom_alphabet_generator(
    alphabet: str,
    default_size: int,
    get_random: Callable[[int], bytes],
) -> Callable[..., str]:
    """
    Core nanoid generator function (matches JS nanoid/index.js customRandom).

    Args:
        alphabet: Characters to use for ID generation.
        default_size: Default length for generated IDs.
        get_random: Function to generate random bytes.

    Returns:
        A function that generates IDs with the custom alphabet.

    Reference: https://github.com/ai/nanoid/blob/main/index.js customRandom()
    """
    alphabet_len = len(alphabet)

    if alphabet_len == 0 or alphabet_len > 256:
        raise ValueError("Alphabet must contain between 1 and 256 symbols")

    # First, a bitmask is necessary to generate the ID. The bitmask makes bytes
    # values closer to the alphabet size. The bitmask calculates the closest
    # `2^31 - 1` number, which exceeds the alphabet size.
    # For example, the bitmask for the alphabet size 30 is 31 (00011111).
    # Matches JS: mask = (2 << (31 - Math.clz32((alphabet.length - 1) | 1))) - 1
    mask = (2 << (31 - _clz32((alphabet_len - 1) | 1))) - 1

    # Though, the bitmask solution is not perfect since the bytes exceeding
    # the alphabet size are refused. Therefore, to reliably generate the ID,
    # the random bytes redundancy has to be satisfied.

    # Note: every hardware random generator call is performance expensive,
    # because the system call for entropy collection takes a lot of time.
    # So, to avoid additional system calls, extra bytes are requested in advance.

    # Next, a step determines how many random bytes to generate.
    # The number of random bytes gets decided upon the ID size, mask,
    # alphabet size, and magic number 1.6 (using 1.6 peaks at performance
    # according to benchmarks).
    # Matches JS: step = Math.ceil((1.6 * mask * defaultSize) / alphabet.length)
    step = ceil((1.6 * mask * default_size) / alphabet_len)

    def generate_id(size: int = default_size) -> str:
        """Generate a nano ID of the specified size."""
        # Matches JS: if (!size) return ''
        if not size:
            return ""

        id_str = ""
        while True:
            random_bytes = get_random(step)

            # A compact alternative for `for (let i = 0; i < step; i++)`.
            # Matches JS nanoid implementation
            i = step
            while i > 0:
                i -= 1
                # Adding `|| ''` refuses a random byte that exceeds the alphabet size.
                # Matches JS: id += alphabet[bytes[i] & mask] || ''
                byte_index = random_bytes[i] & mask
                if byte_index < alphabet_len:
                    id_str += alphabet[byte_index]
                    if len(id_str) >= size:
                        return id_str

    return generate_id


def _clz32(n: int) -> int:
    """
    Count leading zeros in 32-bit integer.
    Matches JavaScript's Math.clz32().

    Examples:
        _clz32(1) == 31
        _clz32(2) == 30
        _clz32(3) == 30
        _clz32(4) == 29
    """
    if n == 0:
        return 32
    # Convert to 32-bit unsigned integer
    n = n & 0xFFFFFFFF
    if n == 0:
        return 32
    # Check each bit from MSB
    for i in range(31, -1, -1):
        if n & (1 << i):
            return 31 - i
    return 32


def custom_alphabet(
    alphabet: str,
    size: int = DEFAULT_SIZE,
) -> Callable[..., str]:
    """
    Create a custom ID generator with a specific alphabet.

    This factory function returns a generator that uses the specified alphabet.
    Matches JS nanoid customAlphabet() function.

    Args:
        alphabet: Characters to use for ID generation.
        size: Default length for generated IDs.

    Returns:
        A function that generates IDs with the custom alphabet.

    Examples:
        # Create a hex ID generator
        hex_id = custom_alphabet('0123456789abcdef', 16)
        id1 = hex_id()     # '4f3a2b1c9d8e7f6a'
        id2 = hex_id(8)    # '9d8e7f6a'

        # Create a custom alphabet generator
        safe_id = custom_alphabet('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        id3 = safe_id()    # 'K3PQ9XY7ZR2J8Y4H3L6M9'
    """
    return _custom_alphabet_generator(alphabet, size, _detect_prng())


def custom_random(
    alphabet: str,
    size: int,
    prng: Callable[[], float],
) -> Callable[..., str]:
    """
    Create a custom ID generator with a specific alphabet and PRNG.

    This matches the style of ulid.monotonic_factory() and allows for
    deterministic ID generation (useful for testing or workflow replay).

    Args:
        alphabet: Characters to use for ID generation.
        size: Default length for generated IDs.
        prng: Pseudo-random number generator function that returns float in [0, 1).
              Example: random.Random(seed).random

    Returns:
        A function that generates IDs with the custom alphabet and PRNG.

    Examples:
        # Create a deterministic generator for testing
        import random
        prng = random.Random(42).random
        test_id = custom_random('0123456789', 10, prng)
        id1 = test_id()  # Always generates the same ID with the same seed

        # Usage in workflow context (similar to ulid)
        prng = random.Random(workflow_seed).random
        nanoid_gen = custom_random(URL_ALPHABET, 21, prng)
        token = nanoid_gen()
    """

    def get_random_bytes(n: int) -> bytes:
        """Convert PRNG floats to random bytes."""
        # Match JavaScript: use floor(prng() * 256) to get byte values
        return bytes(int(prng() * 256) for _ in range(n))

    return _custom_alphabet_generator(alphabet, size, get_random_bytes)


def generate(
    alphabet: str = URL_ALPHABET,
    size: int = DEFAULT_SIZE,
) -> str:
    """
    Generate a Nano ID string using default cryptographically secure randomness.

    This is the main function that matches JS nanoid() behavior.

    Args:
        alphabet: Characters to use for ID generation. Default is URL-safe alphabet.
        size: Length of the generated ID. Default is 21.

    Returns:
        A random string ID of specified size using the specified alphabet.

    Examples:
        # Basic usage with defaults
        id1 = generate()  # 'V1StGXR8_Z5jdHi6B-myT'

        # Custom size
        id2 = generate(size=10)  # 'IRFa-VaY2b'

        # Custom alphabet (numbers only)
        id3 = generate(alphabet='0123456789', size=6)  # '482014'
    """
    generator = _custom_alphabet_generator(alphabet, size, _detect_prng())
    return generator(size)
