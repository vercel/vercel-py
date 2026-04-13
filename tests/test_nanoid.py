"""Test nanoid implementation."""

from vercel.workflow import nanoid


def test_generate_default():
    """Test basic ID generation with defaults."""
    id1 = nanoid.generate()
    assert len(id1) == 21
    assert all(c in nanoid.URL_ALPHABET for c in id1)

    # Generate multiple IDs and check they're unique
    ids = {nanoid.generate() for _ in range(100)}
    assert len(ids) == 100, "Generated IDs should be unique"


def test_generate_custom_size():
    """Test ID generation with custom size."""
    id1 = nanoid.generate(size=10)
    assert len(id1) == 10

    id2 = nanoid.generate(size=5)
    assert len(id2) == 5

    id3 = nanoid.generate(size=50)
    assert len(id3) == 50


def test_generate_custom_alphabet():
    """Test ID generation with custom alphabet."""
    # Numbers only
    id1 = nanoid.generate(alphabet="0123456789", size=10)
    assert len(id1) == 10
    assert all(c in "0123456789" for c in id1)

    # Uppercase only
    id2 = nanoid.generate(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", size=15)
    assert len(id2) == 15
    assert all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" for c in id2)

    # Binary
    id3 = nanoid.generate(alphabet="01", size=20)
    assert len(id3) == 20
    assert all(c in "01" for c in id3)


def test_custom_alphabet_factory():
    """Test custom alphabet factory function."""
    hex_id = nanoid.custom_alphabet("0123456789abcdef", 16)

    id1 = hex_id()
    assert len(id1) == 16
    assert all(c in "0123456789abcdef" for c in id1)

    id2 = hex_id(8)
    assert len(id2) == 8
    assert all(c in "0123456789abcdef" for c in id2)


def test_custom_random():
    """Test custom random function with PRNG (similar to ulid style)."""
    import random

    # Create a deterministic PRNG for testing
    prng = random.Random(42).random

    generator = nanoid.custom_random(nanoid.URL_ALPHABET, 21, prng)
    id1 = generator()

    # Reset PRNG with same seed
    prng = random.Random(42).random
    generator2 = nanoid.custom_random(nanoid.URL_ALPHABET, 21, prng)
    id2 = generator2()

    # Both should be the same because we're using deterministic random
    assert id1 == id2
    assert len(id1) == 21


def test_custom_random_with_custom_size():
    """Test custom random with different sizes."""
    import random

    prng = random.Random(42).random
    generator = nanoid.custom_random("0123456789", 10, prng)

    id1 = generator()
    assert len(id1) == 10
    assert all(c in "0123456789" for c in id1)

    id2 = generator(5)
    assert len(id2) == 5
    assert all(c in "0123456789" for c in id2)


def test_collision_resistance():
    """Test that IDs have low collision probability."""
    # Generate a large number of IDs and check for uniqueness
    ids = {nanoid.generate() for _ in range(10000)}
    assert len(ids) == 10000, "Should have no collisions in 10,000 IDs"


def test_alphabet_validation():
    """Test alphabet validation."""
    import pytest

    # Empty alphabet should raise error
    with pytest.raises(ValueError, match="Alphabet must contain between 1 and 256 symbols"):
        generator = nanoid.custom_alphabet("")
        generator()

    # Alphabet too large should raise error
    with pytest.raises(ValueError, match="Alphabet must contain between 1 and 256 symbols"):
        generator = nanoid.custom_alphabet("a" * 257)
        generator()


def test_size_validation():
    """Test size validation."""
    import pytest

    generator = nanoid.custom_alphabet("0123456789")

    # Zero size should raise error
    with pytest.raises(ValueError, match="Size must be positive"):
        generator(0)

    # Negative size should raise error
    with pytest.raises(ValueError, match="Size must be positive"):
        generator(-1)


def test_single_character_alphabet():
    """Test with single character alphabet."""
    generator = nanoid.custom_alphabet("A", 10)
    id1 = generator()
    assert id1 == "AAAAAAAAAA"


def test_distribution():
    """Test that character distribution is reasonably uniform."""
    # Generate many short IDs and check distribution
    alphabet = "0123456789"
    generator = nanoid.custom_alphabet(alphabet, 10)
    counts = dict.fromkeys(alphabet, 0)

    for _ in range(1000):
        id_str = generator()
        for c in id_str:
            counts[c] += 1

    # Each character should appear roughly 1000 times (10% of 10000)
    # Allow for some variance (between 800 and 1200)
    for c, count in counts.items():
        assert 800 <= count <= 1200, f"Character {c} appeared {count} times (expected ~1000)"


def test_deterministic_with_seed():
    """Test that same seed produces same sequence of IDs."""
    import random

    # Create two generators with same seed
    prng1 = random.Random(12345).random
    gen1 = nanoid.custom_random(nanoid.URL_ALPHABET, 21, prng1)

    prng2 = random.Random(12345).random
    gen2 = nanoid.custom_random(nanoid.URL_ALPHABET, 21, prng2)

    # Generate multiple IDs from each
    ids1 = [gen1() for _ in range(10)]
    ids2 = [gen2() for _ in range(10)]

    # All IDs should match
    assert ids1 == ids2
