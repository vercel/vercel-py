"""Tests for monotonic ULID generation."""

import pytest

from vercel.workflow.ulid import monotonic_factory


class TestMonotonicFactory:
    """Test the monotonic_factory function."""

    def test_basic_generation(self):
        """Test basic ULID generation."""
        ulid_gen = monotonic_factory()
        ulid1 = ulid_gen()
        ulid2 = ulid_gen()

        # Both should be valid 26-character strings
        assert len(ulid1) == 26
        assert len(ulid2) == 26

        # Should be monotonically increasing
        assert ulid1 < ulid2

    def test_with_timestamp(self):
        """Test ULID generation with specific timestamp."""
        ulid_gen = monotonic_factory()

        timestamp_ms = 1469918176385
        ulid1 = ulid_gen(timestamp_ms)

        # Should be valid
        assert len(ulid1) == 26

        # Time part should match (first 10 characters)
        assert ulid1.startswith("01ARYZ6S41")

    def test_monotonic_with_same_timestamp(self):
        """Test that ULIDs remain monotonic even with same timestamp."""
        ulid_gen = monotonic_factory()

        timestamp_ms = 1469918176385
        ulid1 = ulid_gen(timestamp_ms)
        ulid2 = ulid_gen(timestamp_ms)  # Same timestamp
        ulid3 = ulid_gen(timestamp_ms)  # Same timestamp again

        # All should have same time part
        assert ulid1[:10] == ulid2[:10] == ulid3[:10]

        # But should be monotonically increasing
        assert ulid1 < ulid2 < ulid3

    def test_monotonic_with_backwards_time(self):
        """Test that ULIDs remain monotonic even when time goes backwards."""
        ulid_gen = monotonic_factory()

        # Generate with a timestamp
        ulid1 = ulid_gen(1469918176385)

        # Generate with an earlier timestamp - should still be monotonic
        ulid2 = ulid_gen(100000000)
        ulid3 = ulid_gen(10000)

        # Should all be monotonically increasing despite backwards timestamps
        assert ulid1 < ulid2 < ulid3

        # ulid2 and ulid3 should use the same timestamp as ulid1
        assert ulid1[:10] == ulid2[:10] == ulid3[:10]

    def test_monotonic_with_forward_time(self):
        """Test that ULIDs use new timestamp when time moves forward."""
        ulid_gen = monotonic_factory()

        timestamp1 = 1469918176385
        timestamp2 = 1469918176386  # 1ms later

        ulid1 = ulid_gen(timestamp1)
        ulid2 = ulid_gen(timestamp2)

        # Time parts should be different
        assert ulid1[:10] != ulid2[:10]
        assert ulid1[:10] == "01ARYZ6S41"
        assert ulid2[:10] == "01ARYZ6S42"

        # Should be monotonically increasing
        assert ulid1 < ulid2

    def test_with_custom_prng(self):
        """Test ULID generation with custom PRNG for reproducibility."""

        def fixed_prng():
            return 0.96

        ulid_gen = monotonic_factory(fixed_prng)

        ulid1 = ulid_gen(1469918176385)
        ulid2 = ulid_gen(1469918176385)
        ulid3 = ulid_gen(100000000)
        ulid4 = ulid_gen(10000)
        ulid5 = ulid_gen(1469918176386)

        # These should match the JavaScript implementation exactly
        assert ulid1 == "01ARYZ6S41YYYYYYYYYYYYYYYY"
        assert ulid2 == "01ARYZ6S41YYYYYYYYYYYYYYYZ"
        assert ulid3 == "01ARYZ6S41YYYYYYYYYYYYYYZ0"
        assert ulid4 == "01ARYZ6S41YYYYYYYYYYYYYYZ1"
        assert ulid5 == "01ARYZ6S42YYYYYYYYYYYYYYYY"

    def test_javascript_compatibility(self):
        """Test that output matches JavaScript ULID implementation.

        This test replicates the JavaScript test from:
        https://github.com/ulid/javascript/blob/master/test/node/ulid.spec.ts
        """

        def stubbed_prng():
            return 0.96

        ulid_gen = monotonic_factory(stubbed_prng)

        # First call
        ulid1 = ulid_gen(1469918176385)
        assert ulid1 == "01ARYZ6S41YYYYYYYYYYYYYYYY"

        # Second call with same timestamp
        ulid2 = ulid_gen(1469918176385)
        assert ulid2 == "01ARYZ6S41YYYYYYYYYYYYYYYZ"

        # Third call with LOWER timestamp
        ulid3 = ulid_gen(100000000)
        assert ulid3 == "01ARYZ6S41YYYYYYYYYYYYYYZ0"

        # Fourth call with even LOWER timestamp
        ulid4 = ulid_gen(10000)
        assert ulid4 == "01ARYZ6S41YYYYYYYYYYYYYYZ1"

        # Fifth call with HIGHER timestamp
        ulid5 = ulid_gen(1469918176386)
        assert ulid5 == "01ARYZ6S42YYYYYYYYYYYYYYYY"

        # Verify all are monotonically increasing
        ulids = [ulid1, ulid2, ulid3, ulid4, ulid5]
        for i in range(len(ulids) - 1):
            assert ulids[i] < ulids[i + 1], f"Not monotonic: {ulids[i]} >= {ulids[i + 1]}"

    def test_invalid_timestamp_negative(self):
        """Test that negative timestamps raise an error."""
        ulid_gen = monotonic_factory()

        with pytest.raises(ValueError, match="Timestamp must be >= 0"):
            ulid_gen(-1)

    def test_invalid_timestamp_too_large(self):
        """Test that timestamps larger than max raise an error."""
        ulid_gen = monotonic_factory()

        # 2^48 - 1 is the max timestamp
        max_timestamp = 281474976710655

        with pytest.raises(ValueError, match="Timestamp must be <="):
            ulid_gen(max_timestamp + 1)

    def test_separate_factories_are_independent(self):
        """Test that separate factory instances maintain independent state."""
        ulid_gen1 = monotonic_factory()
        ulid_gen2 = monotonic_factory()

        timestamp = 1469918176385

        ulid1a = ulid_gen1(timestamp)
        ulid2a = ulid_gen2(timestamp)

        # Same timestamp but different factories - random parts will differ
        assert ulid1a[:10] == ulid2a[:10]  # Same time part
        # Random parts may differ (unless very unlucky with randomness)

        # But each factory should maintain its own monotonicity
        ulid1b = ulid_gen1(timestamp)
        ulid2b = ulid_gen2(timestamp)

        assert ulid1a < ulid1b
        assert ulid2a < ulid2b

    def test_overflow_handling(self):
        """Test that random part overflow increments timestamp."""
        ulid_gen = monotonic_factory()

        timestamp = 1000000

        # To test overflow, we'd need to generate 2^80 ULIDs which is impractical
        # Instead, we just verify the logic doesn't crash with many same-timestamp calls
        ulids = [ulid_gen(timestamp) for _ in range(100)]

        # All should be monotonically increasing
        for i in range(len(ulids) - 1):
            assert ulids[i] < ulids[i + 1]

    def test_ulid_format(self):
        """Test that generated ULIDs use only valid Crockford Base32 characters."""
        ulid_gen = monotonic_factory()

        ulid = ulid_gen()

        # Should be 26 characters
        assert len(ulid) == 26

        # Should only contain Crockford Base32 characters
        valid_chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        for char in ulid:
            assert char in valid_chars, f"Invalid character: {char}"
