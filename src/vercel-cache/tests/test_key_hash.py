from __future__ import annotations

import pytest

from vercel.cache.utils import create_key_transformer, default_key_hash_function

# Expected digests produced by the TypeScript `defaultKeyHashFunction` in
# `@vercel/functions` (`@noble/hashes` sha256 over `TextEncoder` UTF-8 bytes).
# Python and JavaScript must hash keys identically so they share cache entries.
JS_VECTORS = [
    ("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
    ("key", "2c70e12b7a0646f92279f427c7b38e7334d8e5389cff167a1dc30e73f826b683"),
    ("my-key", "5e78863ed1ffb9fc66b1d61634b126bf8eb20267e7996297eeeb9b19c8c0f732"),
    ("héllo wörld", "a1003f7d04a4115711d0b48a2eaf1359ce565d2d2a6fd65098dfcffadeeef59f"),
    ("日本語のキャッシュキー", "f8bf4d1e6fe2cf74267bc7ff16ac8165f4bc50bdef92acffe8059ea3dcb4b2a4"),
    ("🚀 emoji key 🔑", "51667eebfa1561b4d2babc81a5063ae2070794c5cea379a234c3e6ece68e3673"),
    # Lone surrogate: JS TextEncoder encodes it as U+FFFD.
    ("a\ud800b", "05087813392efc16fe8ff448920c6328e53af865df39419436659d9ffda90f7b"),
]


@pytest.mark.parametrize(("key", "expected"), JS_VECTORS)
def test_default_key_hash_matches_js(key: str, expected: str) -> None:
    assert default_key_hash_function(key) == expected


def test_default_key_hash_does_not_collide() -> None:
    count = 100_000
    hashes = {default_key_hash_function(f"key-{i}") for i in range(count)}
    assert len(hashes) == count


def test_namespace_prefixes_hashed_key() -> None:
    make = create_key_transformer(None, "ns", None)
    assert make("key") == "ns$2c70e12b7a0646f92279f427c7b38e7334d8e5389cff167a1dc30e73f826b683"
