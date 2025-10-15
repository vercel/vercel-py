from typing import Callable

_DEFAULT_NAMESPACE_SEPARATOR = "$"


def default_key_hash_function(key: str) -> str:
    # Mirror TS defaultKeyHashFunction: djb2 xor variant, 32-bit unsigned hex
    h = 5381
    for ch in key:
        h = ((h * 33) ^ ord(ch)) & 0xFFFFFFFF
    return format(h, "x")


def create_key_transformer(
    key_fn: Callable[[str], str] | None,
    ns: str | None,
    sep: str | None,
) -> Callable[[str], str]:
    key_fn = key_fn or default_key_hash_function
    sep = sep or _DEFAULT_NAMESPACE_SEPARATOR

    def make(key: str) -> str:
        if not ns:
            return key_fn(key)
        return f"{ns}{sep}{key_fn(key)}"

    return make
