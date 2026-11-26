import hashlib
import re
from collections.abc import Callable

_DEFAULT_NAMESPACE_SEPARATOR = "$"


# Python strings can hold lone surrogates, which are not encodable as UTF-8.
# JS ``TextEncoder`` replaces them with U+FFFD, so do the same to keep keys
# identical across runtimes.
_LONE_SURROGATE_RE = re.compile("[\ud800-\udfff]")


def default_key_hash_function(key: str) -> str:
    """Hash a cache key with SHA-256 and return the lowercase hex digest.

    Mirrors the TypeScript ``defaultKeyHashFunction`` in ``@vercel/functions``
    (``sha256(utf8(key))`` hex) so Python and JavaScript produce the same keys.
    """
    try:
        data = key.encode("utf-8")
    except UnicodeEncodeError:
        data = _LONE_SURROGATE_RE.sub("\ufffd", key).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


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
