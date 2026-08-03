"""Stable cache identity for a rotating platform token."""

import hashlib
import inspect
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

DEFAULT_IDENTITY_CACHE_SIZE = 32
# Matched to the verifier's clock-skew leeway, so re-verification adds no window
# an expired token did not already have.
DEFAULT_IDENTITY_TTL_SECONDS = 60.0


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class IdentityResolver:
    """Resolves the stable identity a platform token names, and remembers it.

    A Vercel OIDC token is a signature over an identity plus an expiry, so the
    identity outlives the token: a deployment is handed a fresh one whenever the
    previous nears expiry. Keying the token cache on the token would therefore
    discard every cached credential on each refresh, and leave the dead entries
    occupying the LRU until evicted.

    The identity comes from *verified* claims only. Deriving it from unverified
    claims would let a forged token that merely asserts an identity be served the
    credentials cached under the real one, without the Connect API ever seeing it.
    Anything that does not verify — an opaque token, an expired one, an unreachable
    JWKS — falls back to that token's own digest, which no other token can
    reproduce, so a failure costs one extra mint rather than risking disclosure.

    The memo expires so a token that verified once stops naming a live identity
    after it expires, and so a token that failed during a JWKS outage gets another
    chance. Re-verification is a signature check against an already-cached JWKS,
    tens of microseconds.
    """

    __slots__ = ("_resolve_identity", "_max_size", "_ttl", "_entries", "_lock")

    def __init__(
        self,
        *,
        resolve_identity: Callable[[str], Any],
        max_size: int = DEFAULT_IDENTITY_CACHE_SIZE,
        ttl_seconds: float = DEFAULT_IDENTITY_TTL_SECONDS,
    ) -> None:
        self._resolve_identity = resolve_identity
        self._max_size = max_size
        self._ttl = ttl_seconds
        # Keyed by digest, never by the token: this mapping outlives every call.
        self._entries: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._lock = threading.Lock()

    async def resolve(self, token: str) -> str:
        """Return an opaque identity for `token`, verifying it at most once per TTL."""
        digest = token_digest(token)
        with self._lock:
            entry = self._entries.get(digest)
            if entry is not None:
                identity, deadline = entry
                if time.monotonic() < deadline:
                    self._entries.move_to_end(digest)
                    return identity
                del self._entries[digest]

        identity = await self._verified_identity(token, digest)

        with self._lock:
            self._entries[digest] = (identity, time.monotonic() + self._ttl)
            self._entries.move_to_end(digest)
            while len(self._entries) > self._max_size:
                self._entries.popitem(last=False)
        return identity

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    async def _verified_identity(self, token: str, digest: str) -> str:
        try:
            result = self._resolve_identity(token)
            # The sync surface resolves a blocking implementation, the async
            # surface an awaitable one.
            identity = await result if inspect.isawaitable(result) else result
        except Exception:
            # Never fatal: the API remains the authority on whether a token works.
            return f"token:{digest}"
        if not isinstance(identity, str) or not identity:
            return f"token:{digest}"
        # Namespaced so a verified identity cannot collide with a token digest.
        return f"oidc:{identity}"


__all__ = [
    "DEFAULT_IDENTITY_CACHE_SIZE",
    "DEFAULT_IDENTITY_TTL_SECONDS",
    "IdentityResolver",
    "token_digest",
]
