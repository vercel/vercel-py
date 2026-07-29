"""Bounded, single-flight token cache.

Deliberately not a port of the TypeScript cache, which has five known defects:
an order-sensitive `JSON.stringify` key, `validityBufferMs` inside the key, a key
that omits the platform identity (so an overridden token can be served a
credential minted for a different identity), global `clear()` on revoke, and no
in-flight de-duplication.
"""

import hashlib
import json
import threading
import time
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import asdict, is_dataclass
from typing import Any

from vercel.connect._internal.models import (
    ConnectAuthorizationDetail,
    ConnectTokenSubject,
)
from vercel.connect._internal.state import ConnectTokenState


class TokenCacheKey:
    """Canonical, order-independent identity of a token request.

    Includes the connector, the subject, the installation, and every field that
    changes which credential the server would mint, plus a SHA-256 of the
    effective platform identity token so two identities can never share an entry.
    Excludes read-time policy such as the validity buffer.
    """

    __slots__ = ("_value", "_identity")

    def __init__(self, value: str, identity: str) -> None:
        self._value = value
        self._identity = identity

    @property
    def identity(self) -> str:
        """The connector, subject, and installation this entry belongs to."""
        return self._identity

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TokenCacheKey) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        # The identity token is only ever present as a digest, never verbatim.
        return f"TokenCacheKey({self._value!r})"


def _canonical(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _canonical(asdict(value))
    if isinstance(value, dict):
        return {name: _canonical(value[name]) for name in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value


def _identity_of(
    connector: str,
    subject: ConnectTokenSubject,
    installation_id: str | None,
) -> str:
    return json.dumps(
        {
            "connector": connector,
            "subject": _canonical(subject),
            "installationId": installation_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def build_cache_key(
    connector: str,
    *,
    subject: ConnectTokenSubject,
    vercel_token: str,
    scopes: Sequence[str] | None = None,
    installation_id: str | None = None,
    audience: Sequence[str] | None = None,
    resources: Sequence[str] | None = None,
    authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
) -> TokenCacheKey:
    """Build a canonical cache key. Permuted inputs must produce equal keys."""
    payload = {
        "connector": connector,
        "subject": _canonical(subject),
        "installationId": installation_id,
        # Scope order does not change which credential is minted, so it must not
        # change the key either.
        "scopes": None if scopes is None else sorted(scopes),
        "audience": None if audience is None else sorted(audience),
        "resources": None if resources is None else sorted(resources),
        "authorizationDetails": (
            None
            if authorization_details is None
            else sorted(
                json.dumps(_canonical(detail), sort_keys=True, separators=(",", ":"))
                for detail in authorization_details
            )
        ),
        # Hash, never store: two platform identities must never share an entry,
        # and the raw token must not be retrievable from the cache.
        "identity": hashlib.sha256(vercel_token.encode()).hexdigest(),
    }
    value = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return TokenCacheKey(value, _identity_of(connector, subject, installation_id))


class TokenCache:
    """An LRU token cache with per-key single-flight resolution."""

    def __init__(self, *, max_size: int) -> None:
        self._max_size = max_size
        self._entries: OrderedDict[TokenCacheKey, ConnectTokenState] = OrderedDict()
        self._lock = threading.RLock()
        self._in_flight: dict[TokenCacheKey, Any] = {}

    def get(
        self,
        key: TokenCacheKey,
        *,
        validity_buffer_seconds: float,
    ) -> ConnectTokenState | None:
        """Return a cached token still valid beyond the buffer, else None."""
        with self._lock:
            state = self._entries.get(key)
            if state is None:
                return None
            if state.expires_at.timestamp() - time.time() <= validity_buffer_seconds:
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
            return state

    def set(self, key: TokenCacheKey, value: ConnectTokenState) -> None:
        """Store a token, evicting the least recently used entry when full."""
        with self._lock:
            self._entries[key] = value
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_size:
                # O(1) eviction; the TypeScript implementation scans.
                self._entries.popitem(last=False)

    def delete(self, key: TokenCacheKey) -> bool:
        """Drop exactly one entry. Returns whether an entry was removed."""
        with self._lock:
            return self._entries.pop(key, None) is not None

    def delete_by_identity(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        installation_id: str | None = None,
    ) -> int:
        """Drop every entry for a connector/subject/installation identity.

        Scoped invalidation for revocation: unlike the TypeScript SDK this never
        clears unrelated entries.
        """
        identity = _identity_of(connector, subject, installation_id)
        with self._lock:
            doomed = [key for key in self._entries if key.identity == identity]
            for key in doomed:
                del self._entries[key]
            return len(doomed)

    def clear(self) -> None:
        """Drop every entry."""
        with self._lock:
            self._entries.clear()

    def lock_for(self, key: TokenCacheKey) -> Any:
        """Return the per-key lock used to collapse concurrent cold fetches."""
        with self._lock:
            existing = self._in_flight.get(key)
            if existing is None:
                existing = threading.RLock()
                self._in_flight[key] = existing
            return existing

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


__all__ = ["TokenCache", "TokenCacheKey", "build_cache_key"]
