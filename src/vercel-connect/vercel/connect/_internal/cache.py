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
from typing import Any

from vercel.connect._internal.models import (
    ConnectAuthorizationDetail,
    ConnectTokenSubject,
)
from vercel.connect._internal.state import ConnectTokenState
from vercel.connect._internal.wire import (
    serialize_authorization_detail,
    serialize_subject,
)


class _PendingLoad:
    """Bookkeeping for one key with loads in flight.

    `epoch` is bumped by every invalidation of this key. A load carries the epoch
    it started under, so an invalidation cancels the loads that were already
    running without also cancelling loads that begin afterwards.
    """

    __slots__ = ("refs", "epoch")

    def __init__(self) -> None:
        self.refs = 0
        self.epoch = 0


class TokenCacheKey:
    """Canonical, order-independent identity of a token request.

    Includes the connector, the subject, the installation, and every field that
    changes which credential the server would mint, plus a SHA-256 of the
    effective platform identity token so two identities can never share an entry.
    Excludes read-time policy such as the validity buffer.

    Credentials are never embedded verbatim: both the platform identity token and
    an inbound token-exchange credential are reduced to digests, so a key is safe
    to log.
    """

    __slots__ = ("_value", "_connector", "_subject_key", "_installation_id")

    def __init__(
        self,
        value: str,
        *,
        connector: str,
        subject_key: str,
        installation_id: str | None,
    ) -> None:
        self._value = value
        self._connector = connector
        self._subject_key = subject_key
        self._installation_id = installation_id

    @property
    def connector(self) -> str:
        return self._connector

    @property
    def subject_key(self) -> str:
        """Canonical subject identity, with any credential reduced to a digest."""
        return self._subject_key

    @property
    def installation_id(self) -> str | None:
        return self._installation_id

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TokenCacheKey) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        # Only a digest: the full value carries subject fields, and diagnostics
        # must never become a credential disclosure path.
        digest = hashlib.sha256(self._value.encode()).hexdigest()[:16]
        return f"TokenCacheKey({self._connector!r}, digest={digest})"


def _canonical(value: Any) -> Any:
    """Recursively order a JSON-shaped value so equal inputs hash equally."""
    if isinstance(value, dict):
        return {name: _canonical(value[name]) for name in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _canonical_subject(subject: ConnectTokenSubject) -> Any:
    """Canonicalize a subject, replacing any credential with a digest.

    Built from the wire form so the key cannot disagree with the request about
    what the subject is. A token-exchange subject carries a live bearer
    credential: it has to participate in the key so two inbound tokens never
    share an entry, but it must not be stored, so only its digest is kept.
    """
    canonical = _canonical(serialize_subject(subject))
    if isinstance(canonical, dict) and "token" in canonical:
        token = canonical["token"]
        canonical = {name: value for name, value in canonical.items() if name != "token"}
        canonical["tokenDigest"] = _digest(token) if isinstance(token, str) else None
    return canonical


def _subject_key(subject: ConnectTokenSubject) -> str:
    return json.dumps(_canonical_subject(subject), sort_keys=True, separators=(",", ":"))


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
        "subject": _canonical_subject(subject),
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
                # Keyed by the wire form: two requests the server cannot tell
                # apart must not occupy separate cache entries.
                json.dumps(
                    _canonical(serialize_authorization_detail(detail)),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                for detail in authorization_details
            )
        ),
        # Hash, never store: two platform identities must never share an entry,
        # and the raw token must not be retrievable from the cache.
        "identity": hashlib.sha256(vercel_token.encode()).hexdigest(),
    }
    value = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return TokenCacheKey(
        value,
        connector=connector,
        subject_key=_subject_key(subject),
        installation_id=installation_id,
    )


class TokenCache:
    """An LRU token cache with per-key single-flight resolution."""

    def __init__(self, *, max_size: int) -> None:
        self._max_size = max_size
        self._entries: OrderedDict[TokenCacheKey, ConnectTokenState] = OrderedDict()
        self._lock = threading.RLock()
        # In-flight loads, so an invalidation can reach a credential that is being
        # fetched but is not in the cache yet. Tracked per key: a global counter
        # would let one key's eviction discard an unrelated key's fresh token.
        self._pending: dict[TokenCacheKey, _PendingLoad] = {}

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

    def begin_load(self, key: TokenCacheKey) -> int:
        """Register an in-flight load and return the epoch it starts under."""
        with self._lock:
            pending = self._pending.get(key)
            if pending is None:
                pending = _PendingLoad()
                self._pending[key] = pending
            pending.refs += 1
            return pending.epoch

    def finish_load(self, key: TokenCacheKey) -> None:
        """Release an in-flight load registration."""
        with self._lock:
            pending = self._pending.get(key)
            if pending is None:
                return
            pending.refs -= 1
            if pending.refs <= 0:
                del self._pending[key]

    def set(self, key: TokenCacheKey, value: ConnectTokenState, *, epoch: int) -> bool:
        """Store a token, evicting the least recently used entry when full.

        Returns False without storing when this key was invalidated after the
        load began, so a revoked credential is never resurrected. A load that
        started after the invalidation carries the current epoch and is stored
        normally.
        """
        with self._lock:
            pending = self._pending.get(key)
            if pending is not None and pending.epoch != epoch:
                return False
            self._entries[key] = value
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_size:
                # O(1) eviction; the TypeScript implementation scans.
                self._entries.popitem(last=False)
            return True

    def delete(self, key: TokenCacheKey) -> bool:
        """Drop exactly one entry. Returns whether an entry was removed."""
        with self._lock:
            removed = self._entries.pop(key, None) is not None
            pending = self._pending.get(key)
            if pending is not None:
                pending.epoch += 1
            return removed

    def delete_by_identity(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        installation_id: str | None = None,
    ) -> int:
        """Drop every entry for a connector and subject.

                `installation_id=None` means every installation, matching the server's
                revocation semantics, rather than only entries that were cached without
                an installation. Scoped invalidation: unlike the TypeScript SDK this
                never clears unrelated connectors or subjects.

        A connector has two names, an opaque id (`scl_...`) and a readable UID
                (`slack/my-bot`). Cached responses carry both, so an entry stored under one
                name is still evicted by a call naming the other. An in-flight load has no
                response yet, so it is matched on the name used at call time only.
        """
        subject_key = _subject_key(subject)
        with self._lock:
            doomed = [
                key
                for key, state in self._entries.items()
                if self._names_match(key, state, connector)
                and key.subject_key == subject_key
                and (installation_id is None or key.installation_id == installation_id)
            ]
            for key in doomed:
                del self._entries[key]
            # An in-flight load has stored nothing yet, so matching it here is the
            # only way a revoke can stop it from caching a revoked credential.
            for key, pending in self._pending.items():
                if (
                    key.connector == connector
                    and key.subject_key == subject_key
                    and (installation_id is None or key.installation_id == installation_id)
                ):
                    pending.epoch += 1
            return len(doomed)

    @staticmethod
    def _names_match(key: TokenCacheKey, state: ConnectTokenState, connector: str) -> bool:
        """Whether `connector` names the same connector as this entry."""
        return connector in (key.connector, state.connector.id, state.connector.uid)

    def clear(self) -> None:
        """Drop every entry."""
        with self._lock:
            self._entries.clear()
            for pending in self._pending.values():
                pending.epoch += 1

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


__all__ = ["TokenCache", "TokenCacheKey", "build_cache_key"]
