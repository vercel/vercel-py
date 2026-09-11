"""HTTP headers for proxy request inspection."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, TypeVar, overload

_T = TypeVar("_T")

__all__ = ["Headers"]


class Headers(Mapping[str, str]):
    """Immutable, case-insensitive HTTP headers."""

    __slots__ = ("_store",)
    _store: tuple[tuple[str, str], ...]

    def __init__(self, store: tuple[tuple[str, str], ...]) -> None:
        object.__setattr__(self, "_store", store)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__!r} object is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__!r} object is immutable")

    def __getitem__(self, key: str) -> str:
        k = key.lower()
        for stored_key, value in self._store:
            if stored_key == k:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        seen: set[str] = set()
        for k, _ in self._store:
            if k not in seen:
                seen.add(k)
                yield k

    def __len__(self) -> int:
        return len({k for k, _ in self._store})

    @overload
    def get(self, key: str) -> str | None: ...
    @overload
    def get(self, key: str, default: str) -> str: ...
    @overload
    def get(self, key: str, default: _T) -> str | _T: ...
    def get(self, key: str, default: str | _T | None = None) -> str | _T | None:
        """Return the first value for *key*, or *default* if absent."""
        k = key.lower()
        for stored_key, value in self._store:
            if stored_key == k:
                return value
        return default

    def get_all(self, key: str) -> list[str]:
        """Return all values for *key* (useful for ``Set-Cookie`` etc.)."""
        k = key.lower()
        return [v for sk, v in self._store if sk == k]

    @classmethod
    def from_asgi(cls, raw: list[tuple[bytes, bytes]]) -> Headers:
        """Construct from an ASGI headers list."""
        return cls(tuple((k.decode("latin-1").lower(), v.decode("latin-1")) for k, v in raw))

    def __repr__(self) -> str:
        d: dict[str, str | list[str]] = {}
        for k, v in self._store:
            if k in d:
                existing = d[k]
                if isinstance(existing, list):
                    existing.append(v)
                else:
                    d[k] = [existing, v]
            else:
                d[k] = v
        return f"{type(self).__name__}({d!r})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Headers):
            return self._store == other._store
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._store)
