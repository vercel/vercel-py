"""Immutable string-to-string mapping for path and query parameters."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, TypeVar, overload

__all__ = ["Params"]

_T = TypeVar("_T")


class Params(Mapping[str, str]):
    """Immutable mapping of URL parameters."""

    __slots__ = ("_store",)
    _store: tuple[tuple[str, str], ...]

    def __init__(self, store: tuple[tuple[str, str], ...]) -> None:
        object.__setattr__(self, "_store", store)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__!r} object is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__!r} object is immutable")

    def __getitem__(self, key: str) -> str:
        for k, v in self._store:
            if k == key:
                return v
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
        for k, v in self._store:
            if k == key:
                return v
        return default

    def __repr__(self) -> str:
        return f"{type(self).__name__}({dict(self)!r})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Params):
            return self._store == other._store
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._store)
