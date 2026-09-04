from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

__all__ = ["Headers"]


class Headers(Mapping[str, str]):
    """Immutable, case-insensitive HTTP headers with typed accessors.

    Implements :class:`collections.abc.Mapping` — keys are normalised to
    lowercase on construction. Neither the mapping nor its internal storage
    can be mutated after construction. Use :meth:`get_all` to retrieve
    multi-value headers (e.g. ``Set-Cookie``).
    """

    def __getitem__(self, key: str) -> str:  # type: ignore[empty-body]
        raise NotImplementedError

    def __iter__(self) -> Iterator[str]:  # type: ignore[empty-body]
        raise NotImplementedError

    def __len__(self) -> int:  # type: ignore[empty-body]
        raise NotImplementedError

    def get(self, key: str, default: str | None = None) -> str | None:  # type: ignore[override]
        """Return the first value for *key*, or *default* if absent."""
        raise NotImplementedError

    def get_all(self, key: str) -> list[str]:
        """Return all values for *key* (useful for ``Set-Cookie`` etc.)."""
        raise NotImplementedError

    @property
    def content_type(self) -> str | None:
        raise NotImplementedError

    @property
    def authorization(self) -> str | None:
        raise NotImplementedError

    @property
    def host(self) -> str | None:
        raise NotImplementedError

    @property
    def x_forwarded_for(self) -> str | None:
        raise NotImplementedError

    @property
    def x_forwarded_proto(self) -> str | None:
        raise NotImplementedError

    @property
    def cookie(self) -> str | None:
        raise NotImplementedError

    @classmethod
    def from_asgi(cls, raw: list[tuple[bytes, bytes]]) -> Headers:
        """Construct from an ASGI headers list."""
        raise NotImplementedError

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> Headers:
        """Construct from a plain string dict (keys normalised to lowercase)."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{type(self).__name__}(...)"

    def __eq__(self, other: Any) -> bool:  # type: ignore[empty-body]
        raise NotImplementedError

    def __hash__(self) -> int:  # type: ignore[empty-body]
        raise NotImplementedError
