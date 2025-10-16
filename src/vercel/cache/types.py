from __future__ import annotations

from typing import Any, Protocol, Sequence


class Cache(Protocol):
    def delete(self, key: str) -> None: ...

    def get(self, key: str) -> object | None: ...

    def __contains__(self, key: str) -> bool: ...

    def set(
        self,
        key: str,
        value: object,
        options: dict | None = None,
    ) -> None: ...

    def expire_tag(self, tag: str | Sequence[str]) -> None: ...


class AsyncCache(Protocol):
    async def delete(self, key: str) -> None: ...

    async def get(self, key: str) -> object | None: ...

    async def set(
        self,
        key: str,
        value: object,
        options: dict | None = None,
    ) -> None: ...

    async def expire_tag(self, tag: str | Sequence[str]) -> None: ...

    async def contains(self, key: str) -> bool: ...


class PurgeAPI(Protocol):
    """Protocol for the purge API object."""

    def invalidate_by_tag(self, tag: str | list[str]) -> Any:
        """Invalidate cache entries by tag."""
        ...

    def dangerously_delete_by_tag(
        self,
        tag: str | list[str],
        *,
        revalidation_deadline_seconds: int | None = None,
    ) -> Any:
        """Dangerously delete cache entries by tag."""
        ...


class AsyncPurgeAPI(Protocol):
    async def invalidate_by_tag(self, tag: str | list[str]) -> Any: ...

    async def dangerously_delete_by_tag(
        self,
        tag: str | list[str],
        *,
        revalidation_deadline_seconds: int | None = None,
    ) -> Any: ...
