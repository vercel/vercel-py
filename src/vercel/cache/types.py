from __future__ import annotations

from typing import Protocol, Sequence


class RuntimeCache(Protocol):
    async def delete(self, key: str) -> None:
        ...

    async def get(self, key: str) -> object | None:
        ...

    async def set(
        self,
        key: str,
        value: object,
        options: dict | None = None,
    ) -> None:
        ...

    async def expire_tag(self, tag: str | Sequence[str]) -> None:
        ...
