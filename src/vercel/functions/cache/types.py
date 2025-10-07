from __future__ import annotations

from typing import Protocol, Sequence


class RuntimeCache(Protocol):
    async def delete(self, key: str) -> None:  # noqa: D401
        ...

    async def get(self, key: str) -> object | None:  # noqa: D401
        ...

    async def set(
        self,
        key: str,
        value: object,
        options: dict | None = None,
    ) -> None:  # noqa: D401
        ...

    async def expire_tag(self, tag: str | Sequence[str]) -> None:  # noqa: D401
        ...
