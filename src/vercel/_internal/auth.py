"""Small auth protocols shared by service-specific request clients."""

from __future__ import annotations

from typing import Protocol


class TokenProvider(Protocol):
    async def __call__(self) -> str: ...


__all__ = ["TokenProvider"]
