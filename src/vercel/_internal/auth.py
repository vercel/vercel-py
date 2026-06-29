"""Small auth helpers shared by service-specific request clients."""

from __future__ import annotations

from typing import Protocol


class TokenProvider(Protocol):
    async def __call__(self) -> str: ...


def static_token_provider(token: str) -> TokenProvider:
    async def _provider() -> str:
        return token

    return _provider


__all__ = [
    "TokenProvider",
    "static_token_provider",
]
