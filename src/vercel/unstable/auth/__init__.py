"""Minimal auth types for unstable Sandbox credential policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar


@dataclass(frozen=True, slots=True)
class OIDCCredentials:
    token: str


@dataclass(frozen=True, slots=True)
class AccessTokenCredentials:
    token: str
    team_id: str | None = None
    project_id: str | None = None


CredentialsT = TypeVar("CredentialsT", OIDCCredentials, AccessTokenCredentials, covariant=True)


class CredentialProvider(Protocol[CredentialsT]):
    async def resolve(self) -> CredentialsT: ...


@dataclass(frozen=True, slots=True)
class StaticCredentialProvider(Generic[CredentialsT]):
    credentials: CredentialsT

    async def resolve(self) -> CredentialsT:
        return self.credentials


__all__ = [
    "AccessTokenCredentials",
    "CredentialProvider",
    "OIDCCredentials",
    "StaticCredentialProvider",
]
