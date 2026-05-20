"""Internal auth primitives for unstable service credential policy."""

from __future__ import annotations

import base64
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

from vercel._internal.unstable.errors import (
    AuthError,
    CredentialProviderError,
    CredentialResolutionError,
)
from vercel._internal.unstable.settings import MISSING, SettingSource


@dataclass(frozen=True, slots=True)
class OIDCCredentials:
    token: str
    project_id: str
    team_id: str


@dataclass(frozen=True, slots=True)
class AccessTokenCredentials:
    token: str
    project_id: str
    team_id: str


Credentials = OIDCCredentials | AccessTokenCredentials

CredentialsT = TypeVar(
    "CredentialsT",
    OIDCCredentials,
    AccessTokenCredentials,
    Credentials,
    covariant=True,
)


class CredentialProvider(Protocol[CredentialsT]):
    async def resolve(self) -> CredentialsT: ...


class SyncCredentialProvider(Protocol[CredentialsT]):
    def resolve(self) -> CredentialsT: ...


@dataclass(frozen=True, slots=True)
class StaticCredentialProvider(Generic[CredentialsT]):
    credentials: CredentialsT

    async def resolve(self) -> CredentialsT:
        return self.credentials


@dataclass(frozen=True, slots=True)
class SyncStaticCredentialProvider(Generic[CredentialsT]):
    credentials: CredentialsT

    def resolve(self) -> CredentialsT:
        return self.credentials


_CredentialsT = TypeVar("_CredentialsT")


async def resolve_provider(resolve: Callable[[], Awaitable[_CredentialsT]]) -> _CredentialsT:
    try:
        return await resolve()
    except (CredentialResolutionError, CredentialProviderError):
        raise
    except Exception as exc:
        raise CredentialProviderError("credential provider failed to resolve credentials") from exc


def resolve_sync_provider(resolve: Callable[[], _CredentialsT]) -> _CredentialsT:
    try:
        return resolve()
    except (CredentialResolutionError, CredentialProviderError):
        raise
    except Exception as exc:
        raise CredentialProviderError("credential provider failed to resolve credentials") from exc


def header_value(headers: Mapping[str, str], key: str) -> str | None:
    lower_key = key.lower()
    for header, value in headers.items():
        if str(header).lower() == lower_key:
            return str(value)
    return None


def decode_jwt_payload(token: str, *, token_name: str = "JWT") -> dict[str, Any]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError(f"invalid {token_name}")
        base64_part = parts[1].replace("-", "+").replace("_", "/")
        padded = base64_part + "=" * ((4 - (len(base64_part) % 4)) % 4)
        decoded = base64.b64decode(padded)
        payload = json.loads(decoded.decode("utf-8"))
    except Exception as exc:
        raise CredentialResolutionError(f"{token_name} payload could not be decoded") from exc
    if not isinstance(payload, dict):
        raise CredentialResolutionError(f"{token_name} payload must be an object")
    return payload


def require_scope(
    field: str,
    *,
    sources: tuple[SettingSource, ...],
    payload: dict[str, Any] | None,
    payload_key: str,
    source: str,
) -> str:
    value = optional_string_setting(field, sources) or payload_string(payload, payload_key)
    if value is None or value == "":
        raise CredentialResolutionError(f"{source} missing required {field}")
    return value


def optional_string_setting(
    field: str,
    sources: tuple[SettingSource, ...],
    *,
    label: str = "credential",
) -> str | None:
    for source in sources:
        value = source.get_value(field)
        if value is MISSING:
            continue
        if not isinstance(value, str):
            raise CredentialResolutionError(
                f"{label} setting {field!r} from {source.name} must be a string"
            )
        return value
    return None


def payload_string(payload: dict[str, Any] | None, key: str) -> str | None:
    if payload is None:
        return None
    value = payload.get(key)
    if isinstance(value, str):
        return value
    return None


def validate_provider_credentials(
    credentials: Credentials,
    *,
    source: str,
) -> Credentials:
    if not isinstance(credentials, (OIDCCredentials, AccessTokenCredentials)):
        raise CredentialProviderError(f"{source} returned unsupported credentials")
    _require_credential_field(credentials.token, "token", source=source)
    _require_credential_field(credentials.project_id, "project_id", source=source)
    _require_credential_field(credentials.team_id, "team_id", source=source)
    return credentials


def _require_credential_field(value: str, field: str, *, source: str) -> None:
    if value == "":
        raise CredentialProviderError(f"{source} returned empty {field}")


__all__ = [
    "AccessTokenCredentials",
    "AuthError",
    "CredentialProvider",
    "CredentialProviderError",
    "CredentialResolutionError",
    "Credentials",
    "CredentialsT",
    "OIDCCredentials",
    "StaticCredentialProvider",
    "SyncCredentialProvider",
    "SyncStaticCredentialProvider",
    "decode_jwt_payload",
    "header_value",
    "optional_string_setting",
    "payload_string",
    "require_scope",
    "resolve_provider",
    "resolve_sync_provider",
    "validate_provider_credentials",
]
