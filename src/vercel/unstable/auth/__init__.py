"""Minimal auth types for unstable service credential policy."""

from __future__ import annotations

from vercel._internal.unstable.auth import (
    AccessTokenCredentials,
    AuthError,
    CredentialProvider,
    CredentialProviderError,
    CredentialResolutionError,
    OIDCCredentials,
    StaticCredentialProvider,
    SyncCredentialProvider,
    SyncStaticCredentialProvider,
)

__all__ = [
    "AccessTokenCredentials",
    "AuthError",
    "CredentialProvider",
    "CredentialProviderError",
    "CredentialResolutionError",
    "OIDCCredentials",
    "StaticCredentialProvider",
    "SyncCredentialProvider",
    "SyncStaticCredentialProvider",
]
