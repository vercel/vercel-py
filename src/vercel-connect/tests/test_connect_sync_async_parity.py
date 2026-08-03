"""Sync and async surfaces must stay identical in name, signature, and docs."""

import inspect
from typing import Any

import pytest

from vercel import connect
from vercel.connect import sync as connect_sync

OPERATIONS = [
    "get_token",
    "get_token_response",
    "revoke_token",
    "start_authorization",
    "get_connector_metadata",
    "verify_connect_webhook",
    "create_connect_webhook_verifier",
    "delete_token_cache_entry",
    "clear_token_cache",
]


def test_public_names_match() -> None:
    async_names = {name for name in connect.__all__ if name != "sync"}
    sync_names = set(connect_sync.__all__)

    assert async_names == sync_names


@pytest.mark.parametrize("name", OPERATIONS)
def test_operation_exists_in_both_surfaces(name: str) -> None:
    assert callable(getattr(connect, name))
    assert callable(getattr(connect_sync, name))


@pytest.mark.parametrize("name", OPERATIONS)
def test_signatures_match(name: str) -> None:
    async_signature = inspect.signature(getattr(connect, name))
    sync_signature = inspect.signature(getattr(connect_sync, name))

    assert list(async_signature.parameters) == list(sync_signature.parameters)
    for parameter in async_signature.parameters:
        async_parameter = async_signature.parameters[parameter]
        sync_parameter = sync_signature.parameters[parameter]
        assert async_parameter.kind == sync_parameter.kind
        assert async_parameter.default == sync_parameter.default


@pytest.mark.parametrize("name", OPERATIONS)
def test_docstrings_match(name: str) -> None:
    assert getattr(connect, name).__doc__ == getattr(connect_sync, name).__doc__


@pytest.mark.parametrize("name", OPERATIONS)
def test_async_operations_are_coroutines_and_sync_ones_are_not(name: str) -> None:
    async_operation = getattr(connect, name)
    sync_operation = getattr(connect_sync, name)

    if name in {"delete_token_cache_entry", "clear_token_cache", "create_connect_webhook_verifier"}:
        assert not inspect.iscoroutinefunction(async_operation)
    else:
        assert inspect.iscoroutinefunction(async_operation)
    assert not inspect.iscoroutinefunction(sync_operation)


@pytest.mark.parametrize(
    "name",
    [
        "ConnectAppTokenSubject",
        "ConnectUserTokenSubject",
        "ConnectJwtBearerTokenSubject",
        "ConnectTokenExchangeSubject",
        "ConnectGitHubAppInstallationAuthorizationDetail",
        "ConnectCustomAuthorizationDetail",
        "ConnectTokenResponse",
        "ConnectAuthorizationResponse",
        "ConnectorMetadata",
        "ConnectorRef",
        "ConnectWebhookClaims",
        "ConnectOptions",
        "ConnectServiceOptions",
        "ConnectError",
        "ConnectApiError",
        "NoValidTokenError",
        "UserAuthorizationRequiredError",
        "ConnectorInstallationRequiredError",
        "ConnectResponseError",
        "ConnectCredentialsError",
        "ConnectValidationError",
        "ConnectWebhookVerificationError",
    ],
)
def test_shared_types_are_the_same_object_in_both_surfaces(name: str) -> None:
    """Every public variant is exported, unlike the TypeScript package."""
    assert getattr(connect, name) is getattr(connect_sync, name)


def test_no_experimental_surface_is_exposed() -> None:
    exported: Any = set(connect.__all__) | set(connect_sync.__all__)

    assert not [name for name in exported if "experimental" in name.lower()]
    for name in (
        "start_installation",
        "experimental_start_installation",
        "experimental",
        "ConnectInstallationResponse",
    ):
        assert not hasattr(connect, name)
        assert not hasattr(connect_sync, name)
