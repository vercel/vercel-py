"""Error taxonomy and error-body parsing.

The TypeScript suite never exercises this: `NoValidTokenError` appears zero times
in it, and the installation codes are never tested. The taxonomy is the package's
main public contract, so every code is parametrized here.
"""

from typing import Any

import httpx
import pytest
import respx
from conftest import TEST_BASE_URL, session_options

from vercel.api import session
from vercel.connect import (
    ConnectApiError,
    ConnectAppTokenSubject,
    ConnectError,
    ConnectorInstallationRequiredError,
    NoValidTokenError,
    UserAuthorizationRequiredError,
    get_token,
    sync as connect_sync,
)
from vercel.errors import VercelError

TOKEN_URL = f"{TEST_BASE_URL}/v1/connect/token/slack%2Fmy-bot"


def error_response(status: int, body: Any, **headers: str) -> httpx.Response:
    if isinstance(body, (dict, list)):
        return httpx.Response(status, json=body, headers=headers)
    return httpx.Response(status, content=body, headers=headers)


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("no_token", NoValidTokenError),
        ("user_authorization_required", UserAuthorizationRequiredError),
        ("client_installation_required", ConnectorInstallationRequiredError),
        ("connector_installation_required", ConnectorInstallationRequiredError),
        ("forbidden", ConnectApiError),
        ("not_found", ConnectApiError),
        ("something_new_from_the_server", ConnectApiError),
    ],
)
@respx.mock
async def test_error_code_maps_to_class(
    mock_env_clear: None,
    code: str,
    expected: type[ConnectApiError],
) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(400, {"error": {"code": code, "message": "boom"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(expected) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert type(exc_info.value) is expected
    assert exc_info.value.code == code


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("no_token", NoValidTokenError),
        ("user_authorization_required", UserAuthorizationRequiredError),
        ("connector_installation_required", ConnectorInstallationRequiredError),
        ("forbidden", ConnectApiError),
    ],
)
@respx.mock
def test_error_code_maps_to_class_sync(
    mock_env_clear: None,
    code: str,
    expected: type[ConnectApiError],
) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(400, {"error": {"code": code, "message": "boom"}})
    )

    with session(service_options=session_options()):
        with pytest.raises(expected):
            connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
async def test_every_error_subclasses_vercel_error(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(400, {"error": {"code": "no_token", "message": "boom"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(VercelError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    error = exc_info.value
    assert isinstance(error, ConnectError)
    assert isinstance(error, ConnectApiError)
    assert isinstance(error, NoValidTokenError)


@respx.mock
async def test_error_uses_err_key(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(400, {"err": {"code": "no_token", "message": "gone"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(NoValidTokenError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert exc_info.value.code == "no_token"
    assert "gone" in str(exc_info.value)


@pytest.mark.parametrize(
    "body",
    [
        {"error": {"code": "forbidden", "message": "no", "vendor": {"upstream": "slack"}}},
        {
            "error": {
                "code": "forbidden",
                "message": "no",
                "meta": {"vendor": {"upstream": "slack"}},
            }
        },
        {"error": {"code": "forbidden", "message": "no"}, "vendor": {"upstream": "slack"}},
    ],
)
@respx.mock
async def test_vendor_payload_found_in_every_position(
    mock_env_clear: None,
    body: dict[str, Any],
) -> None:
    respx.post(TOKEN_URL).mock(return_value=error_response(403, body))

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert exc_info.value.vendor == {"upstream": "slack"}


@pytest.mark.parametrize(
    "body",
    [
        b"<html>502 Bad Gateway</html>",
        b"",
        b"null",
        b"[]",
        b'["unexpected", "array"]',
        b"{}",
        b'{"error": "a plain string"}',
        b'{"error": {}}',
    ],
)
@respx.mock
async def test_unparseable_error_bodies_still_raise_api_error(
    mock_env_clear: None,
    body: bytes,
) -> None:
    respx.post(TOKEN_URL).mock(return_value=error_response(500, body))

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    error = exc_info.value
    assert type(error) is ConnectApiError
    assert error.status_code == 500
    assert str(error)


@respx.mock
async def test_error_captures_request_id_for_correlation(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(
            500,
            {"error": {"code": "internal_error", "message": "boom"}},
            **{"x-vercel-id": "iad1::abc123"},
        )
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert exc_info.value.request_id == "iad1::abc123"
    assert "iad1::abc123" in str(exc_info.value)


@respx.mock
async def test_error_str_includes_code_and_status(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(404, {"error": {"code": "not_found", "message": "missing"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    rendered = str(exc_info.value)
    assert "missing" in rendered
    assert "not_found" in rendered
    assert "404" in rendered


@respx.mock
async def test_error_exposes_response_and_status_text(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(403, {"error": {"code": "forbidden", "message": "nope"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    error = exc_info.value
    assert error.response.status_code == 403
    assert error.status_text == "Forbidden"
    assert error.data == {"error": {"code": "forbidden", "message": "nope"}}


@respx.mock
async def test_recoverable_errors_are_catchable_as_control_flow(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(
            401,
            {"error": {"code": "user_authorization_required", "message": "consent"}},
        )
    )

    async with session(service_options=session_options()):
        try:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        except UserAuthorizationRequiredError as error:
            assert error.code == "user_authorization_required"
        else:
            pytest.fail("expected UserAuthorizationRequiredError")


@respx.mock
async def test_installation_error_is_not_a_user_authorization_error(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(
            400,
            {"error": {"code": "client_installation_required", "message": "install"}},
        )
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectorInstallationRequiredError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert not isinstance(exc_info.value, UserAuthorizationRequiredError)
