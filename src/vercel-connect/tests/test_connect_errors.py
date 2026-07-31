"""Error taxonomy and error-body parsing.

The TypeScript suite never exercises this: `NoValidTokenError` appears zero times
in it, and the installation codes are never tested. The taxonomy is the package's
main public contract, so every code is parametrized here.
"""

from datetime import timedelta
from typing import Any

import httpx
import pytest
import respx
from conftest import TEST_BASE_URL, session_options

from vercel.api import session
from vercel.connect import (
    AuthorizationDeniedError,
    AuthorizationExpiredError,
    AuthorizationPendingError,
    ConnectApiError,
    ConnectAppTokenSubject,
    ConnectError,
    ConnectNotFoundError,
    ConnectorInstallationRequiredError,
    InvalidGrantError,
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
        ("not_found", ConnectNotFoundError),
        ("authorization_pending", AuthorizationPendingError),
        ("slow_down", AuthorizationPendingError),
        ("access_denied", AuthorizationDeniedError),
        ("expired_token", AuthorizationExpiredError),
        ("invalid_grant", InvalidGrantError),
        ("forbidden", ConnectApiError),
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


@respx.mock
async def test_error_message_is_not_double_formatted(mock_env_clear: None) -> None:
    """Only `__str__` renders code and status, so neither appears twice."""
    respx.post(TOKEN_URL).mock(
        return_value=error_response(
            404,
            {"error": {"code": "not_found", "message": "Connector not found: slack/nope"}},
            **{"x-vercel-id": "sfo1::abc"},
        )
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    rendered = str(exc_info.value)
    assert rendered == (
        "Connector not found: slack/nope (code=not_found, status=404, request_id=sfo1::abc)"
    )
    assert rendered.count("code=") == 1
    assert rendered.count("not_found") == 1
    assert rendered.count("404") == 1
    assert "HTTP 404" not in rendered


@respx.mock
async def test_error_message_omits_absent_details(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=error_response(500, {"error": {"message": "internal failure"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    rendered = str(exc_info.value)
    assert rendered == "internal failure (status=500)"
    assert "code=" not in rendered
    assert "request_id=" not in rendered


@respx.mock
async def test_unreadable_body_still_renders_once(mock_env_clear: None) -> None:
    respx.post(TOKEN_URL).mock(return_value=error_response(502, b"<html>Bad Gateway</html>"))

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    rendered = str(exc_info.value)
    assert "Bad Gateway" in rendered
    assert rendered.count("status=502") == 1
    assert rendered.endswith("(status=502)")


@respx.mock
async def test_top_level_message_body_renders_status_once(mock_env_clear: None) -> None:
    """A body without an `error` envelope must not fall back to text that already
    embeds the status."""
    respx.post(TOKEN_URL).mock(return_value=error_response(403, {"message": "Forbidden"}))

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    rendered = str(exc_info.value)
    assert rendered == "Forbidden (status=403)"
    assert "HTTP 403" not in rendered


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            {"error": "user_authorization_required", "error_description": "consent"},
            UserAuthorizationRequiredError,
        ),
        ({"error": "no_token", "error_description": "gone"}, NoValidTokenError),
        (
            {"error": "connector_installation_required", "error_description": "install"},
            ConnectorInstallationRequiredError,
        ),
        ({"error": "invalid_grant", "error_description": "nope"}, InvalidGrantError),
        (
            {"error": "authorization_pending", "error_description": "waiting"},
            AuthorizationPendingError,
        ),
        ({"error": "slow_down", "error_description": "back off"}, AuthorizationPendingError),
        ({"error": "access_denied", "error_description": "refused"}, AuthorizationDeniedError),
        ({"error": "expired_token", "error_description": "too late"}, AuthorizationExpiredError),
        ({"error": "server_error", "error_description": "upstream"}, ConnectApiError),
    ],
)
@respx.mock
async def test_oauth_shaped_error_bodies_map_to_the_taxonomy(
    mock_env_clear: None,
    body: dict[str, Any],
    expected: type[ConnectApiError],
) -> None:
    """OAuth puts the code directly in `error` as a string rather than an object."""
    respx.post(TOKEN_URL).mock(return_value=error_response(401, body))

    async with session(service_options=session_options()):
        with pytest.raises(expected) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert type(exc_info.value) is expected
    assert exc_info.value.code == body["error"]
    assert body["error_description"] in str(exc_info.value)


@respx.mock
async def test_prose_error_string_is_a_message_not_a_code(mock_env_clear: None) -> None:
    """Some services put a sentence in `error`, so only a known code is a code."""
    respx.post(TOKEN_URL).mock(
        return_value=error_response(403, {"error": "Something went wrong, please try again later"})
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    error = exc_info.value
    assert error.code is None
    assert type(error) is ConnectApiError
    assert str(error) == "Something went wrong, please try again later (status=403)"


@respx.mock
async def test_unrecognized_flat_error_string_is_read_as_prose(mock_env_clear: None) -> None:
    """The flat `error` field is ambiguous, so it is only a code when recognized.

    A code the taxonomy does not list is still reported when the server puts it in
    a field that says so, which `test_unmodelled_code_field_is_still_a_code` covers.
    """
    respx.post(TOKEN_URL).mock(return_value=error_response(400, {"error": "widget_exploded"}))

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert exc_info.value.code is None
    assert "widget_exploded" in str(exc_info.value)


@respx.mock
async def test_unmodelled_code_field_is_still_a_code(mock_env_clear: None) -> None:
    """`code` names what it holds, so a server may add one without a release here."""
    respx.post(TOKEN_URL).mock(
        return_value=error_response(429, {"code": "rate_limited", "message": "slow down"})
    )

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert exc_info.value.code == "rate_limited"
    assert str(exc_info.value) == "slow down (code=rate_limited, status=429)"


@respx.mock
async def test_device_code_polling_distinguishes_pending_from_terminal(
    mock_env_clear: None,
) -> None:
    """A poll loop has to tell "keep waiting" from "stop", which is why these
    authorization codes carry their own classes."""
    bodies = [
        {"error": "authorization_pending"},
        {"error": "slow_down"},
        {"error": "expired_token"},
    ]
    respx.post(TOKEN_URL).mock(
        side_effect=[error_response(400, body) for body in bodies],
    )

    async with session(service_options=session_options()):
        for _ in range(2):
            with pytest.raises(AuthorizationPendingError):
                await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        with pytest.raises(AuthorizationExpiredError):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@respx.mock
async def test_single_word_message_is_still_a_message(mock_env_clear: None) -> None:
    """A short message can look code-shaped; only `error` is ambiguous."""
    respx.post(TOKEN_URL).mock(
        return_value=error_response(400, {"error": {"code": "no_token", "message": "gone"}})
    )

    async with session(service_options=session_options()):
        with pytest.raises(NoValidTokenError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert str(exc_info.value) == "gone (code=no_token, status=400)"


@respx.mock
async def test_code_only_body_renders_the_reason_phrase(mock_env_clear: None) -> None:
    """No message anywhere: use the HTTP reason, not the extractor's preamble."""
    respx.post(TOKEN_URL).mock(return_value=error_response(404, {"error": "not_found"}))

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError) as exc_info:
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    rendered = str(exc_info.value)
    assert rendered == "Not Found (code=not_found, status=404)"
    assert "HTTP 404" not in rendered


def test_validation_errors_are_catchable_as_connect_errors() -> None:
    """Examples catch ConnectError; argument validation must not escape that."""
    from vercel.connect import ConnectServiceOptions, ConnectValidationError

    for kwargs in (
        {"token_cache_size": 0},
        {"validity_buffer": timedelta(seconds=-1)},
        {"timeout": timedelta(0)},
    ):
        with pytest.raises(ConnectValidationError) as exc_info:
            ConnectServiceOptions(**kwargs)  # type: ignore[arg-type]
        error = exc_info.value
        assert isinstance(error, ConnectError)
        # Still a ValueError, so the obvious `except ValueError` keeps working.
        assert isinstance(error, ValueError)


@respx.mock
async def test_per_call_validation_error_is_a_connect_error(mock_env_clear: None) -> None:
    from vercel.connect import ConnectOptions, ConnectValidationError

    async with session(service_options=session_options()):
        with pytest.raises(ConnectValidationError) as exc_info:
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(validity_buffer=-1),
            )

    assert isinstance(exc_info.value, ConnectError)
    assert isinstance(exc_info.value, ValueError)
