"""Shared fixtures for standalone Connect tests."""

from collections.abc import Generator

import pytest

from vercel.connect import ConnectServiceOptions

TEST_BASE_URL = "https://connect.test"
TEST_OIDC_TOKEN = "oidc-token"


@pytest.fixture
def mock_env_clear(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Prevent tests from resolving credentials from the developer environment."""
    for name in (
        "VERCEL_TOKEN",
        "VERCEL_TEAM_ID",
        "VERCEL_PROJECT_ID",
        "VERCEL_OIDC_TOKEN",
        "VERCEL_OIDC_TOKEN_HEADER",
        "VERCEL_ENV",
        "VERCEL_TARGET_ENV",
        "VERCEL_CONNECT_INTERACTIVE_AUTH_MODE",
    ):
        monkeypatch.delenv(name, raising=False)

    from vercel.oidc.token import _clear_cached_oidc_token

    _clear_cached_oidc_token()
    yield
    _clear_cached_oidc_token()


def session_options(
    *,
    base_url: str = TEST_BASE_URL,
    token: str = TEST_OIDC_TOKEN,
    **kwargs: object,
) -> list[ConnectServiceOptions]:
    """Build Connect service options wired to a test base URL and fake identity."""

    async def credentials_factory() -> str:
        return token

    return [
        ConnectServiceOptions(
            base_url=base_url,
            credentials_factory=credentials_factory,
            **kwargs,  # type: ignore[arg-type]
        )
    ]
