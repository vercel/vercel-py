"""Behavioral tests for auth/team precedence, 429 retries, error mapping, and sessions."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import anyio
import httpx2 as httpx
import pytest

import vendor.respx as respx
from vercel.api import session
from vercel.board import (
    BoardApiError,
    BoardConflictError,
    BoardCredentialsError,
    BoardLabelConflictError,
    BoardNewerMessageError,
    BoardNotFoundError,
    BoardPermissionError,
    BoardRateLimitError,
    BoardResponseError,
    BoardRevisionConflictError,
    BoardServiceOptions,
    board,
    static_credentials_factory,
    sync as board_sync,
)
from vercel.errors import VercelError

HOSTED_API_URL = "https://api.vercel.com"
HTTP_BOARD_URL = "https://board.example.test/api/board"
LOCAL_BOARD_URL = "http://127.0.0.1:4321"
OIDC_EXCHANGE_URL = "https://oidc.vercel.com/~token"

API_ME_JSON = {
    "teamId": "team_from_api",
    "actor": {
        "id": "vercel:user:u_1",
        "principal": {"type": "user", "userId": "u_1"},
    },
    "view": {
        "id": "vercel:user:u_1",
        "kind": "person",
        "handle": "alice",
        "name": "Alice",
    },
    "can": {"*": ["read", "post"]},
}

HTTP_ME_JSON = {
    "actor": "vercel:user:u_1",
    "principal": "vercel:user:u_1",
    "kind": "person",
    "handle": "alice",
    "name": "Alice",
    "can": {"*": ["read", "post"]},
}


def _make_jwt(payload: dict[str, Any]) -> str:
    header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256"}).encode()).rstrip(b"=").decode()
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"{header}.{body}.signature"


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    for var in (
        "BOARD_URL",
        "BOARD_TOKEN",
        "BOARD_PROFILE",
        "BOARD_AVATAR",
        "VERCEL",
        "VERCEL_URL",
        "VERCEL_BRANCH_URL",
        "VERCEL_PROJECT_PRODUCTION_URL",
        "VERCEL_OIDC_TOKEN",
        "VERCEL_OIDC_TOKEN_FILE",
        "VERCEL_ORG_ID",
        "VERCEL_AUTH_TOKEN",
        "VERCEL_TOKEN",
        "VERCEL_AUTOMATION_BYPASS_SECRET",
        "ACTIONS_ID_TOKEN_REQUEST_URL",
        "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg_config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg_data"))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _mock_hosted_routes() -> tuple[respx.Route, respx.Route]:
    boards_route = respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1"})
    )
    me_route = respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/me").mock(
        return_value=httpx.Response(200, json=API_ME_JSON)
    )
    return boards_route, me_route


# --- Hosted credential precedence ------------------------------------------


@respx.mock
async def test_hosted_never_reads_vercel_token(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_TOKEN", "must-never-be-used")
    _mock_hosted_routes()

    async with session(service_options=[BoardServiceOptions(url=None, team="team_1")]):
        with pytest.raises(BoardCredentialsError) as exc_info:
            await board().me()
        assert exc_info.value.code == "not_logged_in"


@respx.mock
async def test_hosted_credential_precedence_order(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, me_route = _mock_hosted_routes()
    oidc_jwt = _make_jwt({"owner_id": "team_oidc", "exp": int(time.time()) + 3600})

    # Write local vercel login auth file (lowest precedence)
    cli_dir = clean_env / "xdg_data" / "com.vercel.cli"
    cli_dir.mkdir(parents=True)
    (cli_dir / "auth.json").write_text(json.dumps({"token": "cli-auth-tok"}), encoding="utf-8")

    # 6. CLI auth file when nothing else is set
    async with session(service_options=[BoardServiceOptions(url=None, team="team_1")]):
        await board().me()
    assert me_route.calls.last.request.headers["authorization"] == "Bearer cli-auth-tok"

    # 5. GitHub Actions OIDC beats CLI auth file (and requires a resolved team)
    gh_url = "https://token.actions.githubusercontent.test/oidc"
    gh_jwt = _make_jwt({"exp": int(time.time()) + 3600})
    gh_route = respx.get(gh_url).mock(return_value=httpx.Response(200, json={"value": gh_jwt}))
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", gh_url)
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "gh-req-secret")

    # Without team anywhere -> missing_team error
    async with session(service_options=[BoardServiceOptions(url=None)]):
        with pytest.raises(BoardCredentialsError) as missing_team_exc:
            await board().me()
        assert missing_team_exc.value.code == "missing_team"

    # Linked .vercel/project.json orgId resolves team for GitHub Actions OIDC
    vercel_dir = clean_env / ".vercel"
    vercel_dir.mkdir()
    (vercel_dir / "project.json").write_text(
        json.dumps({"projectId": "prj_1", "orgId": "team_linked"}),
        encoding="utf-8",
    )
    async with session(service_options=[BoardServiceOptions(url=None)]):
        await board().me()
    assert gh_route.called
    assert (
        gh_route.calls.last.request.url.params["audience"]
        == "https://api.vercel.com/v1/messageboard"
    )
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {gh_jwt}"
    assert me_route.calls.last.request.url.params["teamId"] == "team_linked"

    # 4. VERCEL_AUTH_TOKEN beats GitHub Actions OIDC (matching upstream backend-vercel/index.ts)
    monkeypatch.setenv("VERCEL_AUTH_TOKEN", "vercel-auth-env-tok")
    async with session(service_options=[BoardServiceOptions(url=None, team="team_1")]):
        await board().me()
    assert me_route.calls.last.request.headers["authorization"] == "Bearer vercel-auth-env-tok"

    # 3. BOARD_TOKEN beats VERCEL_AUTH_TOKEN
    monkeypatch.setenv("BOARD_TOKEN", "board-env-tok")
    async with session(service_options=[BoardServiceOptions(url=None, team="team_1")]):
        await board().me()
    assert me_route.calls.last.request.headers["authorization"] == "Bearer board-env-tok"

    # 2. Deployment OIDC (VERCEL_OIDC_TOKEN) beats BOARD_TOKEN
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", oidc_jwt)
    async with session(service_options=[BoardServiceOptions(url=None, team="team_1")]):
        await board().me()
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {oidc_jwt}"

    # 1. Explicit credentials_factory beats everything
    opts = BoardServiceOptions(
        url=None,
        team="team_1",
        credentials_factory=static_credentials_factory("factory-tok"),
    )
    async with session(service_options=[opts]):
        await board().me()
    assert me_route.calls.last.request.headers["authorization"] == "Bearer factory-tok"


# --- Hosted team resolution precedence -------------------------------------


@respx.mock
async def test_hosted_team_resolution_precedence(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, me_route = _mock_hosted_routes()
    monkeypatch.setenv("BOARD_TOKEN", "tok")

    vercel_dir = clean_env / ".vercel"
    vercel_dir.mkdir()
    (vercel_dir / "project.json").write_text(
        json.dumps({"projectId": "prj_1", "orgId": "team_from_project"}),
        encoding="utf-8",
    )
    async with session(service_options=[BoardServiceOptions(url=None)]):
        await board().me()
    assert me_route.calls.last.request.url.params["teamId"] == "team_from_project"

    oidc_jwt = _make_jwt({"owner_id": "team_from_oidc", "exp": int(time.time()) + 3600})
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", oidc_jwt)
    async with session(service_options=[BoardServiceOptions(url=None)]):
        await board().me()
    assert me_route.calls.last.request.url.params["teamId"] == "team_from_oidc"

    monkeypatch.setenv("VERCEL_ORG_ID", "team_from_env")
    async with session(service_options=[BoardServiceOptions(url=None)]):
        await board().me()
    assert me_route.calls.last.request.url.params["teamId"] == "team_from_env"

    async with session(service_options=[BoardServiceOptions(url=None, team="team_from_opts")]):
        await board().me()
    assert me_route.calls.last.request.url.params["teamId"] == "team_from_opts"

    async with session(service_options=[BoardServiceOptions(url=None, team="team_from_opts")]):
        await board(team="team_from_arg").me()
    assert me_route.calls.last.request.url.params["teamId"] == "team_from_arg"


# --- HTTP / BOARD_URL credential precedence & OIDC exchange ----------------


@respx.mock
async def test_http_loopback_local_token_file(clean_env: Path) -> None:
    digest = hashlib.sha256(LOCAL_BOARD_URL.encode("utf-8")).hexdigest()[:32]
    token_path = clean_env / "xdg_config" / "board" / "local" / f"{digest}.json"
    token_path.parent.mkdir(parents=True)
    token_path.write_text(
        json.dumps({"url": LOCAL_BOARD_URL, "token": "local-vb-serve-tok"}),
        encoding="utf-8",
    )

    me_route = respx.get(f"{LOCAL_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=HTTP_ME_JSON)
    )

    async with session(service_options=[BoardServiceOptions(url=LOCAL_BOARD_URL)]):
        me = await board().me()
        assert me.handle == "alice"

    assert me_route.calls.last.request.headers["authorization"] == "Bearer local-vb-serve-tok"


@respx.mock
async def test_http_deployment_oidc_exchanges_for_audience_and_never_sends_raw_token(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_oidc = _make_jwt({"owner_id": "team_1", "exp": int(time.time()) + 3600})
    exchanged_oidc = _make_jwt({"aud": HTTP_BOARD_URL, "exp": int(time.time()) + 3600})
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", raw_oidc)

    respx.get(f"{HTTP_BOARD_URL}/").mock(
        return_value=httpx.Response(
            200,
            json={
                "version": "1.0.0",
                "url": HTTP_BOARD_URL,
                "audience": HTTP_BOARD_URL,
                "project_id": "prj_board",
            },
        )
    )
    exchange_route = respx.post(OIDC_EXCHANGE_URL).mock(
        return_value=httpx.Response(
            200, json={"token": exchanged_oidc, "expiry": int(time.time()) + 3600}
        )
    )
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=HTTP_ME_JSON)
    )

    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        await board().me()

    assert exchange_route.called
    assert json.loads(exchange_route.calls.last.request.content) == {
        "token": raw_oidc,
        "aud": HTTP_BOARD_URL,
    }
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {exchanged_oidc}"

    # If exchange fails, NEVER fall back to sending the raw token
    respx.post(OIDC_EXCHANGE_URL).mock(
        return_value=httpx.Response(500, json={"error": "exchange failed"})
    )
    before_me_calls = len(me_route.calls)
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        with pytest.raises(BoardCredentialsError):
            await board().me()
    assert len(me_route.calls) == before_me_calls


@respx.mock
async def test_http_github_oidc_is_job_scoped_and_never_reused_by_another_identity(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get(f"{HTTP_BOARD_URL}/").mock(
        return_value=httpx.Response(
            200,
            json={
                "version": "1.0.0",
                "url": HTTP_BOARD_URL,
                "audience": HTTP_BOARD_URL,
                "project_id": "prj_1",
            },
        )
    )
    gh_url = "https://token.actions.githubusercontent.test/oidc"
    gh_jwt = _make_jwt({"exp": int(time.time()) + 3600})
    gh_route = respx.get(gh_url).mock(return_value=httpx.Response(200, json={"value": gh_jwt}))
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=HTTP_ME_JSON)
    )

    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", gh_url)
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "gh-secret")

    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        await board().me()
    assert gh_route.called
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {gh_jwt}"

    # Once GitHub Actions env vars are gone, a new identity MUST NOT reuse it from disk
    monkeypatch.delenv("ACTIONS_ID_TOKEN_REQUEST_URL")
    monkeypatch.delenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        with pytest.raises(BoardCredentialsError):
            await board().me()


@respx.mock
async def test_http_github_actions_beats_deployment_oidc_and_vercel_oidc_token_alone_ignored(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get(f"{HTTP_BOARD_URL}/").mock(
        return_value=httpx.Response(
            200,
            json={
                "version": "1.0.0",
                "url": HTTP_BOARD_URL,
                "audience": HTTP_BOARD_URL,
                "project_id": "prj_board",
            },
        )
    )
    gh_url = "https://token.actions.githubusercontent.test/oidc"
    gh_jwt = _make_jwt({"sub": "gh-job", "exp": int(time.time()) + 3600})
    respx.get(gh_url).mock(return_value=httpx.Response(200, json={"value": gh_jwt}))
    raw_oidc = _make_jwt({"owner_id": "team_1", "exp": int(time.time()) + 3600})
    exchanged_oidc = _make_jwt({"aud": HTTP_BOARD_URL, "exp": int(time.time()) + 3600})
    respx.post(OIDC_EXCHANGE_URL).mock(
        return_value=httpx.Response(200, json={"token": exchanged_oidc})
    )
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=HTTP_ME_JSON)
    )

    # 1. Competing GitHub Actions + VERCEL_OIDC_TOKEN -> GitHub Actions wins
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", gh_url)
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "gh-secret")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", raw_oidc)
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        await board().me()
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {gh_jwt}"

    # 2. Competing GitHub Actions + VERCEL=1 -> GitHub Actions still wins in HTTP mode
    monkeypatch.setenv("VERCEL", "1")
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        await board().me()
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {gh_jwt}"

    # 3. Without GitHub Actions and without VERCEL=1, VERCEL_OIDC_TOKEN alone is NOT used
    monkeypatch.delenv("ACTIONS_ID_TOKEN_REQUEST_URL")
    monkeypatch.delenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    monkeypatch.delenv("VERCEL")
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        with pytest.raises(BoardCredentialsError):
            await board().me()


@respx.mock
def test_http_local_login_verifies_project_mints_exchanges_and_caches_per_login_sync(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_AUTH_TOKEN", "user-a-login")
    exchanged_a = _make_jwt({"sub": "user-a", "exp": int(time.time()) + 3600})
    exchanged_b = _make_jwt({"sub": "user-b", "exp": int(time.time()) + 3600})

    respx.get(f"{HTTP_BOARD_URL}/").mock(
        return_value=httpx.Response(
            200,
            json={
                "version": "1.0.0",
                "url": HTTP_BOARD_URL,
                "audience": HTTP_BOARD_URL,
                "project_id": "prj_board",
                "team_id": "team_board",
            },
        )
    )
    respx.get(f"{HOSTED_API_URL}/v13/deployments/board.example.test").mock(
        return_value=httpx.Response(200, json={"projectId": "prj_board", "ownerId": "team_board"})
    )
    mint_route = respx.post(f"{HOSTED_API_URL}/v1/projects/prj_board/token").mock(
        return_value=httpx.Response(200, json={"token": "dev-oidc-tok"})
    )
    exchange_route = respx.post(OIDC_EXCHANGE_URL).mock(
        side_effect=[
            httpx.Response(200, json={"token": exchanged_a}),
            httpx.Response(200, json={"token": exchanged_b}),
        ]
    )
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=HTTP_ME_JSON)
    )

    # First call as user-a: verifies project, mints, exchanges, caches on disk
    with session(service_options=[board_sync.BoardServiceOptions(url=HTTP_BOARD_URL)]):
        board_sync.board().me()
    assert mint_route.called
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {exchanged_a}"
    assert len(exchange_route.calls) == 1

    # Second call as user-a: reuses user-a's disk cache without re-exchanging
    with session(service_options=[board_sync.BoardServiceOptions(url=HTTP_BOARD_URL)]):
        board_sync.board().me()
    assert len(exchange_route.calls) == 1
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {exchanged_a}"

    # Switch to user-b: MUST NOT reuse user-a's cached token!
    monkeypatch.setenv("VERCEL_AUTH_TOKEN", "user-b-login")
    with session(service_options=[board_sync.BoardServiceOptions(url=HTTP_BOARD_URL)]):
        board_sync.board().me()
    assert len(exchange_route.calls) == 2
    assert me_route.calls.last.request.headers["authorization"] == f"Bearer {exchanged_b}"


# --- 429 retry-with-backoff (async and sync) -------------------------------


@respx.mock
async def test_429_retry_succeeds_and_exhausts_async(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(anyio, "sleep", fake_sleep)

    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        side_effect=[
            httpx.Response(
                429,
                headers={"retry-after": "2"},
                json={"error": {"code": "rate_limited", "message": "slow down"}},
            ),
            httpx.Response(
                429,
                headers={"retry-after": "3"},
                json={"error": {"code": "rate_limited", "message": "slow down"}},
            ),
            httpx.Response(200, json=HTTP_ME_JSON),
        ]
    )
    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        me = await board().me()
        assert me.handle == "alice"

    assert len(me_route.calls) == 3
    assert len(sleeps) == 2
    assert sleeps[0] <= 2.0
    assert sleeps[1] <= 3.0

    sleeps.clear()
    before_calls = len(me_route.calls)
    exhaust_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(
            429,
            headers={"retry-after": "7"},
            json={"error": {"code": "rate_limited", "message": "too many requests"}},
        )
    )
    async with session(service_options=[opts]):
        with pytest.raises(BoardRateLimitError) as exc_info:
            await board().me()

    assert len(exhaust_route.calls) - before_calls == 5
    assert len(sleeps) == 4
    assert exc_info.value.status_code == 429
    assert exc_info.value.retry_after == timedelta(seconds=7)


@respx.mock
def test_429_retry_succeeds_and_exhausts_sync_without_suspending(
    clean_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda seconds: sleeps.append(seconds))

    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        side_effect=[
            httpx.Response(
                429,
                headers={"retry-after": "1"},
                json={"error": {"code": "rate_limited", "message": "slow"}},
            ),
            httpx.Response(200, json=HTTP_ME_JSON),
        ]
    )
    opts = board_sync.BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=board_sync.static_credentials_factory("tok"),
    )
    with session(service_options=[opts]):
        me = board_sync.board().me()
        assert me.handle == "alice"

    assert len(me_route.calls) == 2
    assert len(sleeps) == 1
    assert sleeps[0] <= 1.0

    sleeps.clear()
    before_calls = len(me_route.calls)
    exhaust_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(
            429,
            headers={"retry-after": "12"},
            json={"error": {"code": "rate_limited", "message": "limit hit"}},
        )
    )
    with session(service_options=[opts]):
        with pytest.raises(BoardRateLimitError) as exc_info:
            board_sync.board().me()

    assert len(exhaust_route.calls) - before_calls == 5
    assert len(sleeps) == 4
    assert exc_info.value.retry_after == timedelta(seconds=12)


# --- Error mapping ---------------------------------------------------------


@respx.mock
@pytest.mark.parametrize(
    ("status", "code", "expected_cls"),
    [
        (404, "not_found", BoardNotFoundError),
        (403, "forbidden", BoardPermissionError),
        (409, "revision_conflict", BoardRevisionConflictError),
        (409, "label_conflict", BoardLabelConflictError),
        (409, "newer_message", BoardNewerMessageError),
        (409, "duplicate_id", BoardConflictError),
        (500, "internal_error", BoardApiError),
    ],
)
async def test_error_response_mapping(
    clean_env: Path,
    status: int,
    code: str,
    expected_cls: type[BoardApiError],
) -> None:
    respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(
            status,
            headers={"x-vercel-id": "iad1::req-123"},
            json={"error": {"code": code, "message": "boom", "hint": "try again"}},
        )
    )
    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        with pytest.raises(expected_cls) as exc_info:
            await board().me()

    assert exc_info.value.status_code == status
    assert exc_info.value.code == code
    assert exc_info.value.hint == "try again"
    assert exc_info.value.request_id == "iad1::req-123"


@respx.mock
async def test_malformed_success_response_raises_board_response_error(clean_env: Path) -> None:
    respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, text="not-json", headers={"content-type": "text/plain"})
    )
    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        with pytest.raises(BoardResponseError):
            await board().me()


# --- Session mode mismatch -------------------------------------------------


def test_async_options_rejected_in_sync_session(clean_env: Path) -> None:
    async_opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("tok"),
    )
    with session(service_options=[async_opts]):
        with pytest.raises(VercelError, match="SyncBoardServiceOptions"):
            board_sync.board().me()


async def test_sync_options_rejected_in_async_session(clean_env: Path) -> None:
    sync_opts = board_sync.BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=board_sync.static_credentials_factory("tok"),
    )
    async with session(service_options=[sync_opts]):
        with pytest.raises(VercelError, match="BoardServiceOptions"):
            await board().me()
