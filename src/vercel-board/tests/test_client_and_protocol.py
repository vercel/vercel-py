"""Behavioral tests for Board handles, BoardClient, as_profile(), and dual protocols."""

from __future__ import annotations

import json
import re
import unicodedata

import httpx2 as httpx
import pytest

import vendor.respx as respx
from vercel.api import session
from vercel.board import (
    ActorKind,
    Board,
    BoardApiError,
    BoardClient,
    BoardConflictError,
    BoardNotFoundError,
    BoardPermissionError,
    BoardServiceOptions,
    BoardValidationError,
    Discovery,
    Me,
    Permission,
    Profile,
    board,
    static_credentials_factory,
    sync as board_sync,
)
from vercel.errors import VercelSessionClosedError

HTTP_BOARD_URL = "https://board.example.test/api/board"
HOSTED_API_URL = "https://api.vercel.com"

UUIDV7_RE = re.compile(r"\A[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")

ME_HTTP_JSON = {
    "actor": "vercel:user:u_123+fixer",
    "principal": "vercel:user:u_123",
    "kind": "agent",
    "handle": "fixer",
    "name": "fixer",
    "avatar": "https://example.com/fixer.png",
    "profile": "fixer",
    "via": {"name": "alice", "environment": None},
    "expiresAt": "2026-12-01T00:00:00Z",
    "can": {"dev/": ["read", "post"]},
}

DISCOVERY_HTTP_JSON = {
    "version": "1.2.0",
    "url": HTTP_BOARD_URL,
    "audience": HTTP_BOARD_URL,
    "team_id": "team_abc",
    "project_id": "prj_123",
    "name": "engineering",
    "sync": [{"key": "slack", "platform": "slack"}],
    "wake_by_path": True,
}

API_ME_JSON = {
    "teamId": "team_abc",
    "actor": {
        "id": "vercel:user:u_123+fixer",
        "principal": {"type": "user", "userId": "u_123"},
        "profile": {"name": "fixer", "avatar": "https://example.com/fixer.png"},
    },
    "view": {
        "id": "vercel:user:u_123+fixer",
        "kind": "agent",
        "handle": "fixer",
        "name": "Fixer Bot",
        "avatar": "https://example.com/fixer.png",
        "via": {"name": "alice"},
    },
    "can": {"dev/": ["read", "post"]},
    "expiresAt": 1796083200000,
}


@pytest.fixture
def clean_board_env(monkeypatch: pytest.MonkeyPatch) -> None:
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
        "VERCEL_ORG_ID",
        "VERCEL_AUTH_TOKEN",
        "VERCEL_TOKEN",
        "VERCEL_AUTOMATION_BYPASS_SECRET",
        "ACTIONS_ID_TOKEN_REQUEST_URL",
        "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)


# --- Handle construction & validation --------------------------------------


def test_board_factory_and_direct_handle_validate_name_and_team(clean_board_env: None) -> None:
    default_handle = board()
    assert default_handle is not None

    decomposed = "  cafe\u0301  "
    assert len(unicodedata.normalize("NFC", decomposed.strip())) == 4
    named = board(name=decomposed, team="team_abc", ensure=False)
    assert named is not None

    for bad_name in ("", "   ", "a/b", "x" * 65):
        with pytest.raises(BoardValidationError):
            board(name=bad_name)
        with pytest.raises(BoardValidationError):
            Board(name=bad_name)
        with pytest.raises(BoardValidationError):
            board_sync.Board(name=bad_name)

    for bad_team in ("", "team/foo", "team foo"):
        with pytest.raises(BoardValidationError):
            board(team=bad_team)
        with pytest.raises(BoardValidationError):
            Board(team=bad_team)


def test_as_profile_validation_and_immutability(clean_board_env: None) -> None:
    base = board()

    with pytest.raises(BoardValidationError):
        base.as_profile(None, avatar="https://example.com/a.png")

    with pytest.raises(BoardValidationError):
        base.as_profile(
            Profile(name="fixer", avatar="https://example.com/a.png"),
            avatar="https://example.com/b.png",
        )

    same = base.as_profile(
        Profile(name="fixer", avatar="https://example.com/a.png"),
        avatar="https://example.com/a.png",
    )
    assert same is not base

    with pytest.raises(BoardValidationError):
        base.as_profile("123-bad")


# --- HTTP (BOARD_URL) protocol ---------------------------------------------


@respx.mock
async def test_http_protocol_discovery_and_me_async(
    clean_board_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Bypass secret is ONLY sent to the deployment's own host and never to public discovery
    monkeypatch.setenv("VERCEL_AUTOMATION_BYPASS_SECRET", "bypass-secret-1")
    monkeypatch.setenv("VERCEL_PROJECT_PRODUCTION_URL", "board.example.test")

    disc_route = respx.get(f"{HTTP_BOARD_URL}/").mock(
        return_value=httpx.Response(200, json=DISCOVERY_HTTP_JSON)
    )
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=ME_HTTP_JSON)
    )

    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("http-tok"),
    )
    async with session(service_options=[opts]):
        handle = board()
        disc = await handle.discovery()
        assert isinstance(disc, Discovery)
        assert disc.version == "1.2.0"
        assert disc.audience == HTTP_BOARD_URL
        assert disc.sync[0].key == "slack"
        assert "authorization" not in disc_route.calls.last.request.headers
        assert (
            disc_route.calls.last.request.headers["x-vercel-protection-bypass"] == "bypass-secret-1"
        )

        narrowed = handle.as_profile(
            "fixer",
            avatar="https://example.com/fixer.png",
            can={"dev/": [Permission.READ, Permission.POST]},
        ).as_profile(can={"*": "all"})

        me = await narrowed.me()
        assert isinstance(me, Me)
        assert me.actor == "vercel:user:u_123+fixer"
        assert me.kind is ActorKind.AGENT
        assert me.can.allows(Permission.POST, "dev/api")

        req_headers = me_route.calls.last.request.headers
        assert req_headers["authorization"] == "Bearer http-tok"
        assert req_headers["x-board-profile"] == 'fixer;avatar="https://example.com/fixer.png"'
        assert "x-board-avatar" not in req_headers
        assert req_headers["x-board-can"] == "dev/=read,post"
        assert req_headers["x-vercel-protection-bypass"] == "bypass-secret-1"


@respx.mock
async def test_bypass_secret_not_sent_to_foreign_host_and_redirects_never_leak_headers(
    clean_board_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_AUTOMATION_BYPASS_SECRET", "top-secret-bypass")

    disc_route = respx.get(f"{HTTP_BOARD_URL}/").mock(
        return_value=httpx.Response(200, json=DISCOVERY_HTTP_JSON)
    )
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=ME_HTTP_JSON)
    )
    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("http-tok"),
    )

    # Outside Vercel (VERCEL != "1"), explicit VERCEL_AUTOMATION_BYPASS_SECRET is honored
    async with session(service_options=[opts]):
        await board().discovery()
        await board().me()

    assert (
        disc_route.calls.last.request.headers["x-vercel-protection-bypass"] == "top-secret-bypass"
    )
    assert me_route.calls.last.request.headers["x-vercel-protection-bypass"] == "top-secret-bypass"

    # On Vercel (VERCEL=1) talking to a foreign host, NEVER send VERCEL_AUTOMATION_BYPASS_SECRET;
    # send x-vercel-trusted-oidc-idp-token instead on discovery and /me
    import base64
    import time

    hdr = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
    body = (
        base64.urlsafe_b64encode(json.dumps({"exp": int(time.time()) + 3600}).encode())
        .rstrip(b"=")
        .decode()
    )
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_URL", "my-app.vercel.app")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", f"{hdr}.{body}.sig")
    exchange_route = respx.post("https://oidc.vercel.com/~token").mock(
        return_value=httpx.Response(200, json={"token": "exchanged-gateway-oidc"})
    )
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        await board().discovery()
    assert exchange_route.called
    assert "authorization" not in disc_route.calls.last.request.headers
    assert "x-vercel-protection-bypass" not in disc_route.calls.last.request.headers
    assert (
        disc_route.calls.last.request.headers["x-vercel-trusted-oidc-idp-token"]
        == "exchanged-gateway-oidc"
    )

    # Even with follow_redirects=True on a custom client, redirects are never followed
    evil_route = respx.get("https://evil.example/steal").mock(
        return_value=httpx.Response(200, json=ME_HTTP_JSON)
    )
    respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(302, headers={"location": "https://evil.example/steal"})
    )
    async with BoardClient.create(
        options=opts,
        httpx_client_factory=lambda: httpx.AsyncClient(follow_redirects=True),
    ) as client:
        with pytest.raises(BoardApiError) as exc_info:
            await client.board().me()
        assert exc_info.value.status_code == 302
    assert not evil_route.called


@respx.mock
async def test_own_host_trusted_sources_and_bypass_rejection_fallback(
    clean_board_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import base64
    import time

    hdr = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
    body = (
        base64.urlsafe_b64encode(json.dumps({"exp": int(time.time()) + 3600}).encode())
        .rstrip(b"=")
        .decode()
    )
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", f"{hdr}.{body}.sig")
    monkeypatch.setenv("VERCEL_PROJECT_PRODUCTION_URL", "board.example.test")

    exchange_route = respx.post("https://oidc.vercel.com/~token").mock(
        return_value=httpx.Response(200, json={"token": "exchanged-own-host-oidc"})
    )
    disc_route = respx.get(f"{HTTP_BOARD_URL}/").mock(
        return_value=httpx.Response(200, json=DISCOVERY_HTTP_JSON)
    )

    # 1. Own host with VERCEL=1 and NO VERCEL_AUTOMATION_BYPASS_SECRET -> Trusted Sources OIDC
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        disc = await board().discovery()
        assert disc.version == "1.2.0"

    assert exchange_route.called
    assert "authorization" not in disc_route.calls.last.request.headers
    assert "x-vercel-protection-bypass" not in disc_route.calls.last.request.headers
    assert (
        disc_route.calls.last.request.headers["x-vercel-trusted-oidc-idp-token"]
        == "exchanged-own-host-oidc"
    )

    # 2. Own host where VERCEL_AUTOMATION_BYPASS_SECRET is rejected by Deployment Protection
    #    -> retries once without bypass, using Trusted Sources OIDC, and succeeds
    monkeypatch.setenv("VERCEL_AUTOMATION_BYPASS_SECRET", "stale-bypass-secret")
    before_disc = len(disc_route.calls)
    disc_route.mock(
        side_effect=[
            httpx.Response(
                401,
                headers={"x-vercel-error": "DEPLOYMENT_PROTECTION"},
                text="unauthorized",
            ),
            httpx.Response(200, json=DISCOVERY_HTTP_JSON),
        ]
    )
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        disc = await board().discovery()
        assert disc.version == "1.2.0"

    assert len(disc_route.calls) - before_disc == 2
    first_req = disc_route.calls[-2].request
    second_req = disc_route.calls[-1].request
    assert first_req.headers["x-vercel-protection-bypass"] == "stale-bypass-secret"
    assert "authorization" not in first_req.headers
    assert "x-vercel-protection-bypass" not in second_req.headers
    assert second_req.headers["x-vercel-trusted-oidc-idp-token"] == "exchanged-own-host-oidc"
    assert "authorization" not in second_req.headers

    # 3. Ordinary JSON board 401 (no x-vercel-error, not HTML) -> does NOT retry bypass
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(
            401,
            json={"error": {"code": "unauthorized", "message": "bad board token"}},
        )
    )
    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("bad-tok"),
    )
    async with session(service_options=[opts]):
        with pytest.raises(BoardApiError) as exc_info:
            await board().me()
        assert exc_info.value.status_code == 401
        assert exc_info.value.code == "unauthorized"
    assert len(me_route.calls) == 1

    # 4. When bypass fails AND Trusted Sources is also rejected with a 200 HTML wall,
    #    classify the final wall as BoardCredentialsError(code="protected")
    from vercel.board import BoardCredentialsError

    before_wall = len(disc_route.calls)
    disc_route.mock(
        side_effect=[
            httpx.Response(
                401,
                headers={"x-vercel-error": "DEPLOYMENT_PROTECTION"},
                text="unauthorized",
            ),
            httpx.Response(
                200,
                headers={"content-type": "text/html; charset=utf-8"},
                text="<html><body>Vercel Login</body></html>",
            ),
        ]
    )
    async with session(service_options=[BoardServiceOptions(url=HTTP_BOARD_URL)]):
        with pytest.raises(BoardCredentialsError) as wall_exc:
            await board().discovery()
        assert wall_exc.value.code == "protected"
        assert wall_exc.value.status_code == 200
    assert len(disc_route.calls) - before_wall == 2


@respx.mock
async def test_http_profile_avatar_rfc8941_ascii_normalization(clean_board_env: None) -> None:
    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=ME_HTTP_JSON)
    )
    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        # IDN hostname + Unicode path
        await board().as_profile("fixer", avatar="https://bücher.example/avatär.png").me()
        assert (
            me_route.calls.last.request.headers["x-board-profile"]
            == 'fixer;avatar="https://xn--bcher-kva.example/avat%C3%A4r.png"'
        )

        # IPv6 literal host preserves brackets
        await board().as_profile("fixer", avatar="https://[::1]/é.png").me()
        assert (
            me_route.calls.last.request.headers["x-board-profile"]
            == 'fixer;avatar="https://[::1]/%C3%A9.png"'
        )

        # Quotes and backslashes escaped per RFC 8941
        await board().as_profile("fixer", avatar='https://example.com/a"b\\c.png').me()
        assert (
            me_route.calls.last.request.headers["x-board-profile"]
            == 'fixer;avatar="https://example.com/a\\"b\\\\c.png"'
        )

        # Control characters and malformed port/authority rejected before transport
        before = len(me_route.calls)
        with pytest.raises(BoardValidationError):
            await board().as_profile("fixer", avatar="https://example.com/bad\r\nheader").me()
        with pytest.raises(BoardValidationError):
            await board().as_profile("fixer", avatar="https://example.com:99999/é.png").me()
        assert len(me_route.calls) == before


@respx.mock
def test_http_protocol_sync_and_board_profile_env_suppression(
    clean_board_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BOARD_PROFILE", "env-bot")
    monkeypatch.setenv("BOARD_AVATAR", "https://example.com/env.png")

    me_route = respx.get(f"{HTTP_BOARD_URL}/me").mock(
        return_value=httpx.Response(200, json=ME_HTTP_JSON)
    )

    opts = board_sync.BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=board_sync.static_credentials_factory("sync-tok"),
    )
    with session(service_options=[opts]):
        handle = board_sync.board()
        handle.me()
        assert (
            me_route.calls.last.request.headers["x-board-profile"]
            == 'env-bot;avatar="https://example.com/env.png"'
        )

        handle.as_profile(None).me()
        assert "x-board-profile" not in me_route.calls.last.request.headers


@respx.mock
async def test_board_client_pins_session_and_rejects_calls_after_close(
    clean_board_env: None,
) -> None:
    respx.get(f"{HTTP_BOARD_URL}/me").mock(return_value=httpx.Response(200, json=ME_HTTP_JSON))

    async_client = BoardClient.create(
        options=BoardServiceOptions(
            url=HTTP_BOARD_URL,
            credentials_factory=static_credentials_factory("pinned-async"),
        )
    )
    async with async_client:
        handle = async_client.board()
        me = await handle.me()
        assert me.handle == "fixer"
    with pytest.raises(VercelSessionClosedError):
        await handle.me()

    sync_client = board_sync.BoardClient.create(
        options=board_sync.BoardServiceOptions(
            url=HTTP_BOARD_URL,
            credentials_factory=board_sync.static_credentials_factory("pinned-sync"),
        )
    )
    with sync_client:
        sync_handle = sync_client.board()
        sync_me = sync_handle.me()
        assert sync_me.handle == "fixer"
    with pytest.raises(VercelSessionClosedError):
        sync_handle.me()


# --- Vercel Hosted (/v1/messageboard) protocol -----------------------------


@respx.mock
async def test_hosted_protocol_default_board_me_and_discovery(clean_board_env: None) -> None:
    boards_route = respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_default"})
    )
    me_route = respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_default/me").mock(
        return_value=httpx.Response(200, json=API_ME_JSON)
    )
    board_info_route = respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_default").mock(
        return_value=httpx.Response(200, json={"board": {"id": "brd_default", "name": "General"}})
    )

    opts = BoardServiceOptions(
        url=None,
        team="team_abc",
        credentials_factory=static_credentials_factory("hosted-tok"),
    )
    async with session(service_options=[opts]):
        handle = board().as_profile(
            "fixer",
            avatar="https://example.com/fixer.png",
            can={"dev/": [Permission.READ, Permission.POST]},
        )
        me = await handle.me()
        assert me.actor == "vercel:user:u_123+fixer"
        assert me.name == "Fixer Bot"
        assert me.profile == "fixer"
        assert me.via is not None
        assert me.via.name == "alice"
        assert me.can.allows(Permission.READ, "dev/x")
        assert me.expires_at is not None

        req = me_route.calls.last.request
        assert req.url.params["teamId"] == "team_abc"
        assert req.headers["authorization"] == "Bearer hosted-tok"
        assert req.headers["x-board-profile"] == "fixer"
        assert req.headers["x-board-avatar"] == "https://example.com/fixer.png"
        assert req.headers["x-board-can"] == "dev/=read,post"
        assert boards_route.called

        disc = await handle.discovery()
        assert disc.team_id == "team_abc"
        assert disc.name == "General"
        assert disc.audience == HOSTED_API_URL
        assert disc.url == f"{HOSTED_API_URL}/v1/messageboard/boards/brd_default?teamId=team_abc"
        assert disc.wake_by_path is True
        assert board_info_route.called


@respx.mock
async def test_hosted_me_default_can_and_actor_attribution_without_view(
    clean_board_env: None,
) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1"})
    )

    # 1. Omitted `can` defaults to read/post/react/label (NEVER manage/sync),
    #    and omitted `view` preserves `via` attribution for profiled actors
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "teamId": "team_abc",
                "actor": {
                    "id": "vercel:project:prj_123:preview+triage",
                    "principal": {
                        "type": "project",
                        "projectId": "prj_123",
                        "environment": "preview",
                    },
                    "profile": {"name": "triage"},
                },
            },
        )
    )
    opts = BoardServiceOptions(
        url=None,
        team="team_abc",
        credentials_factory=static_credentials_factory("hosted-tok"),
    )
    async with session(service_options=[opts]):
        me = await board().me()

    assert me.can.allows(Permission.READ, "general")
    assert me.can.allows(Permission.POST, "general")
    assert me.can.allows(Permission.REACT, "general")
    assert me.can.allows(Permission.LABEL, "general")
    assert not me.can.allows(Permission.MANAGE, "general")
    assert not me.can.allows(Permission.SYNC, "general")
    assert me.via is not None
    assert me.via.name == "prj_123"
    assert me.via.environment == "preview"

    # 2. Unknown permissions / invalid patterns are filtered safely,
    #    and OIDC principal uses encoded actor ID prefix
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "teamId": "team_abc",
                "actor": {
                    "id": "oidc:issuer.example.com%2Fauth:sub_42+bot",
                    "principal": {
                        "type": "oidc",
                        "issuer": "https://issuer.example.com/auth",
                        "subject": "sub_42",
                    },
                    "profile": {"name": "bot"},
                },
                "can": {
                    "dev/": ["read", "future_perm"],
                    "BAD PATTERN!": ["manage"],
                },
            },
        )
    )
    async with session(service_options=[opts]):
        oidc_me = await board().me()

    assert oidc_me.principal == "oidc:issuer.example.com%2Fauth:sub_42"
    assert oidc_me.can.allows(Permission.READ, "dev/x")
    assert not oidc_me.can.allows(Permission.MANAGE, "dev/x")
    assert oidc_me.via is not None
    assert oidc_me.via.name == "sub_42"


@respx.mock
async def test_hosted_board_creation_uses_canonical_uuidv7_and_reuses_on_429_retry(
    clean_board_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import anyio

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(anyio, "sleep", no_sleep)

    # Named board creation with 429 retry
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"boards": []})
    )
    ensure_route = respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/ensure").mock(
        side_effect=[
            httpx.Response(
                429, headers={"retry-after": "1"}, json={"error": {"code": "rate_limited"}}
            ),
            httpx.Response(200, json={"board": {"id": "brd_sec"}, "created": True}),
        ]
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_sec/me").mock(
        return_value=httpx.Response(200, json=API_ME_JSON)
    )

    opts = BoardServiceOptions(
        url=None,
        credentials_factory=static_credentials_factory("hosted-tok"),
    )
    async with session(service_options=[opts]):
        await board(name="security", team="acme", ensure=True).me()

    assert len(ensure_route.calls) == 2
    first_op = json.loads(ensure_route.calls[0].request.content)["operationId"]
    second_op = json.loads(ensure_route.calls[1].request.content)["operationId"]
    assert UUIDV7_RE.fullmatch(first_op) is not None
    assert first_op == second_op

    # Default board creation also uses canonical UUIDv7
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": None})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/me").mock(
        return_value=httpx.Response(200, json=API_ME_JSON)
    )
    default_route = respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/default").mock(
        return_value=httpx.Response(200, json={"board": {"id": "brd_new_default"}})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_new_default/me").mock(
        return_value=httpx.Response(200, json=API_ME_JSON)
    )
    async with session(service_options=[opts]):
        await board(team="acme").me()

    default_op = json.loads(default_route.calls.last.request.content)["operationId"]
    assert UUIDV7_RE.fullmatch(default_op) is not None


@respx.mock
async def test_hosted_board_id_cache_scoped_to_identity_when_team_is_none(
    clean_board_env: None,
) -> None:
    tokens = iter(["token-user-a", "token-user-b"])

    async def switching_factory() -> str:
        return next(tokens)

    def boards_handler(request: httpx.Request) -> httpx.Response:
        if request.headers["authorization"] == "Bearer token-user-a":
            return httpx.Response(200, json={"defaultBoardId": "brd_a"})
        return httpx.Response(200, json={"defaultBoardId": "brd_b"})

    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(side_effect=boards_handler)
    me_a = respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_a/me").mock(
        return_value=httpx.Response(200, json=API_ME_JSON)
    )
    me_b = respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_b/me").mock(
        return_value=httpx.Response(200, json=API_ME_JSON)
    )

    opts = BoardServiceOptions(url=None, credentials_factory=switching_factory)
    async with session(service_options=[opts]):
        await board().me()
        await board().me()

    assert me_a.called
    assert me_b.called


@respx.mock
async def test_hosted_protocol_named_board_errors(clean_board_env: None) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"boards": []})
    )

    opts = BoardServiceOptions(
        url=None,
        team="team_abc",
        credentials_factory=static_credentials_factory("hosted-tok"),
    )

    async with session(service_options=[opts]):
        with pytest.raises(BoardNotFoundError) as exc_info:
            await board(name="missing", ensure=False).me()
        assert exc_info.value.code == "no_board"

    respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/ensure").mock(
        return_value=httpx.Response(
            409,
            json={"error": {"code": "board_name_taken", "message": "already taken"}},
        )
    )
    async with session(service_options=[opts]):
        with pytest.raises(BoardConflictError) as conflict_exc:
            await board(name="taken", ensure=True).me()
        assert conflict_exc.value.code == "board_name_taken"

    respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/ensure").mock(
        return_value=httpx.Response(
            403,
            json={"error": {"code": "board_not_granted", "message": "not granted"}},
        )
    )
    async with session(service_options=[opts]):
        with pytest.raises(BoardPermissionError) as perm_exc:
            await board(name="private", ensure=True).me()
        assert perm_exc.value.code == "board_not_granted"
