"""Tests for how VercelWorld authenticates an HTTP call, mirroring
``getHttpConfig`` and ``makeRequest`` in world-vercel's ``utils.ts``.

A workflow-server that is a preview deployment has deployment protection on,
and callers get past it through Trusted Sources — which reads
``x-vercel-trusted-oidc-idp-token``, not ``Authorization``. Without that header
the request comes back as a 302 to the SSO login page, and because the body used
to be decoded before the status was looked at, the failure surfaced as a CBOR
framing error from an HTML page rather than as the redirect it was.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import cbor2
import httpx
import pytest

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds import vercel as vercel_mod
from vercel.oidc import VercelOidcTokenError

BYPASS_HEADER = "x-vercel-trusted-oidc-idp-token"


class _Ping(w.BaseModel):
    ok: bool


def _cbor_ok(request: httpx.Request) -> httpx.Response:
    del request
    return httpx.Response(
        200,
        content=cbor2.dumps({"ok": True}),
        headers={"Content-Type": "application/cbor"},
    )


@pytest.fixture(autouse=True)
def _no_server_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL_WORKFLOW_SERVER_URL", raising=False)
    monkeypatch.delenv("WORKFLOW_VERCEL_BACKEND_URL", raising=False)


@pytest.fixture(autouse=True)
def _no_ambient_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    """No test may reach the real token resolver: it reads the environment and
    the on-disk CLI credentials, so an unstubbed call is a silent dependency on
    whoever is running the suite."""

    async def _fail() -> str:
        raise AssertionError("get_vercel_oidc_token() was not stubbed")

    monkeypatch.setattr(vercel_mod, "get_vercel_oidc_token", _fail)


def _stub_oidc(monkeypatch: pytest.MonkeyPatch, token: str | None) -> None:
    """Stub the OIDC resolver; ``None`` means "not in a Vercel function"."""

    async def _get() -> str:
        if token is None:
            raise VercelOidcTokenError("The 'x-vercel-oidc-token' header is missing")
        return token

    monkeypatch.setattr(vercel_mod, "get_vercel_oidc_token", _get)


def _capture_requests(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response] = _cbor_ok,
) -> list[httpx.Request]:
    """Answer every outbound request from ``_cbor_request`` locally, and hand
    back the requests as httpx assembled them (client headers merged in)."""
    sent: list[httpx.Request] = []
    real_client = httpx.AsyncClient

    def _record(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return handler(request)

    def _factory(**kwargs: Any) -> httpx.AsyncClient:
        return real_client(transport=httpx.MockTransport(_record), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _factory)
    return sent


async def _get_ping(world: vercel_mod.VercelWorld) -> _Ping:
    return await world._cbor_request("GET", "/v2/runs/wrun_1", schema=_Ping)


async def test_direct_call_carries_the_bypass_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """The two headers can hold different values: the bearer prefers the
    configured token, the bypass header is always the OIDC token."""
    _stub_oidc(monkeypatch, "oidc-token")
    sent = _capture_requests(monkeypatch)

    await _get_ping(vercel_mod.VercelWorld(token="configured-token"))

    assert sent[0].headers["Authorization"] == "Bearer configured-token"
    assert sent[0].headers[BYPASS_HEADER] == "oidc-token"


async def test_bearer_falls_back_to_the_oidc_token(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_oidc(monkeypatch, "oidc-token")
    sent = _capture_requests(monkeypatch)

    await _get_ping(vercel_mod.VercelWorld())

    assert sent[0].headers["Authorization"] == "Bearer oidc-token"
    assert sent[0].headers[BYPASS_HEADER] == "oidc-token"


async def test_missing_oidc_does_not_fail_the_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """Outside a Vercel function there is no OIDC token; the call still goes
    out with whatever bearer it has, as in the TypeScript SDK."""
    _stub_oidc(monkeypatch, None)
    sent = _capture_requests(monkeypatch)

    await _get_ping(vercel_mod.VercelWorld(token="configured-token"))

    assert sent[0].headers["Authorization"] == "Bearer configured-token"
    assert BYPASS_HEADER not in sent[0].headers


async def test_no_token_at_all_still_sends_the_request(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_oidc(monkeypatch, None)
    sent = _capture_requests(monkeypatch)

    await _get_ping(vercel_mod.VercelWorld())

    assert "Authorization" not in sent[0].headers
    assert BYPASS_HEADER not in sent[0].headers


async def test_proxy_call_omits_the_bypass_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """Through api.vercel.com the caller authenticates with a real Vercel auth
    token, and the bypass header is neither needed nor correct. The OIDC
    resolver is not consulted — the autouse fixture asserts it."""
    sent = _capture_requests(monkeypatch)

    world = vercel_mod.VercelWorld(token="vercel-auth-token", project_id="prj_1", team_id="team_1")
    await _get_ping(world)

    assert sent[0].headers["Authorization"] == "Bearer vercel-auth-token"
    assert BYPASS_HEADER not in sent[0].headers


async def test_proxy_without_a_token_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    """The proxy does not accept OIDC, so a missing auth token is a
    configuration error, not an opaque 401 at request time."""
    sent = _capture_requests(monkeypatch)

    world = vercel_mod.VercelWorld(project_id="prj_1", team_id="team_1")
    with pytest.raises(ValueError, match="WORKFLOW_VERCEL_AUTH_TOKEN"):
        await _get_ping(world)

    assert sent == []


async def test_a_redirect_names_deployment_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    """A protected deployment answers with a 302 to an HTML login page. The
    error must say so rather than blaming the CBOR parser."""
    _stub_oidc(monkeypatch, None)

    def _sso_redirect(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            302,
            content=b"<html>login</html>",
            headers={
                "Content-Type": "text/html; charset=utf-8",
                "Location": "https://vercel.com/sso-api?url=...",
            },
        )

    _capture_requests(monkeypatch, _sso_redirect)

    with pytest.raises(w.WorkflowWorldError) as excinfo:
        await _get_ping(vercel_mod.VercelWorld(token="configured-token"))

    assert excinfo.value.status == 302
    message = str(excinfo.value)
    assert "302" in message
    assert "deployment protection" in message
    assert "https://vercel.com/sso-api?url=..." in message


async def test_an_unparseable_error_body_still_reports_the_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A gateway's HTML 502 used to surface as CBORDecodeEOF."""
    _stub_oidc(monkeypatch, "oidc-token")
    _capture_requests(
        monkeypatch,
        lambda request: httpx.Response(502, content=b"<html>bad gateway</html>"),
    )

    with pytest.raises(w.WorkflowWorldError) as excinfo:
        await _get_ping(vercel_mod.VercelWorld())

    assert excinfo.value.status == 502
    assert "502" in str(excinfo.value)


async def test_error_bodies_still_drive_the_typed_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Checking the status first must not cost the server's own message."""
    _stub_oidc(monkeypatch, "oidc-token")
    _capture_requests(
        monkeypatch,
        lambda request: httpx.Response(
            410,
            content=json.dumps({"message": "run expired", "code": "RUN_EXPIRED"}).encode(),
            headers={"Content-Type": "application/json"},
        ),
    )

    with pytest.raises(w.RunExpiredError) as excinfo:
        await _get_ping(vercel_mod.VercelWorld())

    assert str(excinfo.value) == "run expired"
    assert excinfo.value.code == "RUN_EXPIRED"


async def test_throttling_still_reads_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_oidc(monkeypatch, "oidc-token")
    _capture_requests(
        monkeypatch,
        lambda request: httpx.Response(
            429,
            content=cbor2.dumps({"message": "slow down"}),
            headers={"Content-Type": "application/cbor", "Retry-After": "7"},
        ),
    )

    with pytest.raises(w.ThrottleError) as excinfo:
        await _get_ping(vercel_mod.VercelWorld())

    assert excinfo.value.retry_after == 7
    assert str(excinfo.value) == "slow down"


async def test_success_still_decodes_unlabelled_cbor(monkeypatch: pytest.MonkeyPatch) -> None:
    """A proxy may drop the Content-Type; the JSON-then-CBOR fallback stays."""
    _stub_oidc(monkeypatch, "oidc-token")
    _capture_requests(
        monkeypatch,
        lambda request: httpx.Response(200, content=cbor2.dumps({"ok": True})),
    )

    assert (await _get_ping(vercel_mod.VercelWorld())).ok is True
