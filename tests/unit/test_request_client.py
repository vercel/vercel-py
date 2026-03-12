"""Unit tests for RequestClient."""

from typing import Any

import httpx
import pytest

from vercel._internal.http.request_client import (
    RequestClient,
    RetryPolicy,
)
from vercel._internal.http.transport import BaseTransport, JSONBody, RequestBody, SyncTransport
from vercel._internal.iter_coroutine import iter_coroutine


class FakeTransport(BaseTransport):
    """A fake transport that records calls and returns canned responses."""

    def __init__(self, responses: list[httpx.Response] | None = None) -> None:
        self._responses = list(responses or [])
        self._call_count = 0
        self.calls: list[dict[str, Any]] = []

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: RequestBody = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        self.calls.append(
            {
                "method": method,
                "path": path,
                "params": params,
                "headers": headers,
                "body": body,
                "timeout": timeout,
            }
        )
        if self._call_count < len(self._responses):
            resp = self._responses[self._call_count]
        else:
            resp = self._responses[-1] if self._responses else _make_response(200)
        self._call_count += 1
        return resp


class RaisingTransport(BaseTransport):
    """A transport that raises on the first N calls, then succeeds."""

    def __init__(self, fail_count: int, success_response: httpx.Response) -> None:
        self._fail_count = fail_count
        self._success_response = success_response
        self._call_count = 0

    async def send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        self._call_count += 1
        if self._call_count <= self._fail_count:
            raise httpx.ConnectError("connection refused")
        return self._success_response


def _make_response(status_code: int = 200, json_data: Any = None) -> httpx.Response:
    resp = httpx.Response(status_code)
    if json_data is not None:
        import json

        resp._content = json.dumps(json_data).encode()
    return resp


def _noop_sleep(seconds: float) -> None:
    pass


class TestBaseHeaders:
    def test_request_client_does_not_add_auth_header(self) -> None:
        transport = FakeTransport([_make_response(200)])
        rc = RequestClient(
            transport=transport,
            sleep_fn=_noop_sleep,
        )
        iter_coroutine(rc.send("GET", "/test"))
        assert transport.calls[0]["headers"] == {}

    def test_token_compatibility_raises_without_bearer_header(self) -> None:
        rc = RequestClient(
            transport=FakeTransport(),
            base_headers={"x-custom": "value"},
            sleep_fn=_noop_sleep,
        )

        with pytest.raises(RuntimeError, match="no configured bearer token"):
            _ = rc.token

    def test_token_compatibility_reads_bearer_header(self) -> None:
        rc = RequestClient(
            transport=FakeTransport(),
            base_headers={"authorization": "Bearer compat-token"},
            sleep_fn=_noop_sleep,
        )
        assert rc.token == "compat-token"

    def test_per_request_headers_override_base(self) -> None:
        transport = FakeTransport([_make_response(200)])
        rc = RequestClient(
            transport=transport,
            base_headers={
                "authorization": "Bearer base-token",
                "x-custom": "base-value",
            },
            sleep_fn=_noop_sleep,
        )
        iter_coroutine(rc.send("GET", "/test", headers={"x-custom": "override"}))
        assert transport.calls[0]["headers"]["x-custom"] == "override"
        assert transport.calls[0]["headers"]["authorization"] == "Bearer base-token"


class TestParamMerging:
    def test_base_params_sent(self) -> None:
        transport = FakeTransport([_make_response(200)])
        rc = RequestClient(
            transport=transport,
            base_params={"teamId": "team_123"},
            sleep_fn=_noop_sleep,
        )
        iter_coroutine(rc.send("GET", "/test"))
        assert transport.calls[0]["params"]["teamId"] == "team_123"

    def test_per_request_params_override_base(self) -> None:
        transport = FakeTransport([_make_response(200)])
        rc = RequestClient(
            transport=transport,
            base_params={"teamId": "base"},
            sleep_fn=_noop_sleep,
        )
        iter_coroutine(rc.send("GET", "/test", params={"teamId": "override"}))
        assert transport.calls[0]["params"]["teamId"] == "override"


class TestNoRetry:
    def test_no_retry_by_default(self) -> None:
        transport = FakeTransport([_make_response(500)])
        rc = RequestClient(
            transport=transport,
            sleep_fn=_noop_sleep,
        )
        resp = iter_coroutine(rc.send("GET", "/test"))
        assert resp.status_code == 500
        assert len(transport.calls) == 1


class TestRetryOnNetworkError:
    def test_retry_on_transport_error(self) -> None:
        success = _make_response(200)
        transport = RaisingTransport(fail_count=2, success_response=success)
        rc = RequestClient(
            transport=transport,
            retry=RetryPolicy(retries=3, retry_on_network_error=True),
            sleep_fn=_noop_sleep,
        )
        resp = iter_coroutine(rc.send("GET", "/test"))
        assert resp.status_code == 200
        assert transport._call_count == 3  # 2 failures + 1 success

    def test_max_retries_exhausted_raises(self) -> None:
        transport = RaisingTransport(fail_count=5, success_response=_make_response(200))
        rc = RequestClient(
            transport=transport,
            retry=RetryPolicy(retries=2, retry_on_network_error=True),
            sleep_fn=_noop_sleep,
        )
        with pytest.raises(httpx.ConnectError):
            iter_coroutine(rc.send("GET", "/test"))


class TestRetryOnResponse:
    def test_retry_when_callback_returns_true(self) -> None:
        responses = [_make_response(503), _make_response(503), _make_response(200)]
        transport = FakeTransport(responses)
        rc = RequestClient(
            transport=transport,
            retry=RetryPolicy(
                retries=3,
                retry_on_response=lambda r: r.status_code >= 500,
            ),
            sleep_fn=_noop_sleep,
        )
        resp = iter_coroutine(rc.send("GET", "/test"))
        assert resp.status_code == 200
        assert len(transport.calls) == 3

    def test_max_retries_returns_last_response(self) -> None:
        responses = [_make_response(503), _make_response(503), _make_response(503)]
        transport = FakeTransport(responses)
        rc = RequestClient(
            transport=transport,
            retry=RetryPolicy(
                retries=2,
                retry_on_response=lambda r: r.status_code >= 500,
            ),
            sleep_fn=_noop_sleep,
        )
        resp = iter_coroutine(rc.send("GET", "/test"))
        assert resp.status_code == 503
        assert len(transport.calls) == 3  # initial + 2 retries


class TestBackoff:
    def test_backoff_calculation(self) -> None:
        delays: list[float] = []

        def recording_sleep(seconds: float) -> None:
            delays.append(seconds)

        responses = [_make_response(503)] * 4 + [_make_response(200)]
        transport = FakeTransport(responses)
        rc = RequestClient(
            transport=transport,
            retry=RetryPolicy(
                retries=4,
                retry_on_response=lambda r: r.status_code >= 500,
                backoff_base=0.1,
                backoff_max=2.0,
            ),
            sleep_fn=recording_sleep,
        )
        resp = iter_coroutine(rc.send("GET", "/test"))
        assert resp.status_code == 200
        # attempt 0: 0.1 * 2^0 = 0.1
        # attempt 1: 0.1 * 2^1 = 0.2
        # attempt 2: 0.1 * 2^2 = 0.4
        # attempt 3: 0.1 * 2^3 = 0.8
        assert delays == [0.1, 0.2, 0.4, 0.8]

    def test_backoff_capped_at_max(self) -> None:
        delays: list[float] = []

        def recording_sleep(seconds: float) -> None:
            delays.append(seconds)

        responses = [_make_response(503)] * 10 + [_make_response(200)]
        transport = FakeTransport(responses)
        rc = RequestClient(
            transport=transport,
            retry=RetryPolicy(
                retries=10,
                retry_on_response=lambda r: r.status_code >= 500,
                backoff_base=0.5,
                backoff_max=2.0,
            ),
            sleep_fn=recording_sleep,
        )
        iter_coroutine(rc.send("GET", "/test"))
        assert all(d <= 2.0 for d in delays)


class TestSendWithRetryCallables:
    def test_rebuilds_headers_and_body_per_attempt(self) -> None:
        responses = [_make_response(503), _make_response(200)]
        transport = FakeTransport(responses)
        rc = RequestClient(
            transport=transport,
            base_headers={"authorization": "Bearer retry-token"},
            retry=RetryPolicy(
                retries=1,
                retry_on_response=lambda r: r.status_code >= 500,
            ),
            sleep_fn=_noop_sleep,
        )

        header_attempts: list[int] = []
        body_attempts: list[int] = []

        def headers_factory(attempt: int) -> dict[str, str]:
            header_attempts.append(attempt)
            return {"x-attempt": str(attempt)}

        def body_factory(attempt: int) -> RequestBody:
            body_attempts.append(attempt)
            return JSONBody({"attempt": attempt})

        resp = iter_coroutine(
            rc.send(
                "POST",
                "/retry-factory",
                headers=headers_factory,
                body=body_factory,
            )
        )

        assert resp.status_code == 200
        assert header_attempts == [0, 1]
        assert body_attempts == [0, 1]
        assert len(transport.calls) == 2
        assert transport.calls[0]["headers"]["x-attempt"] == "0"
        assert transport.calls[1]["headers"]["x-attempt"] == "1"
        assert transport.calls[0]["body"] == JSONBody({"attempt": 0})
        assert transport.calls[1]["body"] == JSONBody({"attempt": 1})
        assert transport.calls[0]["headers"]["authorization"] == "Bearer retry-token"
        assert transport.calls[1]["headers"]["authorization"] == "Bearer retry-token"

    def test_factory_method_retries_network_errors(self) -> None:
        success = _make_response(200)
        transport = RaisingTransport(fail_count=2, success_response=success)
        rc = RequestClient(
            transport=transport,
            retry=RetryPolicy(retries=2, retry_on_network_error=True),
            sleep_fn=_noop_sleep,
        )

        resp = iter_coroutine(
            rc.send(
                "GET",
                "/retry-network",
                headers=lambda attempt: {"x-attempt": str(attempt)},
                body=lambda attempt: None,
            )
        )

        assert resp.status_code == 200
        assert transport._call_count == 3


class TestSyncTransportEndToEnd:
    def test_with_real_sync_transport(self) -> None:
        """Test that RequestClient works with SyncTransport + iter_coroutine."""
        import httpx

        # Create a real httpx.Client with a mock transport
        mock_transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"ok": True}))
        http_client = httpx.Client(transport=mock_transport, base_url="https://api.example.com")
        transport = SyncTransport(http_client)

        rc = RequestClient(
            transport=transport,
            base_headers={
                "authorization": "Bearer test-token",
                "user-agent": "test/1.0",
            },
            base_params={"teamId": "team_abc"},
            sleep_fn=_noop_sleep,
        )
        resp = iter_coroutine(rc.send("GET", "/v1/resource"))
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

        rc.close()
