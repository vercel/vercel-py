"""The default async session must follow the running event loop.

These tests use a real loopback server rather than `respx`, because the bug they
guard lives in the keep-alive connection pool: a mocked transport never pools a
socket, so it cannot reproduce it.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import cast

import httpx2 as httpx
import pytest

from vercel._internal.core.errors import VercelSessionClosedError
from vercel._internal.core.http import AsyncTransport
from vercel._internal.core.http.transport import ReadResponsePolicy
from vercel._internal.core.session import SdkSession


class _CountingServer(ThreadingHTTPServer):
    """Loopback server that records how many requests it actually served."""

    daemon_threads = True

    def __init__(self, handler: type[BaseHTTPRequestHandler]) -> None:
        super().__init__(("127.0.0.1", 0), handler)
        self.request_count = 0


class _KeepAliveHandler(BaseHTTPRequestHandler):
    # Keep-alive, so the client pools the socket for a later loop to inherit.
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:
        cast(_CountingServer, self.server).request_count += 1
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """Silence the default stderr access log."""


@contextlib.contextmanager
def _keep_alive_server() -> Iterator[_CountingServer]:
    server = _CountingServer(_KeepAliveHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def isolated_default_session() -> Iterator[None]:
    """Swap the process-wide default session out for the duration of a test."""
    previous = SdkSession._default
    SdkSession._default = None
    try:
        yield
    finally:
        created = SdkSession._default
        SdkSession._default = previous
        if created is not None:
            # Best effort: its sockets may belong to a loop that already closed.
            with contextlib.suppress(RuntimeError):
                asyncio.run(created.aclose())


def _url(server: _CountingServer) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}/mint"


async def _post(transport: AsyncTransport, url: str) -> int:
    response = await transport.send("POST", url, body=None, read_response=ReadResponsePolicy.ALWAYS)
    return response.status_code


def test_default_session_transport_survives_successive_event_loops(
    isolated_default_session: None,
) -> None:
    with _keep_alive_server() as server:
        url = _url(server)

        async def call() -> int:
            return await _post(SdkSession.default().get_transport(), url)

        statuses = [asyncio.run(call()) for _ in range(4)]

        assert statuses == [200, 200, 200, 200]
        # Before the fix half the callers failed *after* the server had processed
        # their request, so a retry would have duplicated a non-idempotent call.
        assert server.request_count == 4


def test_memoized_service_does_not_keep_a_transport_bound_to_a_dead_loop(
    isolated_default_session: None,
) -> None:
    """Services capture the transport once, so the fix cannot live in `get_transport`.

    `vercel.sandbox` resolves its service per call, but the service keeps the
    transport it was built with, reaching a stale client without ever asking for
    a transport again.
    """

    class CapturingService:
        def __init__(self, transport: AsyncTransport) -> None:
            self.transport = transport

    with _keep_alive_server() as server:
        url = _url(server)
        services: list[CapturingService] = []

        async def call() -> int:
            session = SdkSession.default()

            def factory() -> CapturingService:
                service = CapturingService(session.get_transport())
                services.append(service)
                return service

            service = session.get_or_create_service(CapturingService, factory)
            return await _post(service.transport, url)

        statuses = [asyncio.run(call()) for _ in range(3)]

        assert statuses == [200, 200, 200]
        assert server.request_count == 3
        # The service itself is still memoized: only its client changed.
        assert len(services) == 1


async def test_a_closed_session_does_not_build_another_client() -> None:
    """A request still holding the transport must not outlive its session.

    Closing empties the per-loop registry, so without this the next lookup would
    treat the session as cold, build a client, and run the request outside the
    lifecycle that was supposed to end.
    """
    with _keep_alive_server() as server:
        session = SdkSession()
        transport = session.get_transport()
        await session.aclose()

        with pytest.raises(VercelSessionClosedError):
            await _post(transport, _url(server))

        assert server.request_count == 0
        assert len(session._clients) == 0


def test_concurrent_loops_in_separate_threads_each_get_their_own_client(
    isolated_default_session: None,
) -> None:
    """A session reachable from several threads must not hand a client across loops."""
    thread_count = 4

    with _keep_alive_server() as server:
        url = _url(server)
        barrier = threading.Barrier(thread_count)
        results: list[object] = []
        # Held, not just recorded: a dropped client's address can be reused, so
        # comparing `id()` of released objects would be unsound.
        clients: list[httpx.AsyncClient] = []
        lock = threading.Lock()

        async def warm_up() -> int:
            return await _post(SdkSession.default().get_transport(), url)

        # Memoize the transport first. Otherwise the threads race to create it and
        # each builds its own client, passing without the per-request lookup.
        assert asyncio.run(warm_up()) == 200

        async def call() -> int:
            session = SdkSession.default()
            transport = session.get_transport()
            # Overlap the requests so every loop is live at the same time.
            await asyncio.to_thread(barrier.wait)
            with lock:
                clients.append(session._client_for_running_loop())
            return await _post(transport, url)

        def worker() -> None:
            try:
                outcome: object = asyncio.run(call())
            except BaseException as exc:  # noqa: BLE001
                outcome = exc
            with lock:
                results.append(outcome)

        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert results == [200] * thread_count
        assert server.request_count == thread_count + 1
        # A client each, so no loop is handed sockets opened by another.
        assert len({id(client) for client in clients}) == thread_count
