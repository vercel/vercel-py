"""Pytest fixtures for the embedded queue server."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from vercel.queue.devserver import EmbeddedQueueDevServer, embedded_queue_dev_server


@pytest.fixture(scope="session")
def _embedded_queue_server_session() -> Iterator[EmbeddedQueueDevServer]:
    """Run one embedded queue server process for the test session."""
    with embedded_queue_dev_server() as server:
        yield server


@pytest.fixture
def embedded_queue_server(
    _embedded_queue_server_session: EmbeddedQueueDevServer,
) -> Iterator[EmbeddedQueueDevServer]:
    """Return an isolated embedded queue server state for a test."""
    _embedded_queue_server_session.reset()
    try:
        yield _embedded_queue_server_session
    finally:
        _embedded_queue_server_session.reset()


@pytest.fixture
def eqs(embedded_queue_server: EmbeddedQueueDevServer) -> EmbeddedQueueDevServer:
    """Alias for the embedded queue server fixture."""
    return embedded_queue_server


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "EmbeddedQueueDevServer",
    "embedded_queue_server",
    "eqs",
)
