from __future__ import annotations

from collections.abc import Iterator

import pytest

from vercel.queue._internal.http import reset_http_client_pools_for_tests
from vercel.queue._internal.lease import reset_lease_renewal_worker_for_tests
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.testing import (
    clear_subscriptions as clear_queue_subscriptions,
    reset_default_queue_clients as reset_queue_clients,
)


def _clear_subscriptions() -> Iterator[None]:
    clear_queue_subscriptions()
    try:
        yield
    finally:
        clear_queue_subscriptions()


@pytest.fixture
def isolated_subscriptions() -> Iterator[None]:
    yield from _clear_subscriptions()


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(autouse=True)
def queue_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_REGION", "iad1")


@pytest.fixture(autouse=True)
def reset_default_queue_clients() -> Iterator[None]:
    reset_lease_renewal_worker_for_tests()
    reset_http_client_pools_for_tests()
    reset_queue_clients()

    yield

    reset_lease_renewal_worker_for_tests()
    reset_http_client_pools_for_tests()
    reset_queue_clients()


@pytest.fixture
def embedded_queue_module_env(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
) -> EmbeddedQueueDevServer:
    monkeypatch.setenv("VERCEL_QUEUE_BASE_URL", eqs.base_url)
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "token")
    return eqs
