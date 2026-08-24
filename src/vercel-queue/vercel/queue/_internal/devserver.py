"""Embedded queue development server helpers."""

from __future__ import annotations

from typing import Any

import argparse
import contextlib
import cProfile
import importlib
import json
import signal
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass

from ..embedded import create_embedded_queue_app
from .asgi import QueueClientAsgiApp, asgi_app
from .client import QueueClient
from .config import CURRENT_DEPLOYMENT, BaseUrl, DeploymentOption
from .embedded import EmbeddedQueueDevServer
from .http import AsyncHttpClientFactory
from .types import Duration


@dataclass(frozen=True, kw_only=True)
class QueueClientAsgiDevServer:
    """Running queue client ASGI dev server."""

    base_url: str
    app: QueueClientAsgiApp
    _server: Any
    _thread: threading.Thread

    def is_running(self) -> bool:
        return self._thread.is_alive() and not self._server.should_exit


@contextlib.contextmanager
def embedded_queue_dev_server(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    manual_clock: bool = True,
    profile: str | None = None,
) -> Iterator[EmbeddedQueueDevServer]:
    """Run the embedded queue server on a localhost HTTP port."""
    uvicorn = _uvicorn()
    app = create_embedded_queue_app(manual_clock=manual_clock)
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        lifespan="off",
        ws="none",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=_profiled_server_run(server, profile), daemon=True)
    thread.start()
    _wait_for_server(server)
    base_url = f"http://{host}:{_server_port(server)}"
    try:
        yield EmbeddedQueueDevServer(
            state=app.state,
            base_url=base_url,
            app=app,
            _thread=thread,
        )
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("embedded queue dev server did not stop")
        app.state.close()


@contextlib.contextmanager
def queue_client_asgi_dev_server(  # noqa: PLR0913
    *,
    client: QueueClient | None = None,
    host: str = "127.0.0.1",
    port: int = 0,
    token: str | None = None,
    region: str | None = None,
    base_url: BaseUrl | None = None,
    deployment: DeploymentOption = CURRENT_DEPLOYMENT,
    headers: Mapping[str, str] | None = None,
    timeout: Duration | None = 10.0,
    http_client_factory: AsyncHttpClientFactory | None = None,
    profile: str | None = None,
) -> Iterator[QueueClientAsgiDevServer]:
    """Run a queue client ASGI callback app on a localhost HTTP port."""
    uvicorn = _uvicorn()
    app = asgi_app(
        client=client,
        token=token,
        region=region,
        base_url=base_url,
        deployment=deployment,
        headers=headers,
        timeout=timeout,
        http_client_factory=http_client_factory,
    )
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        lifespan="on",
        ws="none",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=_profiled_server_run(server, profile), daemon=True)
    thread.start()
    _wait_for_server(server, "queue client ASGI dev server")
    try:
        yield QueueClientAsgiDevServer(
            base_url=f"http://{host}:{_server_port(server)}",
            app=app,
            _server=server,
            _thread=thread,
        )
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("queue client ASGI dev server did not stop")


def main(argv: list[str] | None = None) -> int:
    """Run an embedded queue HTTP server for cross-runtime tests."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="host interface to bind")
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="port to bind, or 0 for a random available port (default)",
    )
    args = parser.parse_args(argv)

    stop = threading.Event()

    def _handle_signal(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    with embedded_queue_dev_server(
        host=args.host,
        port=args.port,
        manual_clock=False,
    ) as server:
        print(json.dumps({"baseUrl": server.base_url}), flush=True)  # noqa: T201

        while not stop.wait(0.1):
            if not server.is_running():
                raise RuntimeError("embedded queue server stopped unexpectedly")

    return 0


def _uvicorn() -> Any:
    try:
        return importlib.import_module("uvicorn")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Queue dev server support requires uvicorn. Install it with 'vercel-queue[devserver]'."
        ) from exc


def _wait_for_server(server: Any, name: str = "embedded queue dev server") -> None:
    deadline = time.monotonic() + 5
    while not server.started:
        if not server.should_exit and time.monotonic() < deadline:
            time.sleep(0.01)
            continue
        raise RuntimeError(f"{name} did not start")


def _server_port(server: Any) -> int:
    for asyncio_server in server.servers:
        sockets = asyncio_server.sockets or ()
        for sock in sockets:
            address = sock.getsockname()
            if isinstance(address, tuple):
                return int(address[1])
    raise RuntimeError("embedded queue dev server did not expose a TCP port")


def _profiled_server_run(server: Any, profile: str | None) -> Callable[[], None]:
    if profile is None:
        return server.run

    def run() -> None:
        profiler = cProfile.Profile()
        try:
            profiler.runcall(server.run)
        finally:
            profiler.dump_stats(profile)

    return run
