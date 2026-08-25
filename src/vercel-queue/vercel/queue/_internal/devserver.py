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
from collections.abc import Callable, Iterator

from ..embedded import create_embedded_queue_app
from .embedded import EmbeddedQueueDevServer


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
