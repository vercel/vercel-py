from __future__ import annotations

import contextlib
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from types import SimpleNamespace

import anyio
import pytest

import vercel.queue._internal.devserver as queue_devserver_internal
import vercel.queue.devserver as queue_devserver
from vercel.queue.devserver import EmbeddedQueueDevServer


def test_devserver_exports_expected_symbols() -> None:
    assert queue_devserver.EmbeddedQueueDevServer is EmbeddedQueueDevServer
    assert callable(queue_devserver.embedded_queue_dev_server)


def test_devserver_main_defaults_to_random_port(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[dict[str, object]] = []

    @dataclass(frozen=True)
    class _Server:
        base_url: str = "http://127.0.0.1:54321"

        def is_running(self) -> bool:
            return False

    @contextlib.contextmanager
    def _server(**kwargs: object) -> Iterator[_Server]:
        calls.append(kwargs)
        yield _Server()

    monkeypatch.setattr(queue_devserver_internal, "embedded_queue_dev_server", _server)

    with pytest.raises(RuntimeError, match="stopped unexpectedly"):
        queue_devserver.main(["--host", "127.0.0.1"])

    assert calls == [
        {
            "host": "127.0.0.1",
            "port": 0,
            "manual_clock": False,
        }
    ]
    assert capsys.readouterr().out == '{"baseUrl": "http://127.0.0.1:54321"}\n'


def test_devserver_shutdown_cancels_a_stuck_server() -> None:
    class _Server:
        should_exit = False
        force_exit = False
        cancelled = False

        def cancel(self) -> None:
            self.cancelled = True

    class _Thread(threading.Thread):
        def __init__(self, server: _Server) -> None:
            super().__init__()
            self._server = server
            self.join_timeouts: list[float] = []

        def join(self, timeout: float | None = None) -> None:
            assert timeout is not None
            self.join_timeouts.append(timeout)

        def is_alive(self) -> bool:
            return not self._server.cancelled

    server = _Server()
    thread = _Thread(server)

    queue_devserver_internal._stop_server(server, thread, "test server", timeout=2.5)

    assert server.should_exit is True
    assert server.force_exit is True
    assert server.cancelled is True
    assert thread.join_timeouts == [3.5, 2.5]


def test_cancellable_devserver_stops_its_event_loop_task() -> None:
    started = threading.Event()

    class _Config:
        def get_loop_factory(self) -> None:
            return None

    class _Server:
        def __init__(self, config: object) -> None:
            self.config = config

        async def serve(self, sockets: list[object] | None = None) -> None:
            started.set()
            await anyio.sleep_forever()

    uvicorn = SimpleNamespace(Server=_Server)
    server = queue_devserver_internal._cancellable_server(uvicorn, _Config())
    thread = threading.Thread(target=server.run)
    thread.start()

    assert started.wait(timeout=5)
    server.cancel()
    thread.join(timeout=5)

    assert not thread.is_alive()
