from __future__ import annotations

import contextlib
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from types import SimpleNamespace

import anyio
import pytest
from anyio import to_thread

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


def test_running_devserver_passes_its_liveness_check() -> None:
    with queue_devserver.embedded_queue_dev_server() as server:
        server.get_sync_client().send("emails", {"ok": True})

        server.ensure_running()

    with pytest.raises(RuntimeError, match="stopped unexpectedly"):
        server.ensure_running()


class _LoopEndedError(Exception):
    pass


def _leaves(error: BaseException | None) -> list[type[BaseException]]:
    nested = getattr(error, "exceptions", None)
    if nested is None:
        return [] if error is None else [type(error)]
    return [leaf for each in nested for leaf in _leaves(each)]


# The crashed server thread still reports its error through threading.excepthook.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
@pytest.mark.parametrize(("ending", "cause"), [("crash", [_LoopEndedError]), ("exit", [])])
def test_stopped_devserver_fails_its_liveness_check_with_the_reason(
    monkeypatch: pytest.MonkeyPatch,
    ending: str,
    cause: list[type[BaseException]],
) -> None:
    # A server that dies mid-session must name why, instead of leaving later
    # tests to fail one by one with connection errors.
    uvicorn = pytest.importorskip("uvicorn")
    end_loop = threading.Event()

    async def main_loop(self: object) -> None:
        del self
        await to_thread.run_sync(end_loop.wait)
        if ending == "crash":
            raise _LoopEndedError

    monkeypatch.setattr(uvicorn.Server, "main_loop", main_loop)

    with queue_devserver.embedded_queue_dev_server() as server:
        server.ensure_running()
        end_loop.set()
        assert server._thread is not None
        server._thread.join(timeout=10)

        with pytest.raises(RuntimeError, match="stopped unexpectedly") as exc_info:
            server.ensure_running()

    assert _leaves(exc_info.value.__cause__) == cause
