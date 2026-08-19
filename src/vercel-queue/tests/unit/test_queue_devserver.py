from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass

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
