from __future__ import annotations

from typing import Any

import sys
from types import ModuleType

import pytest

from vercel.integrations.apscheduler import _adapter, _automatic
from vercel.integrations.apscheduler._driver import (
    APSchedulerConfigurationError,
)
from vercel.integrations.apscheduler._options import is_queue_serving_runtime


@pytest.mark.parametrize(
    ("environment", "timeout", "expected_interval"),
    [
        ("production", None, None),
        ("preview", "1800", 300.0),
        ("preview", "60", 20.0),
    ],
)
def test_registers_request_driven_automatic_activation(
    monkeypatch: pytest.MonkeyPatch,
    environment: str,
    timeout: str | None,
    expected_interval: float | None,
) -> None:
    registered: list[tuple[str, Any, float | None]] = []
    invocation_hooks = ModuleType("vercel_runtime.invocation_hooks")

    def register_invocation_hook(
        name: str,
        callback: Any,
        *,
        min_interval_seconds: float | None = None,
    ) -> None:
        registered.append((name, callback, min_interval_seconds))

    runtime = ModuleType("vercel_runtime")
    runtime.__path__ = []  # type: ignore[attr-defined]
    invocation_hooks.__dict__["register_invocation_hook"] = register_invocation_hook
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)
    monkeypatch.setitem(
        sys.modules,
        "vercel_runtime.invocation_hooks",
        invocation_hooks,
    )
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", environment)
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler","entrypoint":"scheduler:scheduler"}]',
    )
    if timeout is None:
        monkeypatch.delenv(
            _automatic.PREVIEW_IDLE_TIMEOUT_ENV,
            raising=False,
        )
    else:
        monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, timeout)

    _automatic.register_automatic_activation()

    assert len(registered) == 1
    name, callback, interval = registered[0]
    assert name == _automatic.ACTIVATION_HOOK_NAME
    assert callback is _automatic._automatic_activation_hook
    assert interval == expected_interval


def test_preview_automatic_activation_is_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler","entrypoint":"scheduler:scheduler"}]',
    )
    monkeypatch.delenv(
        _automatic.PREVIEW_IDLE_TIMEOUT_ENV,
        raising=False,
    )

    assert not _automatic._automatic_environment()


def test_subscriber_request_does_not_register_automatic_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation_hooks = ModuleType("vercel_runtime.invocation_hooks")

    def register_invocation_hook(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        pytest.fail("subscriber requests must not register automatic activation")

    runtime = ModuleType("vercel_runtime")
    runtime.__path__ = []  # type: ignore[attr-defined]
    invocation_hooks.__dict__["register_invocation_hook"] = register_invocation_hook
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)
    monkeypatch.setitem(
        sys.modules,
        "vercel_runtime.invocation_hooks",
        invocation_hooks,
    )
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler_scheduler")
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler_scheduler","entrypoint":"scheduler:scheduler"}]',
    )
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "60")

    assert is_queue_serving_runtime()
    _automatic.register_automatic_activation()


@pytest.mark.asyncio
async def test_automatic_activation_hook_runs_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int | None] = []
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "120")
    monkeypatch.setattr(
        _automatic,
        "_activate_configured_schedulers",
        calls.append,
    )

    await _automatic._automatic_activation_hook()

    assert calls == [120]


def test_automatic_activation_calls_every_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int | None] = []

    class FakeAdapter:
        def auto_activate(
            self,
            *,
            idle_timeout_seconds: int | None = None,
        ) -> None:
            calls.append(idle_timeout_seconds)

    monkeypatch.setattr(
        _automatic,
        "_configured_schedulers",
        lambda: [object(), object()],
    )
    monkeypatch.setattr(_adapter, "get_adapter", lambda scheduler: FakeAdapter())

    _automatic._activate_configured_schedulers(1800)

    assert calls == [1800, 1800]


def test_preview_timeout_must_be_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "0")

    with pytest.raises(APSchedulerConfigurationError, match="positive integer"):
        _automatic._preview_idle_timeout()
