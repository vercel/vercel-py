from __future__ import annotations

from typing import Any

import sys
from types import ModuleType

import pytest

from vercel.integrations.apscheduler import (
    APSchedulerConfigurationError,
    _adapter,
    _automatic,
)
from vercel.integrations.apscheduler._options import is_queue_serving_runtime


def test_registers_request_driven_automatic_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered: list[tuple[str, Any, float | None]] = []
    invocation_hooks = ModuleType("vercel_runtime.invocation_hooks")

    def run_on_next_invocation(
        name: str,
        callback: Any,
        *,
        repeat_after_seconds: float | None = None,
    ) -> None:
        registered.append((name, callback, repeat_after_seconds))

    runtime = ModuleType("vercel_runtime")
    runtime.__path__ = []  # type: ignore[attr-defined]
    invocation_hooks.__dict__["run_on_next_invocation"] = run_on_next_invocation
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)
    monkeypatch.setitem(
        sys.modules,
        "vercel_runtime.invocation_hooks",
        invocation_hooks,
    )
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler","entrypoint":"scheduler:scheduler"}]',
    )

    _automatic.register_automatic_activation()

    assert registered == [
        (
            _automatic.ACTIVATION_HOOK_NAME,
            _automatic._automatic_activation_hook,
            _automatic.HEAL_SWEEP_INTERVAL_SECONDS,
        )
    ]


def test_malformed_subscriber_entry_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The builder owns this variable; a bad entry is a regression, not noise."""
    monkeypatch.setenv(_automatic.SUBSCRIBERS_ENV, '[{"id":"scheduler"}]')

    with pytest.raises(APSchedulerConfigurationError, match="malformed"):
        _automatic._configured_schedulers()


def test_preview_deployments_do_not_activate_automatically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler","entrypoint":"scheduler:scheduler"}]',
    )

    assert not _automatic._automatic_environment()


def test_subscriber_request_does_not_register_automatic_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation_hooks = ModuleType("vercel_runtime.invocation_hooks")

    def run_on_next_invocation(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        pytest.fail("subscriber requests must not register automatic activation")

    runtime = ModuleType("vercel_runtime")
    runtime.__path__ = []  # type: ignore[attr-defined]
    invocation_hooks.__dict__["run_on_next_invocation"] = run_on_next_invocation
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)
    monkeypatch.setitem(
        sys.modules,
        "vercel_runtime.invocation_hooks",
        invocation_hooks,
    )
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler_scheduler")
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler_scheduler","entrypoint":"scheduler:scheduler"}]',
    )

    assert is_queue_serving_runtime()
    _automatic.register_automatic_activation()


def test_automatic_activation_calls_every_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeAdapter:
        def auto_activate(self) -> None:
            calls.append("activated")

    monkeypatch.setattr(
        _automatic,
        "_configured_schedulers",
        lambda: [object(), object()],
    )
    monkeypatch.setattr(_adapter, "get_adapter", lambda scheduler: FakeAdapter())

    _automatic._activate_configured_schedulers()

    assert calls == ["activated", "activated"]
