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


@pytest.mark.parametrize(
    ("environment", "timeout", "expected_interval"),
    [
        ("production", None, 300.0),
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
    monkeypatch.setenv("VERCEL_ENV", environment)
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
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


@pytest.mark.parametrize(
    ("forwarded_host", "expected"),
    [
        # Environment aliases route only to the promoted deployment.
        ("test-app.vercel.app", True),
        ("www.example.com", True),
        # The deployment's own URL reaches it forever; proves nothing.
        ("app-abc123-team.vercel.app", False),
        # The branch URL tracks the branch's newest deployment, which is
        # exactly wrong during a rollback.
        ("app-git-main-team.vercel.app", False),
        (None, False),
    ],
)
def test_alias_routed_requests_are_the_takeover_signal(
    monkeypatch: pytest.MonkeyPatch,
    forwarded_host: str | None,
    expected: bool,  # noqa: FBT001 - parametrized expectation
) -> None:
    invocation_hooks = ModuleType("vercel_runtime.invocation_hooks")
    invocation_hooks.__dict__["current_forwarded_host"] = lambda: forwarded_host
    runtime = ModuleType("vercel_runtime")
    runtime.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)
    monkeypatch.setitem(
        sys.modules,
        "vercel_runtime.invocation_hooks",
        invocation_hooks,
    )
    monkeypatch.setenv("VERCEL_URL", "app-abc123-team.vercel.app")
    monkeypatch.setenv("VERCEL_BRANCH_URL", "app-git-main-team.vercel.app")

    assert _automatic._request_is_alias_routed() is expected


def test_runtime_without_host_support_disables_takeover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation_hooks = ModuleType("vercel_runtime.invocation_hooks")
    runtime = ModuleType("vercel_runtime")
    runtime.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)
    monkeypatch.setitem(
        sys.modules,
        "vercel_runtime.invocation_hooks",
        invocation_hooks,
    )
    monkeypatch.setattr(_automatic, "_takeover_warning_emitted", False)

    assert _automatic._request_is_alias_routed() is False


def test_malformed_subscriber_entry_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The builder owns this variable; a bad entry is a regression, not noise."""
    monkeypatch.setenv(_automatic.SUBSCRIBERS_ENV, '[{"id":"scheduler"}]')

    with pytest.raises(APSchedulerConfigurationError, match="malformed"):
        _automatic._configured_schedulers()


def test_preview_automatic_activation_is_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler","entrypoint":"scheduler:scheduler"}]',
    )
    monkeypatch.delenv(
        _automatic.PREVIEW_IDLE_TIMEOUT_ENV,
        raising=False,
    )

    assert not _automatic._automatic_environment()


def test_custom_environments_activate_automatically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A custom environment is a named environment, not a preview.

    It reports ``VERCEL_ENV=preview``, so the gate must resolve the
    environment exactly the way durable state scoping does.
    """
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("VERCEL_TARGET_ENV", "staging")
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler","entrypoint":"scheduler:scheduler"}]',
    )

    assert _automatic._automatic_environment()


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
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler_scheduler")
    monkeypatch.setenv(
        _automatic.SUBSCRIBERS_ENV,
        '[{"id":"scheduler_scheduler","entrypoint":"scheduler:scheduler"}]',
    )
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "60")

    assert is_queue_serving_runtime()
    _automatic.register_automatic_activation()


def test_automatic_activation_hook_uses_current_preview_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int | None] = []
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "120")

    def record_activation(timeout: int | None, *, takeover_allowed: bool) -> None:
        del takeover_allowed
        calls.append(timeout)

    monkeypatch.setattr(
        _automatic,
        "_activate_configured_schedulers",
        record_activation,
    )

    _automatic._automatic_activation_hook()

    assert calls == [120]


def test_automatic_activation_calls_every_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int | None, bool]] = []

    class FakeAdapter:
        def auto_activate(
            self,
            *,
            idle_timeout_seconds: int | None = None,
            takeover_allowed: bool = False,
        ) -> None:
            calls.append((idle_timeout_seconds, takeover_allowed))

    monkeypatch.setattr(
        _automatic,
        "_configured_schedulers",
        lambda: [object(), object()],
    )
    monkeypatch.setattr(_adapter, "get_adapter", lambda scheduler: FakeAdapter())

    _automatic._activate_configured_schedulers(1800, takeover_allowed=True)

    assert calls == [(1800, True), (1800, True)]


def test_preview_timeout_must_be_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "0")

    with pytest.raises(APSchedulerConfigurationError, match="positive integer"):
        _automatic._preview_idle_timeout()
