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
        ("development", None, 300.0),
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


def _reset_hook_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_automatic, "_unsettled", False)
    monkeypatch.setattr(_automatic, "_last_sweep", None)


def test_automatic_activation_hook_uses_current_preview_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int | None] = []
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "120")
    _reset_hook_state(monkeypatch)

    def record_activation(timeout: int | None, *, takeover_allowed: bool) -> bool:
        del takeover_allowed
        calls.append(timeout)
        return True

    monkeypatch.setattr(
        _automatic,
        "_activate_configured_schedulers",
        record_activation,
    )

    assert _automatic._automatic_activation_hook() is None

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
        ) -> bool:
            calls.append((idle_timeout_seconds, takeover_allowed))
            return True

    monkeypatch.setattr(
        _automatic,
        "_configured_schedulers",
        lambda: [object(), object()],
    )
    monkeypatch.setattr(_adapter, "get_adapter", lambda scheduler: FakeAdapter())

    assert _automatic._activate_configured_schedulers(1800, takeover_allowed=True)

    assert calls == [(1800, True), (1800, True)]


def _fake_forwarded_host(
    monkeypatch: pytest.MonkeyPatch,
    host_holder: dict[str, str | None],
) -> None:
    invocation_hooks = ModuleType("vercel_runtime.invocation_hooks")
    invocation_hooks.__dict__["current_forwarded_host"] = lambda: host_holder["host"]
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


def test_own_host_run_does_not_blind_the_alias_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bug this fixes: takeover must not depend on request ordering.

    A request through the deployment's own URL proves nothing; its hook run
    must not consume the sweep window, so the alias-routed request seconds
    later still takes the chain over.
    """
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.delenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, raising=False)
    _reset_hook_state(monkeypatch)
    host_holder: dict[str, str | None] = {"host": "app-abc123-team.vercel.app"}
    _fake_forwarded_host(monkeypatch, host_holder)
    sweeps: list[bool] = []

    def record_activation(timeout: int | None, *, takeover_allowed: bool) -> bool:
        del timeout
        sweeps.append(takeover_allowed)
        return takeover_allowed  # settles only once a takeover was allowed

    monkeypatch.setattr(
        _automatic,
        "_activate_configured_schedulers",
        record_activation,
    )

    # First request arrives through the deployment URL: sweep runs, proves
    # nothing, and the hook asks to stay eligible.
    assert _automatic._automatic_activation_hook() == pytest.approx(0.0)
    # Second request, seconds later through the production alias: it must
    # sweep again immediately and settle.
    host_holder["host"] = "test-app.vercel.app"
    assert _automatic._automatic_activation_hook() is None

    assert sweeps == [False, True]


def test_unsettled_own_host_requests_skip_the_sweep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """While a takeover is owed, own-host traffic must cost no Redis calls."""
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.delenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, raising=False)
    _reset_hook_state(monkeypatch)
    monkeypatch.setattr(_automatic, "_unsettled", True)
    monkeypatch.setattr(_automatic, "_last_sweep", _automatic.monotonic())
    host_holder: dict[str, str | None] = {"host": "app-abc123-team.vercel.app"}
    _fake_forwarded_host(monkeypatch, host_holder)

    def unexpected_sweep(timeout: int | None, *, takeover_allowed: bool) -> bool:
        del timeout, takeover_allowed
        pytest.fail("an own-host request while unsettled must not sweep")

    monkeypatch.setattr(
        _automatic,
        "_activate_configured_schedulers",
        unexpected_sweep,
    )

    assert _automatic._automatic_activation_hook() == pytest.approx(0.0)


def test_unsettled_sweeps_resume_after_the_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The interval fallback resyncs stale state, for example after a
    manual start() through a user endpoint, even without alias traffic."""
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.delenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, raising=False)
    _reset_hook_state(monkeypatch)
    monkeypatch.setattr(_automatic, "_unsettled", True)
    monkeypatch.setattr(
        _automatic,
        "_last_sweep",
        _automatic.monotonic() - _automatic.HEAL_SWEEP_INTERVAL_SECONDS - 1,
    )
    host_holder: dict[str, str | None] = {"host": "app-abc123-team.vercel.app"}
    _fake_forwarded_host(monkeypatch, host_holder)
    sweeps: list[bool] = []

    def record_activation(timeout: int | None, *, takeover_allowed: bool) -> bool:
        del timeout
        sweeps.append(takeover_allowed)
        return True

    monkeypatch.setattr(
        _automatic,
        "_activate_configured_schedulers",
        record_activation,
    )

    assert _automatic._automatic_activation_hook() is None

    assert sweeps == [False]


def test_preview_timeout_must_be_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "0")

    with pytest.raises(APSchedulerConfigurationError, match="positive integer"):
        _automatic._preview_idle_timeout()


def test_custom_environments_never_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A custom environment reports VERCEL_ENV=preview but must not idle.

    Its chain is environment-scoped and behaves like production; applying a
    preview idle deadline would silently stop its jobs between requests.
    """
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("VERCEL_TARGET_ENV", "staging")
    monkeypatch.setenv(_automatic.PREVIEW_IDLE_TIMEOUT_ENV, "120")

    assert _automatic._preview_idle_timeout() is None
