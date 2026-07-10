from __future__ import annotations

from typing import Any, ClassVar, cast

from collections.abc import Iterator
from datetime import timedelta

import pytest
from celery import Celery as CeleryApp, group
from celery.app.defaults import DEFAULTS as CELERY_DEFAULTS
from celery.backends.base import BackendGetMetaError, BackendStoreError

import vercel.integrations.celery as public_vqs_celery
import vercel.integrations.celery._broker as vqs_celery
import vercel.integrations.celery._result_backend as vqs_celery_backend


def install_result_backend_integration() -> None:
    public_vqs_celery.install_vercel_celery_integration(
        register_queues=False,
        set_default_broker=False,
    )


def unwrap_cache_payload(value: object) -> str | bytes:
    return vqs_celery_backend.VercelRuntimeCacheBackend._unwrap_value(value)


def stored_wrapper(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast("dict[str, object]", value)


class FakeRuntimeCache:
    instances: ClassVar[list[FakeRuntimeCache]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.values: dict[str, object] = {}
        self.set_options: dict[str, dict | None] = {}
        self.deleted: list[str] = []
        self.get_error: Exception | None = None
        self.set_error: Exception | None = None
        self.delete_error: Exception | None = None
        FakeRuntimeCache.instances.append(self)

    def get(self, key: str) -> object | None:
        if self.get_error is not None:
            raise self.get_error
        return self.values.get(key)

    def set(self, key: str, value: object, options: dict | None = None) -> None:
        if self.set_error is not None:
            raise self.set_error
        self.values[key] = value
        self.set_options[key] = options

    def delete(self, key: str) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(key)
        self.values.pop(key, None)


@pytest.fixture(autouse=True)
def clean_celery_integration_state() -> Iterator[None]:
    original_broker_url = CELERY_DEFAULTS.get("broker_url")
    original_result_backend = CELERY_DEFAULTS.get("result_backend")
    CELERY_DEFAULTS["broker_url"] = None
    CELERY_DEFAULTS["result_backend"] = None
    vqs_celery._registered_app_queues.clear()
    vqs_celery._registered_callbacks.clear()
    vqs_celery._push_channels.clear()
    vqs_celery._finalize_hook_state.installed = False
    try:
        yield
    finally:
        CELERY_DEFAULTS["broker_url"] = original_broker_url
        CELERY_DEFAULTS["result_backend"] = original_result_backend
        vqs_celery._registered_app_queues.clear()
        vqs_celery._registered_callbacks.clear()
        vqs_celery._push_channels.clear()
        vqs_celery._finalize_hook_state.installed = False


@pytest.fixture(autouse=True)
def clean_vercel_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL", raising=False)


@pytest.fixture(autouse=True)
def fake_runtime_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeRuntimeCache.instances.clear()
    monkeypatch.setattr(vqs_celery_backend, "RuntimeCache", FakeRuntimeCache)


def test_runtime_cache_result_backend_round_trips_success_result() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-success", backend="vercel-runtime-cache://")
    app.conf.result_backend_transport_options = {"namespace": "results", "ttl": 60}

    app.backend.store_result("task-1", 42, "SUCCESS")

    assert app.backend.get_task_meta("task-1") == {
        "status": "SUCCESS",
        "result": 42,
        "traceback": None,
        "children": None,
        "date_done": app.backend.get_task_meta("task-1")["date_done"],
        "task_id": "task-1",
    }
    cache = FakeRuntimeCache.instances[0]
    stored = stored_wrapper(cache.values["celery-task-meta-task-1"])
    assert stored["__vercel_celery_result__"] == 1
    assert stored["encoding"] == "utf-8"
    assert isinstance(unwrap_cache_payload(stored), str)
    assert cache.kwargs == {
        "key_hash_function": None,
        "namespace": "results",
        "namespace_separator": None,
        "strict": True,
    }
    assert cache.set_options["celery-task-meta-task-1"] == {
        "name": "celery-task-meta-task-1",
        "ttl": 60,
    }


def test_runtime_cache_result_backend_round_trips_failure_result() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-failure", backend="vercel-runtime-cache://")
    error = ValueError("bad value")

    app.backend.store_result("task-1", error, "FAILURE")
    meta = app.backend.get_task_meta("task-1")

    assert meta["status"] == "FAILURE"
    assert isinstance(meta["result"], ValueError)
    assert str(meta["result"]) == "bad value"


def test_runtime_cache_result_backend_round_trips_pickle_bytes_payload() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-pickle", backend="vercel-runtime-cache://")
    app.conf.result_serializer = "pickle"
    app.conf.accept_content = ["pickle"]

    app.backend.store_result("task-1", {"answer": 42}, "SUCCESS")

    cache = FakeRuntimeCache.instances[0]
    stored = stored_wrapper(cache.values["celery-task-meta-task-1"])
    assert stored["encoding"] == "base64"
    assert isinstance(unwrap_cache_payload(stored), bytes)
    assert app.backend.get_task_meta("task-1")["result"] == {"answer": 42}


def test_runtime_cache_result_backend_saves_and_restores_group_result() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-group", backend="vercel-runtime-cache://")
    signature = app.signature("tasks.add", args=(1, 2))
    result = group(signature).freeze(group_id="group-1")

    app.backend.save_group("group-1", result)

    restored = app.backend.restore_group("group-1")

    assert restored is not None
    assert restored.id == "group-1"


def test_runtime_cache_result_backend_transport_options_control_cache_set() -> None:
    def key_hash(key: str) -> str:
        return f"hashed:{key}"

    install_result_backend_integration()
    app = CeleryApp("result-options", backend="vercel-runtime-cache://")
    app.conf.result_backend_transport_options = {
        "key_hash_function": key_hash,
        "namespace": "custom-results",
        "namespace_separator": ":",
        "ttl": 120,
        "tags": ["celery", "results"],
        "name": "",
    }

    app.backend.store_result("task-1", "ok", "SUCCESS")

    cache = FakeRuntimeCache.instances[0]
    assert cache.kwargs == {
        "key_hash_function": key_hash,
        "namespace": "custom-results",
        "namespace_separator": ":",
        "strict": True,
    }
    assert cache.set_options["celery-task-meta-task-1"] == {
        "name": "",
        "ttl": 120,
        "tags": ["celery", "results"],
    }


def test_runtime_cache_result_backend_derives_namespace_from_main() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-app", backend="vercel-runtime-cache://")

    app.backend.store_result("task-1", "ok", "SUCCESS")

    assert FakeRuntimeCache.instances[0].kwargs["namespace"] == "celery-results-result-app"


def test_runtime_cache_result_backend_sanitizes_derived_namespace() -> None:
    install_result_backend_integration()
    app = CeleryApp("result.app", backend="vercel-runtime-cache://")

    app.backend.store_result("task-1", "ok", "SUCCESS")

    assert FakeRuntimeCache.instances[0].kwargs["namespace"] == "celery-results-result_Dapp"


def test_runtime_cache_result_backend_falls_back_without_main() -> None:
    install_result_backend_integration()
    app = CeleryApp(backend="vercel-runtime-cache://")

    app.backend.store_result("task-1", "ok", "SUCCESS")

    assert FakeRuntimeCache.instances[0].kwargs["namespace"] == "celery-results"


def test_runtime_cache_result_backend_capabilities_match_runtime_cache() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-capabilities", backend="vercel-runtime-cache://")

    assert app.backend.supports_native_join is False
    assert app.backend.implements_incr is False
    assert app.backend.supports_autoexpire is True


def test_runtime_cache_result_backend_uses_result_expires_as_default_ttl() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-expires", backend="vercel-runtime-cache://")
    app.conf.result_expires = 30

    app.backend.store_result("task-1", "ok", "SUCCESS")

    cache = FakeRuntimeCache.instances[0]
    assert cache.set_options["celery-task-meta-task-1"] == {
        "name": "celery-task-meta-task-1",
        "ttl": 30,
    }


def test_runtime_cache_result_backend_respects_constructor_expires() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-constructor-expires")
    backend = vqs_celery_backend.VercelRuntimeCacheBackend(app=app, expires=45)

    backend.store_result("task-1", "ok", "SUCCESS")

    cache = FakeRuntimeCache.instances[0]
    assert cache.set_options["celery-task-meta-task-1"] == {
        "name": "celery-task-meta-task-1",
        "ttl": 45,
    }


def test_runtime_cache_result_backend_zero_result_expires_omits_ttl() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-zero-expires", backend="vercel-runtime-cache://")
    app.conf.result_expires = 0

    app.backend.store_result("task-1", "ok", "SUCCESS")

    cache = FakeRuntimeCache.instances[0]
    assert cache.set_options["celery-task-meta-task-1"] == {
        "name": "celery-task-meta-task-1",
    }


def test_runtime_cache_result_backend_explicit_ttl_overrides_result_expires() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-explicit-ttl", backend="vercel-runtime-cache://")
    app.conf.result_expires = 30
    app.conf.result_backend_transport_options = {"ttl": 90}

    app.backend.store_result("task-1", "ok", "SUCCESS")

    options = FakeRuntimeCache.instances[0].set_options["celery-task-meta-task-1"]
    assert options is not None
    assert options["ttl"] == 90


@pytest.mark.parametrize(
    ("ttl", "expected"),
    [
        (12.9, 12),
        (timedelta(seconds=15), 15),
    ],
)
def test_runtime_cache_result_backend_accepts_duration_ttl(
    ttl: object,
    expected: int,
) -> None:
    install_result_backend_integration()
    app = CeleryApp("result-duration-ttl", backend="vercel-runtime-cache://")
    app.conf.result_backend_transport_options = {"ttl": ttl}

    app.backend.store_result("task-1", "ok", "SUCCESS")

    options = FakeRuntimeCache.instances[0].set_options["celery-task-meta-task-1"]
    assert options is not None
    assert options["ttl"] == expected


@pytest.mark.parametrize("ttl", [True, False, 0, 0.0, timedelta(0)])
def test_runtime_cache_result_backend_ignores_non_positive_or_bool_ttl(ttl: object) -> None:
    install_result_backend_integration()
    app = CeleryApp("result-ignored-ttl", backend="vercel-runtime-cache://")
    app.conf.result_backend_transport_options = {"ttl": ttl}

    app.backend.store_result("task-1", "ok", "SUCCESS")

    assert FakeRuntimeCache.instances[0].set_options["celery-task-meta-task-1"] == {
        "name": "celery-task-meta-task-1",
        "ttl": 86400,
    }


def test_runtime_cache_result_backend_forget_deletes_task_key() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-forget", backend="vercel-runtime-cache://")
    app.backend.store_result("task-1", "ok", "SUCCESS")

    app.backend.forget("task-1")

    assert FakeRuntimeCache.instances[0].deleted == ["celery-task-meta-task-1"]


def test_runtime_cache_result_backend_malformed_wrapper_raises_get_error() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-malformed", backend="vercel-runtime-cache://")
    backend = app.backend
    FakeRuntimeCache.instances[0].values["celery-task-meta-task-1"] = {"payload": "broken"}

    with pytest.raises(BackendGetMetaError):
        backend.get_task_meta("task-1")


def test_runtime_cache_result_backend_set_error_propagates() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-set-error", backend="vercel-runtime-cache://")
    backend = app.backend
    FakeRuntimeCache.instances[0].set_error = RuntimeError("set failed")

    with pytest.raises(BackendStoreError):
        backend.store_result("task-1", "ok", "SUCCESS")


def test_runtime_cache_result_backend_delete_error_propagates() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-delete-error", backend="vercel-runtime-cache://")
    app.backend.store_result("task-1", "ok", "SUCCESS")
    FakeRuntimeCache.instances[0].delete_error = RuntimeError("delete failed")

    with pytest.raises(BackendStoreError):
        app.backend.forget("task-1")


def test_runtime_cache_result_backend_mget_preserves_key_order() -> None:
    install_result_backend_integration()
    app = CeleryApp("result-mget", backend="vercel-runtime-cache://")
    backend = app.backend
    backend.set("first", "one")
    backend.set("second", "two")

    assert backend.mget(["second", "missing", "first"]) == ["two", None, "one"]
    assert backend.mget(["first", "second"]) == ["one", "two"]
    assert not hasattr(backend, "_mget_executor")
