from __future__ import annotations

from typing import Any, ClassVar

import pytest
from dramatiq.results.backend import Missing

import vercel.integrations.dramatiq._result_backend as vqs_dramatiq_backend
from vercel.integrations.dramatiq import VercelRuntimeCacheBackend

from .test_dramatiq_broker import dramatiq_message


class FakeRuntimeCache:
    instances: ClassVar[list[FakeRuntimeCache]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.values: dict[str, object] = {}
        self.set_options: dict[str, dict[str, object]] = {}
        self.get_error: Exception | None = None
        self.set_error: Exception | None = None
        FakeRuntimeCache.instances.append(self)

    def get(self, key: str) -> object | None:
        if self.get_error is not None:
            raise self.get_error
        return self.values.get(key)

    def set(self, key: str, value: object, options: dict[str, object]) -> None:
        if self.set_error is not None:
            raise self.set_error
        self.values[key] = value
        self.set_options[key] = options


@pytest.fixture(autouse=True)
def fake_runtime_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeRuntimeCache.instances.clear()
    monkeypatch.setattr(vqs_dramatiq_backend, "RuntimeCache", FakeRuntimeCache)


def cache() -> FakeRuntimeCache:
    return FakeRuntimeCache.instances[0]


def unwrap_cache_value(value: object) -> object:
    return VercelRuntimeCacheBackend().encoder.decode(vqs_dramatiq_backend._unwrap_value(value))


def test_runtime_cache_backend_round_trips_success_result() -> None:
    backend = VercelRuntimeCacheBackend(namespace="results")
    message = dramatiq_message()

    backend.store_result(message, {"answer": 42}, ttl=30000)

    assert backend.get_result(message) == {"answer": 42}
    stored_key = backend.build_message_key(message)
    assert unwrap_cache_value(cache().values[stored_key]) == {"answer": 42}
    assert cache().set_options[stored_key] == {"name": stored_key, "ttl": 30}


def test_runtime_cache_backend_round_trips_exception_result() -> None:
    backend = VercelRuntimeCacheBackend()
    message = dramatiq_message()

    backend.store_exception(message, ValueError("bad value"), ttl=10000)

    with pytest.raises(Exception, match="bad value"):
        backend.get_result(message)


def test_runtime_cache_backend_returns_missing_for_absent_cache_entry() -> None:
    backend = VercelRuntimeCacheBackend()

    assert backend._get("missing") is Missing


def test_runtime_cache_backend_options_control_cache_namespace_and_set_options() -> None:
    def key_hash(key: str) -> str:
        return f"hashed:{key}"

    backend = VercelRuntimeCacheBackend(
        namespace="dramatiq.app",
        runtime_cache_namespace="runtime-results",
        namespace_separator=":",
        key_hash_function=key_hash,
        name="result-name",
        tags=["dramatiq", "results"],
    )

    backend.store_result(dramatiq_message(), "ok", ttl=1500)

    assert cache().kwargs == {
        "key_hash_function": key_hash,
        "namespace": "runtime-results",
        "namespace_separator": ":",
        "strict": True,
    }
    assert list(cache().set_options.values()) == [
        {"name": "result-name", "ttl": 1, "tags": ["dramatiq", "results"]}
    ]
    assert backend.namespace == "dramatiq_Dapp"


def test_runtime_cache_backend_omits_non_positive_ttl() -> None:
    backend = VercelRuntimeCacheBackend()

    backend.store_result(dramatiq_message(), "ok", ttl=0)

    assert list(cache().set_options.values()) == [{"name": next(iter(cache().values))}]


def test_runtime_cache_backend_rejects_malformed_payload() -> None:
    backend = VercelRuntimeCacheBackend()

    cache().values["bad"] = "not-wrapper"

    with pytest.raises(TypeError, match="wrapper object"):
        backend._get("bad")


def test_runtime_cache_backend_propagates_runtime_cache_errors() -> None:
    backend = VercelRuntimeCacheBackend()
    cache().get_error = RuntimeError("cache unavailable")

    with pytest.raises(RuntimeError, match="cache unavailable"):
        backend._get("key")

    cache().get_error = None
    cache().set_error = RuntimeError("cache unavailable")
    with pytest.raises(RuntimeError, match="cache unavailable"):
        backend.store_result(dramatiq_message(), "ok", ttl=1000)
