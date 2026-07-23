"""Identity checks for transitional aggregate-owned core forwarders."""

from vercel._internal import (
    byte_stream as legacy_byte_stream,
    http as legacy_http,
    iter_coroutine as legacy_iter_coroutine,
    polyfills as legacy_polyfills,
    time as legacy_time,
    url as legacy_url,
)
from vercel._internal.http import (
    config as legacy_http_config,
    httpx as legacy_httpx,
    retry as legacy_http_retry,
    transport as legacy_http_transport,
)
from vercel.internal.core import (
    byte_stream,
    http,
    iter_coroutine,
    polyfills,
    time,
    url,
)
from vercel.internal.core.http import (
    config as http_config,
    httpx as core_httpx,
    retry as http_retry,
    transport as http_transport,
)


def test_legacy_forwarders_preserve_canonical_core_identity() -> None:
    assert legacy_byte_stream.SyncByteStreamRuntime is byte_stream.SyncByteStreamRuntime
    assert legacy_byte_stream.AsyncByteStreamRuntime is byte_stream.AsyncByteStreamRuntime
    assert legacy_iter_coroutine.iter_coroutine is iter_coroutine.iter_coroutine
    assert legacy_time.coerce_duration is time.coerce_duration
    assert legacy_url.format_url_path is url.format_url_path
    assert legacy_polyfills.StrEnum is polyfills.StrEnum
    assert legacy_polyfills.Self is polyfills.Self
    assert legacy_polyfills.UTC is polyfills.UTC
    assert legacy_http_transport.BaseTransport is http_transport.BaseTransport
    assert legacy_http_transport.SyncTransport is http_transport.SyncTransport
    assert legacy_http_transport.AsyncTransport is http_transport.AsyncTransport
    assert legacy_http_transport.TransportOptions is http_transport.TransportOptions
    assert legacy_http.SyncTransport is http.SyncTransport
    assert legacy_http.DEFAULT_TIMEOUT is http.DEFAULT_TIMEOUT
    assert legacy_http_config.DEFAULT_API_BASE_URL is http_config.DEFAULT_API_BASE_URL
    assert legacy_http_config.DEFAULT_TIMEOUT is http_config.DEFAULT_TIMEOUT
    assert legacy_httpx.create_base_client is core_httpx.create_base_client
    assert legacy_httpx.create_base_async_client is core_httpx.create_base_async_client
    assert legacy_http_retry.RetryPolicy is http_retry.RetryPolicy
    assert legacy_http_retry.SleepFn is http_retry.SleepFn
