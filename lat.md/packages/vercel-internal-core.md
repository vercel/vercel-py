# vercel-internal-core

Shared runtime for Vercel service packages, installed only as their dependency: sessions, HTTP transports, sync/async bridging, and errors.

Lives in [[src/vercel-internal-core]]. Most of it is under `vercel._internal.core`. It also contributes the public [[src/vercel-internal-core/vercel/api/__init__.py|vercel.api]] (the session context) and [[src/vercel-internal-core/vercel/errors/__init__.py|vercel.errors]] (shared exception types) namespace portions.

## Versioning contract

Dependents pin this package to a single minor series because its internal API carries no compatibility promise.

Every consumer declares a dependency like `vercel-internal-core>=0.2.0,<0.3.0`. A breaking internal change therefore ships as a new minor version, and each dependent opts in by widening its bound in the same release.

## Sessions and service options

Sessions own pooled HTTP clients and per-service configuration, scoped through a context variable; the cross-package model is in [[runtime#SDK sessions]].

[[src/vercel-internal-core/vercel/_internal/core/session.py#SdkSession]] keeps one `httpx2.AsyncClient` per event loop, because the process-wide default session can be reached from several threads that each run their own loop. Services cache their per-session objects with `get_or_create_service`, so a service's API client is built once per session.

[[src/vercel-internal-core/vercel/_internal/core/options.py#ServiceOptions]] subclasses key themselves by logical service via `service_options_key`. That lets a sync and an async options class configure the same service, and lets [[src/vercel-internal-core/vercel/_internal/core/options.py#merge_service_options]] replace a parent's options for one service without touching the others.

## HTTP transports

Transports are the only place HTTP requests are built and sent, so service code never touches `httpx2` clients directly.

[[src/vercel-internal-core/vercel/_internal/core/http/transport.py#BaseTransport]] exposes one async-shaped API: plain `send`, plus scoped request and response streaming. [[src/vercel-internal-core/vercel/_internal/core/http/transport.py#SyncTransport]] implements it with blocking I/O and never suspends, which is what makes [[runtime#Sync and async parity]] work. Request bodies are explicit types (`JSONBody`, `BytesBody`, `RawBody`), and `ReadResponsePolicy` controls when a streamed response body is read eagerly.

[[src/vercel-internal-core/vercel/_internal/core/http/retry.py#RetryPolicy]] describes retry behavior. [[src/vercel-internal-core/vercel/_internal/core/http/transport.py#extract_structured_error]] normalizes Vercel API error bodies for the services that wrap them.

### Legacy httpx compatibility

HTTPX2 is the only HTTP family the SDK installs, but a session factory may return a legacy `httpx` client as a runtime-only migration path.

[[src/vercel-internal-core/vercel/_internal/core/http/_compat.py]] recognizes legacy clients and exception types only when `httpx` is already imported, so the SDK never imports it on its own. Static types stay HTTPX2-based, and legacy transport objects cannot be mounted inside an `httpx2` client.

## Sync/async helpers

Small utilities let one async-shaped implementation serve both public API modes.

- [[src/vercel-internal-core/vercel/_internal/core/iter_coroutine.py#iter_coroutine]] drives a non-suspending coroutine to completion and raises if it ever suspends.
- [[src/vercel-internal-core/vercel/_internal/core/byte_stream.py]] adapts sync and async byte sources to one `ReadableByteStream` protocol. It also provides a `StagingFileRuntime` for spooling uploads, with sync and async runtimes supplied by the session.
- [[src/vercel-internal-core/vercel/_internal/core/time.py#coerce_duration]] and its siblings normalize numeric and `timedelta` durations.

## Agent detection

The SDK reports which coding agent, if any, is driving the process, mirroring `@vercel/detect-agent`.

[[src/vercel-internal-core/vercel/_internal/core/detect_agent.py#detect_agent_name]] applies the same environment-variable rules as the JavaScript package; its filesystem-based Devin check is deliberately omitted. [[vercel-sandbox]] adds the result to its user agent unless `VERCEL_TELEMETRY_DISABLED` or `VERCEL_SANDBOX_TELEMETRY_DISABLED` is set.
