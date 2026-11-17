# Runtime

Conventions shared by the service packages for HTTP, sync/async parity, configuration scope, and credentials.

Most of this lives in [[vercel-internal-core]], with request-scoped state in [[vercel-headers]] and token handling in [[vercel-oidc]].

## Sync and async parity

Clients are offered in sync and async form, and both share a single async-shaped implementation instead of duplicating business logic.

Shared logic is written as `async` methods on a base client that talks to a [[src/vercel-internal-core/vercel/_internal/core/http/transport.py#BaseTransport]]. The async API awaits it over an [[src/vercel-internal-core/vercel/_internal/core/http/transport.py#AsyncTransport]]. The sync API runs the same coroutine over a [[src/vercel-internal-core/vercel/_internal/core/http/transport.py#SyncTransport]], whose methods perform blocking I/O and never suspend, and drives it to completion with [[src/vercel-internal-core/vercel/_internal/core/iter_coroutine.py#iter_coroutine]].

`iter_coroutine` raises if the coroutine tries to suspend, so sync code paths must not await real async work such as `anyio.sleep` or user callbacks that may be async. The full migration recipe is the [[.agents/skills/iter-coroutine/SKILL.md|iter-coroutine skill]].

The [[vercel]] REST clients, [[vercel-blob]], [[vercel-sandbox]], and [[vercel-connect]] use the shared transports and sessions, and their feature modules never construct HTTP clients directly. [[vercel-queue]] follows the same base-client pattern over its own HTTP runtime and its own copy of `iter_coroutine`. [[vercel-cache]], [[vercel-oidc]], and [[vercel-internal-telemetry]] call HTTPX2 directly.

## SDK sessions

An SDK session scopes service configuration and pooled HTTP clients, and is bound to the current context with `vercel.api.session(...)`.

[[src/vercel-internal-core/vercel/_internal/core/session.py#session]] returns a context manager usable with `with` (binding a [[src/vercel-internal-core/vercel/_internal/core/session.py#SyncSdkSession]]) or `async with` (binding a [[src/vercel-internal-core/vercel/_internal/core/session.py#SdkSession]]). Sessions are held in a context variable, nest by merging their options over the parent's, and close their clients on exit. Without an explicit session, services use a process-wide default. Sessions apply only to packages built on the shared transports (see [[runtime#Sync and async parity]]).

A session's mode must match the API being called: sync APIs raise inside an async session and vice versa, which keeps blocking and async clients from being mixed.

Each service contributes its own [[src/vercel-internal-core/vercel/_internal/core/options.py#ServiceOptions]] subclass; a session accepts at most one options object per logical service. A session can also take an `httpx_client_factory`, which is the supported way to plug in a custom or legacy `httpx` client.

## Service packages

A new service package follows [[vercel-sandbox]], then [[vercel-blob]]. [[vercel-connect]] predates the conventions below and is not a template for them.

Each service keeps its behavior in one async-only service class, wrapped by thin sync and async runtimes with matching names and arguments. The sections below cover the layers that new packages most often get wrong.

### API client and wire models

The API client owns the wire format through private, typed request and response models, as [[src/vercel-sandbox/vercel/sandbox/_internal/api_client.py#_CreateSandboxRequest]] does.

API client methods take typed keyword arguments in the service's vocabulary, build a request model, and serialize it. No layer passes an untyped `dict` body. Rich values such as `datetime`, `timedelta`, and model types stay rich up to the request model. Only its serializers convert them to wire names and units, such as camelCase keys, milliseconds, or a formatted string. When the API changes, tighten these models first, then adapt the layers above.

The service validates arguments and makes the decisions a request depends on. It does not build bodies.

### Credentials and service options

Each service has an async and a sync options class that share one registry key, and each takes a credentials factory for its own mode.

[[src/vercel-sandbox/vercel/sandbox/_internal/options.py#_SandboxServiceOptionsKey]] shows the shared key. The async class takes an async factory, and the sync class takes a plain function, which may block on a file or the network. The sync module exports its class under the async name, as `vercel.sandbox.sync` does. Do not run an async factory in a sync session.

The factory is the only credential source. Do not add a separate `token` field: a fixed token is a factory that returns it.

### Listing and validation

Paging iterators are named `query_<resource>`, with a `query_<resource>_page` method underneath, as in [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#query_snapshots]] and [[src/vercel-blob/vercel/blob/__init__.py#query_blobs]].

Validate on the client only what can be decided without the server, and fail before the request. Use one helper for each kind of check, such as a non-empty string. Do not check a value passed through to the API against host data, such as the local timezone database, which may reject a value the server accepts.

## Request headers

Function helpers read the incoming Vercel request headers from one context-local store, registered once per request.

[[src/vercel-headers/vercel/headers/__init__.py#set_headers]] stores the headers in a context variable and [[src/vercel-headers/vercel/headers/__init__.py#get_headers]] reads them back. OIDC, cache, [[vercel-passport]], and geolocation helpers such as [[src/vercel-headers/vercel/headers/__init__.py#geolocation]] all consume this store, so frameworks only need to register headers in one place.

## OIDC credentials

Services authenticate with a Vercel OIDC token when one is available and fall back to an explicit access token otherwise.

[[src/vercel-oidc/vercel/oidc/token.py#get_vercel_oidc_token]] resolves a token in this order:

1. The `x-vercel-oidc-token` request header from [[runtime#Request headers]].
2. The `VERCEL_OIDC_TOKEN` environment variable.
3. A token file named by `VERCEL_OIDC_TOKEN_FILE`, re-read shortly before it expires.
4. In local development with a linked `.vercel` project, a token refreshed through the Vercel CLI login and cached on disk.

[[src/vercel-oidc/vercel/oidc/credentials.py#get_credentials]] combines that token with project and team IDs, taken from the environment or decoded from the token's claims. Without an OIDC token it requires `VERCEL_TOKEN`, `VERCEL_PROJECT_ID`, and `VERCEL_TEAM_ID`.

Token verification for inbound requests (`verify_vercel_oidc_token`) needs the `verify` extra and fails closed: when the expected project or environment cannot be determined, every token is rejected.
