# ADR: `vercel.unstable` SDK API Design

## Status

Proposed for team ratification.

## Context

The existing Python SDK exposes several package-level APIs and facades, with
many sync-first or static/classmethod-style entrypoints. New work around Vercel
Sandbox, persistent sandbox behavior, runtime OIDC credentials, and generated
management resources needs a more coherent API model.

The design target is an unstable namespace where maintainers can iterate
quickly without implying stable public support. The namespace should be strict
about types, explicit about runtime lifecycle, and grounded in concrete service
use cases.

The design session explored the API tree branch by branch. This record captures
the major forks, the options considered, and the decisions that emerged.

## Decision 1: Document Role And Audience

Decision: `src/vercel/unstable/README.md` is a living design charter plus RFC
outline for SDK maintainers.

Alternatives considered:

- high-level design charter only
- concrete RFC for the first API only
- user-facing external documentation

Rationale:

The document needs enough detail to guide implementation planning and team
ratification, but the API is not ready to be friendly to external users. It
should be explicit that `vercel.unstable` may change without migration support.

## Decision 2: Curated Facade With Domain Subpackages

Decision: `vercel.unstable` is a curated top-level facade. It exports only
top-of-funnel concepts such as `Session`, `SyncSession`, `SessionOptions`,
default-session helpers, and default-bound accessors.

Domain types stay in domain packages:

- `vercel.unstable.sandbox`
- `vercel.unstable.auth`
- `vercel.unstable.resources`
- `vercel.unstable.testing`

Alternatives considered:

- re-export every experimental service API from `vercel.unstable`
- make subpackages the only public import path
- expose everything available for convenience

Rationale:

The top-level namespace should communicate the new API model, not become a
grab bag. Domain-specific types such as `SandboxStatus`, `SandboxOptions`,
and credential providers should remain near their domain until multiple
services prove they need the exact same abstraction.

## Decision 3: Session Model

Decision: `Session` is the runtime and lifecycle container. It owns shared
transport pools, observability/logging primitives, close hooks, and lazy
initialization state. It does not own universal credential policy.

Alternatives considered:

- session as the primary auth/config container
- session as only a passive configuration object
- per-operation temporary sessions

Rationale:

The SDK needs shared lifecycle resources such as HTTP pooling, but credential
requirements vary by service boundary. Treating session as the owner of
runtime resources keeps lifecycle coherent while allowing accessors to own
their auth policy.

## Decision 4: Default Session Strategy

Decision: default facade access resolves a session by first checking a
context-local binding, then falling back to a process-global lazy default
session.

```python
async with unstable.use_session(session):
    await unstable.sandbox.create()
```

Alternatives considered:

- one process-global singleton only
- create and close a new default session per operation
- create a new default session per accessor clone
- context-local only with no global fallback

Rationale:

A per-operation session has clean lifecycle but breaks the domain object model:
returned `Sandbox` handles need a live runtime context for later actions such
as `refresh()`, commands, logs, and snapshots. A process-global singleton
preserves pooling but is awkward for tests and framework contexts. Contextvars
provide scoped override while retaining a simple global fallback.

`use_session(session)` binds but does not own or close the session. Ownership
stays explicit.

## Decision 5: Async-first Naming

Decision: async is the conceptual default. Unsuffixed names are async-capable
primary types. Sync mirrors use `Sync` names.

Examples:

- `Session`
- `SyncSession`
- `Sandbox`
- `SyncSandbox`

There is no default sync facade initially. Sync usage starts through
`SyncSession`.

Alternatives considered:

- `AsyncSession` / `Session`
- default `unstable.sync.sandbox` facade immediately
- only async in unstable

Rationale:

The new design should be async-first while still acknowledging sync usage.
Avoiding a sync default facade keeps the first surface smaller and leaves room
to add it later.

## Decision 6: Named Accessors, Not Factories

Decision: use named accessors such as `session.sandbox` and
`session.resources.projects`.

Alternatives considered:

- boto3-style `resource("sandbox")` / `client("sandbox")`
- `sandbox_service` style names
- `sandbox.instances.create(...)`

Rationale:

The design borrows boto3's session/default-session concept, but not its
stringly typed service registry. Vercel has far fewer service domains, and
named accessors improve strict typing, autocomplete, and API discovery.

The top-level Sandbox accessor is singular because the product name is Vercel
Sandbox. This rejects the otherwise attractive plural `sandboxes` collection
grammar. Child accessors use plural names such as `sandbox.snapshots`.

## Decision 7: Accessor Semantics

Decision: accessors are configured service or collection clients bound to a
session. They are objects with options and lazy state, not modules of functions.

`session.sandbox` returns the same default accessor instance each time.
`with_options(...)` always returns a clone, even with no arguments.

`with_session(...)` rebinds accessors to another session. Domain objects do not
support `with_session(...)` initially.

Alternatives considered:

- accessors as simple namespaces of functions
- accessors as generated client modules
- allow session rebinding through options
- allow domain objects to be rebound to another session

Rationale:

Accessors need service-specific options, credential policy, lazy clients, and
clear identity. Rebinding sessions through a separate method keeps runtime
ownership distinct from behavior settings.

## Decision 8: Options Vocabulary

Decision: use `Options`, not `Config`, as the shared vocabulary.

Options are user-supplied settings for sessions, services, resources, and
operations. They may include dependency objects such as credential providers,
but not runtime state such as initialized clients or opened pools.

Options classes should be frozen dataclasses.

Alternatives considered:

- `Config` for inert data and `Options` for call-site overrides
- constructor keyword sprawl instead of options objects
- put providers outside options entirely

Rationale:

The design initially tried to distinguish inert config from ergonomic options.
That became messy once credential providers were recognized as service-level
settings. `Options` covers both inert values and injected dependencies while
still excluding runtime state.

`with_options(...)` accepts either a typed options object or keyword overrides,
not both. Explicit unset sentinels handle inheritance; `None` is only a real
value where the field type allows it.

## Decision 9: Service-specific Auth Policy

Decision: credential provider configuration lives on service/resource options,
not directly on `Session`.

Runtime services such as Sandbox may use OIDC-first fallback to access-token
credentials. Management resources such as Projects require access-token
credentials.

Alternatives considered:

- session-wide credential provider
- session-owned credential provider registry
- direct `Session(credentials=...)` constructor values
- allow both direct credentials and provider objects

Rationale:

Credential requirements are boundary-specific. Putting credentials on session
would make it too easy to accidentally use an auth form in the wrong place.
Provider objects give one mental model and avoid precedence rules between
direct credentials and provider settings.

## Decision 10: Auth Package And Credential Types

Decision: auth-related types live under `vercel.unstable.auth`.

Credential types are nominal siblings:

- `OIDCCredentials`
- `AccessTokenCredentials`

They may share a public protocol under `auth`, but services should continue to
type their accepted credential unions explicitly.

Provider protocols are generic and use `resolve()`:

```python
class CredentialProvider[T_Credentials](Protocol):
    async def resolve(self) -> T_Credentials: ...
```

Alternatives considered:

- package named `credentials`
- provider method named `get_credentials()`
- provider as `__call__()`
- raw string or header mappings instead of structured credential objects

Rationale:

`auth` is broader than credentials and can contain OIDC helpers, token exchange,
and future auth utilities. Structured nominal credentials preserve service
constraints. `resolve()` better communicates layered lookup and token exchange.

Credential resolution happens once per authenticated request. It must not be
performed during accessor initialization, because Vercel Function OIDC
credentials can be request-local and fresh for each invocation.

## Decision 11: Request Layering

Decision: each service/resource accessor owns a credential-aware internal
`RequestClient`. Service methods assemble requests and parse responses, but the
`RequestClient` resolves credentials per authenticated request.

The `RequestClient` may also expose anonymous request paths for methods that do
not require auth.

Alternatives considered:

- service methods call the credential provider directly
- generated/internal clients handle arbitrary auth without accessor policy
- expose `RequestClient` publicly
- expose `Transport` publicly

Rationale:

Centralizing request-time credential resolution in `RequestClient` keeps service
methods focused on API semantics while preserving fresh credentials. Keeping
`RequestClient` and `Transport` internal avoids freezing low-level abstractions
too early.

## Decision 12: Service/Resource Split

Decision: distinguish runtime services from management resources.

Runtime services may support OIDC and embedded project/team scope. Management
resources represent REST resources and require access-token credentials.

Examples:

```python
await session.sandbox.create()
await session.resources.projects.get("prj_123")
```

Alternatives considered:

- separate top-level `sdk` package for management resources
- put all APIs directly under session
- let auth behavior be implicit and endpoint-driven only

Rationale:

The auth difference is central enough to be part of the API shape. A
`resources` namespace communicates that management REST resources are a
different boundary without prematurely creating a separate top-level package.

## Decision 13: Model Categories

Decision: use different model strategies for different surfaces.

- Resource inputs: `TypedDict` or keyword-shaped typed inputs.
- Resource outputs: Pydantic models, likely generated from OpenAPI.
- Service objects: custom behavioral classes with private typed state.

Alternatives considered:

- Pydantic everywhere
- dataclasses everywhere
- generated model classes everywhere
- direct response dictionaries

Rationale:

Resource APIs benefit from generated typed input/output shapes. Service objects
such as `Sandbox` need behavior, child accessors, state transitions, and
lifecycle methods, so they should not be plain Pydantic response models.

## Decision 14: Sandbox Object State

Decision: `Sandbox` objects are stable handles with explicit mutable cached
state.

Properties read cached state only and never perform hidden I/O. Methods such as
`refresh()` perform explicit I/O, update the object in place, and return `self`.

Alternatives considered:

- immutable snapshots where `refresh()` returns a new object
- live proxies that fetch on property access
- expose raw `.data` as the primary interface

Rationale:

The identity of a sandbox handle should feel stable while the remote sandbox
boots, stops, snapshots, or changes state. In-place refresh is the more
Pythonic fit for a behavioral resource handle, while prohibiting hidden I/O
keeps async behavior visible.

## Decision 15: Sandbox Waiting

Decision: lifecycle methods wait for their natural completion status by
default. Pass `wait=False` to return immediately after the API call. There
is no `WaitOptions` type; custom waiting uses `wait_for_status(...)`
directly.

Each lifecycle method has an implicit target status — for example, `create`
waits for `READY` and `stop` waits for `STOPPED`. Per-method targets are
defined alongside the implementation.

Methods accept an overall `timeout=` argument that bounds the entire call
(transport, retries, and any wait). Per-method default timeouts are sized
for the typical case and tuned with telemetry. Timeout is a per-call
argument for now; if telemetry shows shared blanket-policy needs,
equivalent fields can be added to `SandboxOptions` later.

```python
await sandbox.stop()                              # wait for STOPPED with default timeout
await sandbox.stop(timeout=timedelta(seconds=60)) # bound the whole call
await sandbox.stop(wait=False)                    # return after issuing the API call
```

`wait_for_status(...)` is the low-level primitive. It accepts one or more
statuses, understands terminal states, and raises typed sandbox exceptions
when a terminal non-target state is reached. Timeout is a separate error.

Alternatives considered:

- opt-in waiting with a `WAIT` sentinel and a `WaitOptions` object (the
  prior position)
- `wait=True` opt-in with a separate `WaitOptions` argument
- enum or string directives such as `"wait-for-stopped"`
- return operation/result objects from lifecycle mutations
- split timeouts for transport vs. wait phases on the call site
- make wait controls top-level cross-service concepts immediately

Rationale:

The 80% case is "wait until done." Making that the default removes a
required import (`WAIT`) and eliminates `WaitOptions` as a public type. A
plain `wait: bool = True` parameter is readable at the call site; callers
who need more control opt out with `wait=False` and use `wait_for_status`
directly, which is also the right primitive for waiting on multiple
acceptable statuses. A single overall method timeout matches how callers
think about the operation — "I'm willing to wait N seconds for `stop()` to
finish" — and lets the SDK manage the internal split between transport and
wait phases.

## Decision 16: Child Resources

Decision: child resources such as `sandbox.snapshots` are plural cached
accessors on the domain object.

They inherit from the sandbox object's bound context at creation/get time.

Alternatives considered:

- create child accessors fresh on each property access
- inherit from the latest parent service accessor
- expose child operations as top-level methods requiring sandbox IDs

Rationale:

Cached child accessors preserve identity and lazy state. Binding them to the
domain object's context makes behavior stable even if another accessor clone is
created later.

## Decision 17: Cancellation Contract

Decision: cancellation propagates without server-side rollback.

If a method is cancelled — by its own `timeout=` expiring or by an outer
`anyio` cancel scope — the SDK does not attempt to undo any state changes
that the API may have already committed. The cancelled method raises and
returns no handle.

Sandbox creation is a named operation. After a cancelled `create(name=...)`,
the sandbox may already exist server-side; a retry with the same name may
return a conflict. Callers requiring at-least-once semantics should
reconcile via `session.sandbox.get(name)` before retrying. A future
`create_or_get(...)` upsert flow is planned to absorb this case.

Alternatives considered:

- best-effort SDK-side rollback on cancellation
- silent retry with idempotency tokens

Rationale:

Rollback inside the SDK is the wrong layer; users have varying ideas of
what rollback means (delete, leave, mark for cleanup), and the SDK cannot
tell them apart. Keeping cancellation honest about what was committed lets
callers decide. A planned `create_or_get(...)` will smooth the most common
retry-after-cancel collision without changing this contract.

## Decision 18: Common Error Base

Decision: a single top-level `VercelError`, exported from `vercel.unstable`,
is the root of all SDK exceptions. Domain-specific errors such as
`SandboxError` and `AuthError` extend it directly. There is no intermediate
service/resource layer.

Alternatives considered:

- no common base; users catch `Exception`
- middle layers such as `RuntimeServiceError` and `ResourceError`

Rationale:

A single root makes "catch any SDK error" trivial. Middle layers can be
introduced later if shared semantics emerge across services; predeclaring
them forces every domain to commit to a category before there is evidence.

## Decision 19: Observability

Decision: the SDK uses widely available primitives for logs and traces.

Logging goes through the stdlib `logging` module under the namespace
`vercel.unstable.*`, with a logger per domain package. The SDK does not
install handlers; users configure logging through stdlib mechanisms.

Tracing uses OpenTelemetry. Spans are emitted lazily, only when
`opentelemetry-api` is importable in the user's environment. Span names and
attributes follow OTel semantic conventions where they exist.

Span attributes describe the operation (sandbox id, region, runtime, and
similar fields), not SDK plumbing. Session identity is intentionally not
part of span data: two sessions that differ only in pool size or lazy state
are indistinguishable in traces.

Alternatives considered:

- proprietary log format or custom handler
- always-on tracing regardless of OpenTelemetry availability
- session identity attached to every span

Rationale:

Stdlib `logging` is the universal Python contract; users already configure
it. Optional OpenTelemetry keeps the SDK lightweight when traces aren't
needed and integrates cleanly when they are. Keeping span attributes
operation-focused avoids leaking SDK internals into user telemetry.

## Consequences

Positive consequences:

- Stronger static typing than a generic factory model.
- Coherent lifecycle for returned domain objects.
- Fresh request-time auth for OIDC-heavy runtime services.
- Clear boundary between runtime services and management resources.
- Explicit space for generated REST resources without forcing Sandbox into a
  generated model shape.

Costs:

- More concepts than the current static/classmethod-heavy API.
- Context-local defaults and accessor clones require careful implementation.
- Options merging and unset semantics need strong tests.
- Service-specific auth policy means some duplication across accessors is
  expected initially.

## Open Issues

- Define exact default credential lookup order per service/resource.
- Define the exact fields for `SessionOptions`, `SandboxOptions`, and resource
  options.
- Decide how generated resources are organized under `resources`.
- Decide whether sync default-session context support is needed.
- Decide which concepts, if any, graduate from domain packages to the curated
  top-level facade after reuse.
- Decide whether `create_or_get(...)` (and analogous upserts for named
  resources) lands alongside the first sandbox release or as a follow-up.
