# vercel-connect

Client for Vercel Connect: exchanges the deployment's OIDC token for short-lived third-party credentials, drives user authorization, and verifies forwarded webhooks.

Lives in [[src/vercel-connect]] and is exposed as [[src/vercel-connect/vercel/connect/__init__.py|vercel.connect]] (async) and [[src/vercel-connect/vercel/connect/sync.py|vercel.connect.sync]], which use identical names and arguments. Projects never store provider secrets; Connect owns the OAuth client, PKCE, refresh, and revocation.

## Layering

Like [[vercel-sandbox]], Connect keeps all behavior in one async-only service and adds sync and async runtimes as thin facades.

[[src/vercel-connect/vercel/connect/_internal/service.py#ConnectService]] mints, caches, revokes, and authorizes. [[src/vercel-connect/vercel/connect/_internal/service.py#get_connect_service]] builds it per [[runtime#SDK sessions|SDK session]] from [[src/vercel-connect/vercel/connect/_internal/options.py#ConnectServiceOptions]]. Per-call behavior, such as `force_refresh`, the validity buffer, or an explicit `vercel_token`, goes in [[src/vercel-connect/vercel/connect/_internal/options.py#ConnectOptions]]. Mixing an async call into a sync session, or the reverse, is rejected.

## Subjects and value types

A subject names whose authority a credential carries, and every value on the public surface is a frozen Pydantic model.

There are four subjects: the app installation, a named user who has consented, a user asserted with a JWT bearer, and a token exchange of a credential the caller already holds. They are typed values because three of them carry their own fields.

Models are frozen, so a subject cannot change after a credential has been cached against it. Containers of strings are stored as tuples, and a bare string is rejected rather than split into characters. Every validation failure raises `ConnectValidationError`, never a Pydantic error. Models live in [[src/vercel-connect/vercel/connect/_internal/models.py]].

## Token cache

Credentials are cached per request shape and caller identity, in a bounded LRU that de-duplicates concurrent cold fetches.

[[src/vercel-connect/vercel/connect/_internal/cache.py#TokenCache]] is deliberately not a port of the TypeScript cache, which has known defects. Its key ([[src/vercel-connect/vercel/connect/_internal/cache.py#build_cache_key]]) is order-insensitive, excludes the validity buffer, and includes the caller's identity. Invalidation is per key and reaches loads that are still in flight. Entries within the validity buffer of expiry are treated as misses.

The identity in the key comes from [[src/vercel-connect/vercel/connect/_internal/identity.py#IdentityResolver]], which reduces a *verified* OIDC token to a stable identity using [[vercel-oidc#Verification]]. Because the identity survives token refreshes, a refresh does not discard cached credentials. A token that fails verification falls back to its own digest, which costs an extra mint but can never be served another identity's credentials.

Concurrent misses for one key collapse into a single request. Threads block on a mutex ([[src/vercel-connect/vercel/connect/_internal/single_flight.py#SyncSingleFlight]]), while tasks await a future ([[src/vercel-connect/vercel/connect/_internal/single_flight.py#AsyncSingleFlight]]), because blocking the event loop while the holder awaits the network would deadlock.

## Authorization flows

States that need user action are distinct exceptions, so callers branch on the exception type instead of inspecting error codes.

`UserAuthorizationRequiredError` means the caller should start a redirect flow with `start_authorization`. Headless clients request a device code and poll `get_token` with `force_refresh`, which surfaces `AuthorizationPendingError` (also used for `slow_down`) until the user approves. `AuthorizationDeniedError` and `AuthorizationExpiredError` are terminal. Errors are defined in [[src/vercel-connect/vercel/connect/_internal/errors.py]].

## Webhook verification

Provider webhooks forwarded by Connect carry a Vercel OIDC token, so one verifier replaces per-provider signature schemes.

`verify_connect_webhook` applies [[vercel-oidc#Verification]] to the forwarded token. It accepts any valid token for the current project and environment and fails closed when those cannot be resolved; it is not pinned to a particular connector or deployment.
