# Changelog

## 0.7.0 - 2026-09-23

### Breaking Changes

- Remove operation-level ``project_id`` arguments. Sandbox operations now always
  use the project associated with the active credentials. (#409)

### Features

- Add a ``delete_orphan_snapshots`` option when destroying a sandbox. (#400)
- Return a ``(drive, created)`` tuple from ``get_or_create_drive`` and use the configured Sandbox region when creating Drives. (#402)
- Add standalone `SandboxClient` and `SyncSandboxClient` factories for explicitly configured, injectable service clients. (#404)
- Support Secure Compute networks when creating, forking, and updating sandboxes with `network_id`. Expose the attached network ID on sandbox handles and allow `update(network_id=None)` to disconnect a sandbox. (#411)

### Bug Fixes

- When an auto-resuming operation (or explicit session acquisition) encounters a stopping or snapshotting sandbox, we now directly resume the sandbox instead of polling. (#412)

## 0.6.0 - 2026-09-17

### Features

- Add Sandbox Drive creation, listing, deletion, and mount support to the synchronous and asynchronous APIs. (#397)

## 0.5.2 - 2026-09-14

> **Release note:** Supersedes repository-declared versions `0.5.1` and `0.5.0`,
> which were not published to PyPI. The complete changes since published `0.4.0`
> are included below.

### Features

- Add sync and async `fork_sandbox(...)` support for creating a sandbox from an
  existing named sandbox with optional configuration overrides. (#257)

- Add `region` and `failover_regions` configuration for sandbox creation, forks,
  and updates, plus multi-region snapshot availability reporting. (#308)

- Forward private ``__``-prefixed parameters to the Sandbox API. (#350)

- Replace the unmaintained `httpx` dependency with its maintained `httpx2` successor while retaining runtime-only support for explicitly installed legacy clients returned by the session factory. (#356)

### Bug Fixes

- Allow Sandbox process waits and log streams to remain idle longer than the session HTTP timeout. (#307)
- Expose Linux process signals consistently on every SDK host platform. (#352)

- Read `VERCEL_REGION` directly so Sandbox does not depend on the unavailable `vercel-env` distribution. (#388)

### Internal

- Require `vercel-internal-core>=0.2.0,<0.3.0` for the coordinated `httpx2` release.

## 0.5.1 - 2026-09-09

> **Release note:** Version 0.5.0 was declared in repository history but was
> never published to PyPI. This forward release includes its intended contents.

### Features

- Replace the unmaintained `httpx` dependency with its maintained `httpx2` successor while retaining runtime-only support for explicitly installed legacy clients returned by the session factory. (#356)

## 0.5.0 - 2026-09-01

### Features

- Add sync and async `fork_sandbox(...)` support for creating a sandbox from an
  existing named sandbox with optional configuration overrides. (#257)

- Add `region` and `failover_regions` configuration for sandbox creation, forks,
  and updates, plus multi-region snapshot availability reporting. (#308)

- Forward private ``__``-prefixed parameters to the Sandbox API. (#350)

### Bug Fixes

- Allow Sandbox process waits and log streams to remain idle longer than the session HTTP timeout. (#307)
- Expose Linux process signals consistently on every SDK host platform. (#352)

## 0.4.0 - 2026-08-07

### Breaking Changes

- Require synchronous credential factories when configuring `vercel.sandbox.sync`; use asynchronous factories only with the async Sandbox API. (#242)
- Replace the legacy `runtime` selector with `image` when creating sandboxes. Sandbox creation now uses API v3, defaults to `vercel/sandbox/universal:latest`, and supports custom Vercel Container Registry images. (#252)

### Internal

- Run Sandbox examples through the package-owned workspace Poe task. (#234)

## 0.3.0 - 2026-07-31

### Features

- Add `get_or_create_sandbox` to retrieve a named sandbox or create it when it does not exist, for both async and synchronous APIs. (#220)

## 0.2.0 - 2026-07-22

### Features

- Promote the Sandbox SDK as a standalone distribution at `vercel.sandbox`.
