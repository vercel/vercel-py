# Changelog

## 0.10.0 - 2026-10-23

### Features

- Add async `Drive.fork(name=...)` and sync `SyncDrive.fork(name=...)` to copy committed Drive data with inherited region and maximum size, plus optional `parent_drive_id` and `root_drive_id` lineage fields. Use `fork_drive(source=..., name=...)` on the top-level APIs or standalone clients to fork directly without fetching the source. (#444)
- Support process standard input: `create_process(stdin=subprocess.PIPE)` exposes `process.stdin` as an `anyio.abc.ByteSendStream` (a writable `io.RawIOBase` in the sync API), `communicate(input)` writes and closes it, `run_process(input=...)` feeds a process it waits on, and processes from `get_process` and `query_processes` can write to the stdin of a process started elsewhere. User handles take the same options. Writes are delivered in order and retried without duplicating input, so stdio servers such as MCP servers can run inside a sandbox. (#449)

- Add an `on_start` callback to `run_process` on sandboxes and runtime sessions in both APIs, the counterpart of `runCommand({ onStart })` in the TypeScript SDK. It receives the `Process` or `SyncProcess` handle as soon as the command starts, before any output is delivered, while the same request keeps streaming output and waiting for the exit. The callback must be synchronous. If it raises, `run_process` raises the same exception without killing the process, and a started process is never resumed or run again. (#477)

  Process handles no longer lose their return code when a `send_signal`, `kill`, or `refresh` response that was in flight when the process exited arrives after its exit was recorded.

- `Sandbox.update()` now applies changes to the running session before it saves the configuration, matching the TypeScript SDK. A larger `execution_time_limit` extends the running session, and `network_policy` is applied to it. If applying the policy fails, the configuration and tags are not saved. `update()` never resumes a stopped sandbox. (#479)

  `Sandbox.update_network_policy()` and `SyncSandbox.update_network_policy()` are deprecated in favor of `update(network_policy=...)`. To change only a running session's policy without saving it, use `update_network_policy()` on the session.

- Add immutable Linux user handles with permission-enforced commands and reads, HTTP file uploads with user ownership, account provisioning and selection with recoverable duplicate-account errors, and shared synchronous and asynchronous execution and filesystem Protocols. (#438)

### Bug Fixes

- Fix process stdout and stderr readers stalling when both are read at the same time from separate tasks or threads: a line routed to one reader while another was pulling from the shared log stream could sit unread until more output arrived. (#450)
- Recover from network interruptions while streaming process output. `run_process` no longer fails or reruns the command when its connection drops after the process starts; it resumes the output from the logs endpoint and waits for the exit code. If the sandbox stops after the process starts, `run_process` raises `SandboxStreamError` with the lifecycle code instead of resuming the sandbox and running the command again. Process `stdout`/`stderr` readers reconnect without duplicating output, including when a stream ends partway through a record or the API reports `sandbox_stream_closed`, and process waits retry transient connection failures, rate limits, and server errors. (#447)

### Internal

- Give process stdin its own write locks, independent of the output readers. (#478)

## 0.9.1 - 2026-10-15

- Update dependencies.

## 0.9.0 - 2026-10-07

### Breaking Changes

- Drive operations now always use the project resolved from Sandbox credentials. Remove the `project_id` override from the API client and runtime drive functions; Drive deletion also uses credentials rather than the handle’s project metadata. (#439)

### Features

- Add `get_drive(name_or_id=...)` to look up existing Drives by name or ID without creating them, with async and sync support. Missing Drives raise `SandboxApiError` with status code 404. (#439)

## 0.8.0 - 2026-10-06

### Features

- Add network policy response rules so the Sandbox proxy can answer matching requests without contacting the destination. (#401)
- Add `open_interactive()` for interactive PTY sessions. Terminal I/O is a standard stream on the session: an `anyio.abc.ByteStream` asynchronously and an `io.RawIOBase` synchronously, alongside `resize()` and `wait()` for the remote exit code. (#415)
- Tag Sandbox API requests with the coding agent driving the Python process, when detected, as an `agent/<name>` User-Agent phrase. This covers normal requests, streams, and uploads. (#424)

### Bug Fixes

- Allow Drive mounts to be combined with failover regions when creating sandboxes, while retaining primary-region validation. (#417)
- Use a shared AnyIO dependency (>=4.11,<5) so application callbacks and SDK code share cancellation scopes and exception types. Include the WebSocket protocol dependency in the shared bundle. (#436)

## 0.7.0 - 2026-09-23

### Breaking Changes

- Remove operation-level ``project_id`` arguments. Sandbox operations now always
  use the project associated with the active credentials. (#409)
- ``get_or_create_drive`` now returns a ``(drive, created)`` tuple instead of a Drive. Drive creation also uses the configured Sandbox region. (#402)

### Features

- Add a ``delete_orphan_snapshots`` option when destroying a sandbox. (#400)
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
