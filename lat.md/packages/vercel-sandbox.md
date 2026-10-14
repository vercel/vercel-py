# vercel-sandbox

Client for Vercel Sandbox: create, fork, and restore sandboxes; run processes; move files; mount drives; and attach interactive PTYs.

Lives in [[src/vercel-sandbox]] and is exposed as [[src/vercel-sandbox/vercel/sandbox/__init__.py|vercel.sandbox]] (async) and [[src/vercel-sandbox/vercel/sandbox/sync/__init__.py|vercel.sandbox.sync]]. It is the most complete user of [[runtime#SDK sessions]] and [[runtime#Sync and async parity]].

## Layering

All behavior lives in one async-only service that returns plain state, and thin sync and async handle layers wrap that state for users.

- [[src/vercel-sandbox/vercel/sandbox/_internal/api_client.py#SandboxApiClient]] speaks the Sandbox HTTP API over the session's transport.
- [[src/vercel-sandbox/vercel/sandbox/_internal/service.py#SandboxService]] orchestrates operations and returns neutral dataclasses from [[src/vercel-sandbox/vercel/sandbox/_internal/state.py]].
- [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#Sandbox]] and [[src/vercel-sandbox/vercel/sandbox/_internal/sync_runtime.py#SyncSandbox]] wrap that state in user-facing handles. The sync handles drive the same service coroutines with `iter_coroutine` over a sync transport.
- Logic that does not depend on the I/O mode, such as validation and handle bookkeeping, lives in [[src/vercel-sandbox/vercel/sandbox/_internal/runtime_common.py]].

New features should follow this split: put the logic in the service, then expose it from both runtimes with matching names and arguments.

## Configuration and clients

Sandbox configuration comes from the active SDK session, or from a standalone client that never touches the session.

[[src/vercel-sandbox/vercel/sandbox/_internal/service.py#get_sandbox_service]] builds one service per session from [[src/vercel-sandbox/vercel/sandbox/_internal/options.py#SandboxServiceOptions]]: base URL, credentials factory, file-transfer timeout, and default region (falling back to `VERCEL_REGION`). The sync and async options classes share one registry key. Supplying options replaces the whole Sandbox configuration for that session scope. Default credentials come from [[vercel-oidc#Credentials]].

[[src/vercel-sandbox/vercel/sandbox/client.py#SandboxClient]] and [[src/vercel-sandbox/vercel/sandbox/sync/client.py#SyncSandboxClient]] serve dependency-injected applications. Construction performs no I/O, and the owner must close the client.

## Lifecycle and sessions

A named sandbox outlives the VM sessions that run it, and sandbox-level operations resume a stopped sandbox transparently.

Operations through a `Sandbox` handle that fail because the sandbox is stopped, stopping, or snapshotting are classified by [[src/vercel-sandbox/vercel/sandbox/_internal/recovery.py#classify_sandbox_lifecycle_error]]. [[src/vercel-sandbox/vercel/sandbox/_internal/recovery.py#execute_with_sandbox_recovery]] then waits for one shared resume and replays the operation exactly once against the new session. Concurrent callers share that resume, and a failed replay is never retried again. Only direct API errors take part; stream failures and arbitrary exception chains do not.

`box.session()` pins operations to one session identity, which never auto-resumes; its managed form stops exactly the session it yielded. Exiting a managed sandbox or session does not wait for concurrent operations.

Sandboxes can be created from a runtime, image, Git repository, tarball, or snapshot. Forking copies a named sandbox's configuration and current snapshot, but never its drive mounts.

## Processes and filesystem

Processes and file transfers stream with bounded memory in both I/O modes.

Process output is routed by [[src/vercel-sandbox/vercel/sandbox/_internal/process_output.py#ProcessOutputRouter]] and read through [[src/vercel-sandbox/vercel/sandbox/_internal/text_reader.py]]. File reads and writes share state machines in [[src/vercel-sandbox/vercel/sandbox/_internal/filesystem_handle_core.py]], with sync and async facades on top. Bulk transfers are encoded as tar+gzip on the fly by [[src/vercel-sandbox/vercel/sandbox/_internal/streaming_archive.py#ArchiveRequestWriter]] rather than being built in memory.

## Drives

Drives are named persistent filesystems scoped to one project and region and mounted into sandboxes at creation.

A mount is read-write by default, or a point-in-time read-only snapshot. Mount paths are canonicalized and validated in [[src/vercel-sandbox/vercel/sandbox/_internal/models.py]]: absolute, non-overlapping, no `..`, never `/`. Each drive must be in the sandbox's region, and mounted sandboxes cannot use failover regions. A read-write drive attached to another running sandbox is rejected by the API, and a drive must be unmounted before it can be deleted.

## Interactive PTY sessions

PTY sessions run over a WebSocket and expose terminal I/O as the native byte stream of each mode.

[[src/vercel-sandbox/vercel/sandbox/_internal/interactive_session.py]] implements the framing and transport. The async session's stream is an AnyIO `ByteStream` ([[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#InteractiveStream]]); the sync one is an `io.RawIOBase` ([[src/vercel-sandbox/vercel/sandbox/_internal/sync_runtime.py#SyncInteractiveStream]]).

A connection that ends before the process reports an exit code raises rather than looking like a clean end of stream. Output that arrives while waiting is buffered up to a bound, after which `wait()` blocks until the stream is read.

## Console scripts

The package installs `vercel-sandbox` and `sandbox` commands that delegate to the JavaScript CLI.

Both run `npx sandbox` with the original arguments ([[src/vercel-sandbox/vercel/sandbox/__main__.py]]), so they need Node.js; the Python API itself does not.
