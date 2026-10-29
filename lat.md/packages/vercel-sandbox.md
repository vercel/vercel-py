# vercel-sandbox

Client for Vercel Sandbox: create, fork, and restore sandboxes; run processes; move files; mount drives; and attach interactive PTYs.

Lives in [[src/vercel-sandbox]] and is exposed as [[src/vercel-sandbox/vercel/sandbox/__init__.py|vercel.sandbox]] (async) and [[src/vercel-sandbox/vercel/sandbox/sync/__init__.py|vercel.sandbox.sync]]. It is the most complete user of [[runtime#SDK sessions]] and [[runtime#Sync and async parity]].

## Layering

All behavior lives in one async-only service that returns plain state, and thin sync and async handle layers wrap that state for users.

- [[src/vercel-sandbox/vercel/sandbox/_internal/api_client.py#SandboxApiClient]] speaks the Sandbox HTTP API over the session's transport.
- [[src/vercel-sandbox/vercel/sandbox/_internal/service.py#SandboxService]] orchestrates operations and returns neutral state records from [[src/vercel-sandbox/vercel/sandbox/_internal/state.py]].
- [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#Sandbox]] and [[src/vercel-sandbox/vercel/sandbox/_internal/sync_runtime.py#SyncSandbox]] wrap that state in user-facing handles. The sync handles drive the same service coroutines with `iter_coroutine` over a sync transport.
- Logic that does not depend on the I/O mode, such as validation and handle bookkeeping, lives in [[src/vercel-sandbox/vercel/sandbox/_internal/runtime_common.py]].

New features should follow this split: put the logic in the service, then expose it from both runtimes with matching names and arguments.

## Data models

Sandbox data objects use Pydantic v2 throughout. Public value fields remain frozen; internal records preserve the mutability required by their runtime roles.

[[src/vercel-sandbox/vercel/sandbox/_internal/value_model.py#SandboxValueModel]] supplies the shared frozen configuration for public values and internal records. These include network policies, process results, filesystem entries, mounts, snapshot expiration, credentials, and service options. [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SandboxRouteState|Routes]] and [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SnapshotRetentionState|snapshot-retention state]] also reach callers through sandbox properties and use the same model configuration.

[[src/vercel-sandbox/vercel/sandbox/_internal/models.py#NetworkPolicy]] validates nested model dictionaries while preserving immutable, defensively copied mappings. Model dumps represent Python fields; the API client retains responsibility for wire names, policy shapes, and duration units. Convenience constructors for matchers, snapshot expiration, mounts, and credentials preserve positional calls. Other model construction uses keyword arguments, and invalid input or frozen-field assignment raises Pydantic validation errors.

[[src/vercel-sandbox/vercel/sandbox/user.py#SandboxUser]] and [[src/vercel-sandbox/vercel/sandbox/sync/user.py#SyncSandboxUser]] are ordinary runtime handles with private context and filesystem bindings and read-only public properties. Like other live handles, they do not expose model validation or serialization. Service options retain callable credential factories and the shared session registry key. [[src/vercel-sandbox/tests/test_public_models.py|Public model tests]] cover nested policy validation and serialization, Drive mount name serialization, and snapshot-retention roundtrips with API duration conversion. [[src/vercel-sandbox/tests/test_sandbox_users.py#test_selection_is_lazy_and_handle_properties_are_read_only|User handle tests]] cover lazy identity resolution, read-only properties, and structural capabilities.

### Internal data models

The internal dataclass migration covers 31 frozen records and three mutable coordination helpers. Behavioral handles, clients, transports, and services remain plain classes.

[[src/vercel-sandbox/vercel/sandbox/_internal/value_model.py#SandboxModel]] allows arbitrary runtime types and rejects extra fields; its frozen subclass supplies the value-record configuration. Structural stream fields bypass instance validation because their protocols are not runtime-checkable. Serialization of runtime references does not reconstruct live resources.

- [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#ProcessState|ProcessState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#CompletedProcessState|CompletedProcessState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#InteractiveSessionState|InteractiveSessionState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SandboxRuntimeSessionState|SandboxRuntimeSessionState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SandboxState|SandboxState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#RuntimeSessionStopState|RuntimeSessionStopState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SnapshotState|SnapshotState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#DriveState|DriveState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SandboxesPageState|SandboxesPageState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#RuntimeSessionsPageState|RuntimeSessionsPageState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SnapshotsPageState|SnapshotsPageState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#DrivesPageState|DrivesPageState]], [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#SnapshotSessionState|SnapshotSessionState]]: Frozen nested service results. Sparse attachment flags are ordinary model fields, and trusted handle updates use shallow `model_copy(update=...)` to preserve session identity.
- [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QuerySandboxesPage|QuerySandboxesPage]], [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QuerySessionsPage|QuerySessionsPage]], [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QuerySnapshotsPage|QuerySnapshotsPage]], [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QueryDrivesPage|QueryDrivesPage]]: Frozen generic pages retain live handles by reference; iterators still yield individual handles lazily.
- [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QuerySandboxesParams|QuerySandboxesParams]], [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QuerySessionsParams|QuerySessionsParams]], [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QuerySnapshotsParams|QuerySnapshotsParams]], [[src/vercel-sandbox/vercel/sandbox/_internal/pagination.py#QueryDrivesParams|QueryDrivesParams]]: Frozen parameters validate page sizes on construction and `model_validate`; `with_cursor` preserves the size.
- [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#_CreateSandboxParams|_CreateSandboxParams]], [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#_ForkSandboxParams|_ForkSandboxParams]], [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#_ResumeSandboxParams|_ResumeSandboxParams]]: Frozen deferred inputs validate on operation construction; private-parameter factories preserve the immutable empty default without copying it. Create and fork retain instance-only network-policy checks and live Drive references in mounts.
- [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#_AsyncRecoveryAttempt|_AsyncRecoveryAttempt]]: Mutable shared outcome retains the same AnyIO event and exception.
- [[src/vercel-sandbox/vercel/sandbox/_internal/sync_runtime.py#_SyncRecoveryAttempt|_SyncRecoveryAttempt]]: Mutable shared outcome retains exception identity across condition-variable waiters.
- [[src/vercel-sandbox/vercel/sandbox/_internal/api_client.py#_SandboxUserAgent|_SandboxUserAgent]], [[src/vercel-sandbox/vercel/sandbox/_internal/api_client.py#_PreparedSandboxCreation|_PreparedSandboxCreation]]: Frozen header metadata and prepared requests retain header formatting, credentials, and encoded request bytes.
- [[src/vercel-sandbox/vercel/sandbox/_internal/service.py#_SandboxQueryCriteria|_SandboxQueryCriteria]], [[src/vercel-sandbox/vercel/sandbox/_internal/service.py#_DriveQueryCriteria|_DriveQueryCriteria]]: Frozen normalized query criteria.
- [[src/vercel-sandbox/vercel/sandbox/_internal/models.py#_WriteFile|_WriteFile]]: Frozen staged upload metadata preserves binary contents.
- [[src/vercel-sandbox/vercel/sandbox/_internal/runtime_common.py#_UploadFileEntry|_UploadFileEntry]]: Frozen generic upload metadata preserves structural stream objects through `SkipValidation` and shallow copies.
- [[src/vercel-sandbox/vercel/sandbox/_internal/interactive_session.py#Frame|Frame]]: Frozen binary terminal events retain the shared closed-stream sentinel.
- [[src/vercel-sandbox/vercel/sandbox/_internal/process_output.py#_OutputTarget|_OutputTarget]]: Mutable output state preserves writable stream objects through `SkipValidation`; merged stdout/stderr share one target.

Five internal state types can reach callers through [[src/vercel-sandbox/vercel/sandbox/_internal/errors.py#SandboxResponseError]] data: process, snapshot, Drive, runtime-session, and sandbox state. They now support model validation and serialization. API diagnostic `raw` dictionaries use JSON-mode dumps so tuple-backed response fields become valid JSON lists.

Existing lifecycle, pagination, streaming, process-output, and user-context tests cover the internal refactor through runtime behavior. [[src/vercel-sandbox/tests/test_sandbox_public_flow.py#test_mutating_handles_reject_mismatched_response_identity|Mutation error tests]] also verify serialization of diagnostic state attached to a public response error.

[[src/vercel-sandbox/tests/test_sandbox_public_flow.py#test_deferred_sandbox_inputs_validate_on_construction|Deferred-input tests]] verify invalid create/fork names fail before awaiting or making a request. [[src/vercel-sandbox/tests/test_sandbox_public_flow.py#test_create_and_fork_reject_non_network_policy|Policy input tests]] preserve rejection of policy dictionaries, strings, and readback objects on both APIs.

## Configuration and clients

Sandbox configuration comes from the active SDK session, or from a standalone client that never touches the session.

[[src/vercel-sandbox/vercel/sandbox/_internal/service.py#get_sandbox_service]] builds one service per session from [[src/vercel-sandbox/vercel/sandbox/_internal/options.py#SandboxServiceOptions]]: base URL, credentials factory, file-transfer timeout, and default region (falling back to `VERCEL_REGION`). The sync and async options classes share one registry key. Supplying options replaces the whole Sandbox configuration for that session scope. Default credentials come from [[vercel-oidc#Credentials]].

[[src/vercel-sandbox/vercel/sandbox/client.py#SandboxClient]] and [[src/vercel-sandbox/vercel/sandbox/sync/client.py#SyncSandboxClient]] serve dependency-injected applications. Construction performs no I/O, and the owner must close the client.

## Lifecycle and sessions

A named sandbox outlives the VM sessions that run it, and sandbox-level operations resume a stopped sandbox transparently.

Operations through a `Sandbox` handle that fail because the sandbox is stopped, stopping, or snapshotting are classified by [[src/vercel-sandbox/vercel/sandbox/_internal/recovery.py#classify_sandbox_lifecycle_error]]. [[src/vercel-sandbox/vercel/sandbox/_internal/recovery.py#execute_with_sandbox_recovery]] then waits for one shared resume and replays the operation exactly once against the new session. Concurrent callers share that resume, and a failed replay is never retried again. Only direct API errors take part; stream failures and arbitrary exception chains do not.

`box.session()` pins operations to one session identity, which never auto-resumes; its managed form stops exactly the session it yielded. Exiting a managed sandbox or session does not wait for concurrent operations.

Sandboxes can be created from a runtime, image, Git repository, tarball, or snapshot. Forking copies a named sandbox's configuration and current snapshot, but never its drive mounts.

## Sandbox updates

`Sandbox.update()` changes the live current session before saving sandbox configuration, and does not resume a stopped session.

Update requests send only caller-supplied configuration. The default region from service options or `VERCEL_REGION` applies to creation and forking; updates include a region only when the caller supplies one. [[src/vercel-sandbox/tests/test_sandbox_public_flow.py#test_update_sends_only_explicit_configuration|Public API tests]] cover both sync and async updates, including explicit empty collections, false values, and nullable field clears.

[[src/vercel-sandbox/vercel/sandbox/_internal/service.py#SandboxService#update_sandbox]] owns the ordering. It is an async generator that takes the current session handle's latest state and yields an [[src/vercel-sandbox/vercel/sandbox/_internal/state.py#UpdateSandboxStep]] per step. A larger `execution_time_limit` extends a running current session by the difference. Failures that [[src/vercel-sandbox/vercel/sandbox/_internal/service.py#_is_session_gone_error]] recognizes are ignored: any HTTP 410 and HTTP 422 `sandbox_stopping`, matching the TypeScript SDK rather than lifecycle recovery. `network_policy` is applied to the current session unless its status is stopped, stopping, aborted, or failed; a live-policy failure raises before configuration is saved. The configuration and tags are saved last, and the operations are not atomic.

[[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#Sandbox#update]] and [[src/vercel-sandbox/vercel/sandbox/_internal/sync_runtime.py#SyncSandbox#update]] drain it through [[src/vercel-sandbox/vercel/sandbox/_internal/runtime_common.py#SandboxHandleBase#_apply_update_steps]], a coroutine the sync runtime runs with `iter_coroutine`. Each step is applied before the next one runs, so a later failure keeps the changes that already succeeded. Live session results go through [[src/vercel-sandbox/vercel/sandbox/_internal/runtime_common.py#SandboxHandleBase#_apply_current_session_payload_if_current]], which ignores a result for a session that a concurrent recovery has replaced. The final saved sandbox is applied whole.

The sandbox-level `update_network_policy()` still works but emits `DeprecationWarning` in favor of `update(network_policy=...)`, matching the TypeScript SDK. [[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#SandboxRuntimeSession#update_network_policy]] is not deprecated: like TypeScript `session.update()`, it changes only that session without saving the policy, and keeps the parent sandbox's cached session state in sync.

## Processes and filesystem

Processes and file transfers stream with bounded memory in both I/O modes.

[[src/vercel-sandbox/vercel/sandbox/protocols.py#SandboxExecution|Async execution]] and [[src/vercel-sandbox/vercel/sandbox/protocols.py#SyncSandboxExecution|sync execution]] expose `create_command` and `run_command` on sandbox, runtime session, and user handles, matching product and TypeScript SDK naming. The former returns a live process handle; the latter waits for a completed process. Process types, lookup methods, service internals, and wire endpoints retain their process names. The old `create_process` and `run_process` methods delegate to the command methods with `DeprecationWarning`, retaining their signatures, return values, and exceptions. [[src/vercel-sandbox/tests/test_sandbox_users.py#test_deprecated_process_methods_preserve_command_contract|Compatibility tests]] cover forwarding, warning locations, and failure propagation across all handles.

Process output is routed by [[src/vercel-sandbox/vercel/sandbox/_internal/process_output.py#ProcessOutputRouter]] and read through [[src/vercel-sandbox/vercel/sandbox/_internal/text_reader.py]]. File reads and writes share state machines in [[src/vercel-sandbox/vercel/sandbox/_internal/filesystem_handle_core.py]], with sync and async facades on top. Bulk transfers are encoded as tar+gzip on the fly by [[src/vercel-sandbox/vercel/sandbox/_internal/streaming_archive.py#ArchiveRequestWriter]] rather than being built in memory.

[[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#Sandbox#run_command|run_command]] accepts a synchronous `on_start` callback, adapted by [[src/vercel-sandbox/vercel/sandbox/_internal/runtime_common.py#_ProcessStartHook]]. [[src/vercel-sandbox/vercel/sandbox/_internal/api_client.py#SandboxApiClient#run_process]] calls it in the caller's task or thread right after the start record and before routing output, so output waits until it returns. With `input`, it runs before the input is fed, which still happens if it raises; if that feed fails the process is killed, but the callback's exception is the one raised, as in the TypeScript SDK. Its exceptions bypass the interrupted-stream resume path, and the hook's `can_replay` stops lifecycle recovery from rerunning a started command. [[src/vercel-sandbox/tests/test_sandbox_process.py#test_run_command_on_start_error_propagates_without_kill_or_replay|Process tests]] cover ordering, output equivalence, error propagation, and replay.

## Process stdin

Process stdin is a byte stream backed by offset-carrying requests, so writes can be resent without duplicating input and any handle can continue a stream.

[[src/vercel-sandbox/vercel/sandbox/_internal/process_stdin.py#StdinCore]] holds the shared write logic. The async [[src/vercel-sandbox/vercel/sandbox/_internal/process_stdin.py#ProcessStdin]] is an AnyIO `ByteSendStream`; the sync [[src/vercel-sandbox/vercel/sandbox/_internal/process_stdin.py#SyncProcessStdin]] is a writable `io.RawIOBase`. Writes are serialized in call order and split into [[src/vercel-sandbox/vercel/sandbox/_internal/process_stdin.py#STDIN_CHUNK_SIZE]] requests.

Before its first write a handle reads the service's confirmed byte count, and fails without writing if the service doesn't report it. Every write then carries that offset, which is what makes network errors, 429, 502, 503, and progressing 504 timeouts safe to resend; a timeout without a byte count counts as stalled. A failure that may have delivered part of a write breaks the stream for further writes but not for closing.

Processes created with `stdin=subprocess.PIPE`, and every process from `get_process` or `query_processes`, have a writer. The service reports "never attached" and "stopped reading" with the same error, so a looked-up handle treats that error on its first request as stdin being unavailable rather than as a closed pipe. `communicate()` on a looked-up handle closes stdin only when given input or after writing. `run_command(input=...)` feeds input via [[src/vercel-sandbox/vercel/sandbox/_internal/process_stdin.py#feed_process_input]] once the start record arrives, killing the command if that fails.

User processes inherit stdin through the launcher, which saves the original descriptor and hands it to the command.

## Linux user contexts

User handles enforce Linux permissions for commands and reads while treating SDK uploads and directory creation as trusted orchestration.

[[src/vercel-sandbox/vercel/sandbox/_internal/user_core.py#UserContext#provision|Account provisioning]] creates a Bash account with a home owned by that account and the sandbox default group, with group access for HTTP uploads. Existing usernames raise [[src/vercel-sandbox/vercel/sandbox/_internal/errors.py#SandboxUserAlreadyExistsError]] with the username for recovery through account selection. Occupied homes, conflicting groups without accounts, and other provisioning failures remain generic Sandbox errors. Existing homes and symlinks are rejected before account creation. Selecting an existing account resolves its actual home and primary group without changing its home permissions.

[[src/vercel-sandbox/vercel/sandbox/_internal/user_core.py#UserService#open_archive_upload|Uploads]] stream through the parent HTTP archive endpoint; file contents never enter command arguments. After success, privileged metadata commands assign files to the selected account and its primary group. Ancestors below that account's home are assigned to the account and sandbox default group with group write and traversal access. [[src/vercel-sandbox/vercel/sandbox/_internal/user_core.py#UserService#mkdir|Directory creation]] uses the same HTTP account and directory metadata policy, including the requested directory itself.

These SDK writes can reach paths that the selected account cannot access through commands. They are not a permission boundary for untrusted callers, and still depend on the HTTP account's filesystem access. An existing account's home may need explicit preparation. Failed transfers or metadata commands can leave files; ownership is applied only after a successful HTTP upload.

[[src/vercel-sandbox/vercel/sandbox/_internal/user_core.py#UserService#open_read_response|File reads]] execute as the selected account and decode base64 command output to preserve binary bytes. Other filesystem commands also retain the account's permissions. User handles created from sandboxes follow lazy resume; session handles remain pinned.

### User filesystem tests

Public API regressions verify native HTTP payload transport, ownership, binary reads, and the distinction between trusted SDK writes and account permissions.

[[src/vercel-sandbox/tests/test_sandbox_users.py#test_create_user_distinguishes_existing_accounts_from_provisioning_errors|Provisioning error tests]] verify both public APIs expose a recoverable existing-username error while preserving other failures. [[src/vercel-sandbox/tests/test_sandbox_users.py#test_user_upload_keeps_payload_out_of_commands|Wire tests]] cover both APIs, ensure the payload appears in the archive rather than command arguments, and propagate HTTP or metadata failures. [[src/vercel-sandbox/tests/live/test_sandbox_users_live.py#test_user_identity_and_permission_enforcement|Live tests]] cover binary eager, batched, and streamed writes; explicit close; file and directory ownership; private reads and command denial; and existing accounts with different homes and groups.

## Drives

Drives are named persistent filesystems scoped to one project and region and mounted into sandboxes at creation.

A mount is read-write by default, or a point-in-time read-only snapshot. Mount paths are canonicalized and validated in [[src/vercel-sandbox/vercel/sandbox/_internal/models.py]]: absolute, non-overlapping, no `..`, never `/`. Each drive must be in the sandbox's region, and mounted sandboxes cannot use failover regions. A read-write drive attached to another running sandbox is rejected by the API, and a drive must be unmounted before it can be deleted.

[[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#Drive#fork|Drive.fork]] and [[src/vercel-sandbox/vercel/sandbox/_internal/sync_runtime.py#SyncDrive#fork|SyncDrive.fork]] copy committed data into an independent Drive with a new name, inheriting the source's region and maximum size. The top-level [[src/vercel-sandbox/vercel/sandbox/__init__.py#fork_drive|fork_drive]] and its [[src/vercel-sandbox/vercel/sandbox/sync/__init__.py#fork_drive|sync counterpart]] accept a source name directly, avoiding a lookup. Standalone clients expose the same operation. Handles expose optional immediate-parent and root Drive IDs for lineage; API errors preserve missing-source, name-conflict, and uninitialized-source failures.

[[src/vercel-sandbox/tests/live/test_sandbox_live.py#test_drive_fork_and_fork_by_name_copy_committed_data_independently|The live fork test]] verifies both APIs against committed data, inherited metadata, lineage, and independent writes, and cleans up all allocated resources.

## Interactive PTY sessions

PTY sessions run over a WebSocket and expose terminal I/O as the native byte stream of each mode.

[[src/vercel-sandbox/vercel/sandbox/_internal/interactive_session.py]] implements the framing and transport. The async session's stream is an AnyIO `ByteStream` ([[src/vercel-sandbox/vercel/sandbox/_internal/async_runtime.py#InteractiveStream]]); the sync one is an `io.RawIOBase` ([[src/vercel-sandbox/vercel/sandbox/_internal/sync_runtime.py#SyncInteractiveStream]]).

A connection that ends before the process reports an exit code raises rather than looking like a clean end of stream. Output that arrives while waiting is buffered up to a bound, after which `wait()` blocks until the stream is read.

## Console scripts

The package installs `vercel-sandbox` and `sandbox` commands that delegate to the JavaScript CLI.

Both run `npx sandbox` with the original arguments ([[src/vercel-sandbox/vercel/sandbox/__main__.py]]), so they need Node.js; the Python API itself does not.
