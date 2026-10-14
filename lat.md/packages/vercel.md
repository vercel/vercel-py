# vercel

The batteries-included distribution: Vercel REST API helpers, Blob, cron, and function helpers, plus dependencies on most service packages.

Lives in [[src/vercel]]. Its sources sit directly in that directory rather than a nested `vercel/` package; the Hatch wheel config maps the directory onto `vercel`. Modules such as `vercel.sandbox`, `vercel.queue`, and `vercel.workflow` come from its dependencies, not from this distribution.

## REST API clients

Deployments, projects, and project routing rules are thin clients over the Vercel REST API, available sync and async.

[[src/vercel/client.py#Vercel]] and [[src/vercel/client.py#AsyncVercel]] group the per-resource clients behind one access token. The module-level helpers ([[src/vercel/deployments/__init__.py|vercel.deployments]], [[src/vercel/projects/__init__.py|vercel.projects]], [[src/vercel/project_routes/__init__.py|vercel.project_routes]]) take the token per call.

Project routes is the reference implementation of [[runtime#Sync and async parity]]. [[src/vercel/_internal/project_routes/core.py#BaseProjectRoutesOpsClient]] holds the async business logic, and the sync and async ops clients only choose the transport. Routing changes follow the platform's staging model: rules are staged as a version, then promoted, restored, or discarded.

## Blob

Vercel Blob storage: uploads, downloads, metadata, listing, copy, folders, and multipart uploads for large objects.

[[src/vercel/blob/__init__.py|vercel.blob]] exposes module functions such as [[src/vercel/blob/ops.py#put]] alongside `BlobClient` and `AsyncBlobClient`. All of them sit on [[src/vercel/_internal/blob/core.py#BaseBlobOpsClient]], with transport-specific subclasses for sync and async. The token comes from the call or from `BLOB_READ_WRITE_TOKEN` (or `VERCEL_BLOB_READ_WRITE_TOKEN`). The store ID is parsed out of that token to build public or private blob URLs.

Requests that fail with retryable error codes are retried, up to `VERCEL_BLOB_RETRIES` times (10 by default). Uploads can be split into parts, either automatically via `multipart=True` or manually through [[src/vercel/blob/multipart/__init__.py|vercel.blob.multipart]]. Shared part sizing and ordering live in [[src/vercel/_internal/blob/multipart.py]].

## Function helpers

Helpers for code running inside a Vercel Function, mostly re-exported from the packages that own them.

[[src/vercel/functions/__init__.py|vercel.functions]] gathers cache, headers, geolocation, environment, and lifecycle helpers in one import:

- [[src/vercel/functions/wait_until.py#wait_until]] keeps the invocation alive for an awaitable after the response is sent, through the runtime callback registered in the [[vercel-cache#Request context]]. Outside a Function it is a no-op that closes an un-awaited coroutine rather than leaking a warning.
- [[src/vercel/functions/deadline.py#get_deadline]] returns the invocation deadline from the `vercel_runtime` module when it is present, and `None` off Vercel.
- [[src/vercel/env.py#get_env]] parses Vercel's system environment variables into a typed [[src/vercel/env.py#Env]].

## Cron jobs

Cron schedules are declared in code with a decorator that the Vercel build discovers.

[[src/vercel/cron/crontab.py#cron]] attaches a `get_crons()` hook to the decorated function, and [[src/vercel/cron/crontab.py#CronTab]] collects several jobs in one registry. Either form yields `module:function` entrypoints paired with schedule strings. A schedule string without five fields fails at decoration time. Only module-level functions that resolve back to themselves by `module:name` can be registered, which is checked when the build collects the jobs.

## Console script

Installing the package adds a `vercel` command that delegates to the Vercel CLI.

[[src/vercel/__main__.py#main]] executes `npx vercel` with the original arguments and fails with a clear message when `npx` is unavailable. The Python package does not reimplement the CLI.
