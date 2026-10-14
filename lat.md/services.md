# Services

Each Vercel product has its own workspace distribution, and task-queue frameworks get thin adapters onto Vercel Queues.

Package READMEs are the user-facing reference; this page records what each package is responsible for and how they depend on one another.

## Service packages

Service packages wrap one Vercel product each and build on [[runtime]] for HTTP, sessions, and credentials.

- **Queues** ([[src/vercel-queue]]) — publish to topics and consume messages, either push-triggered on Vercel or via polling anywhere, with typed topics and pluggable serialization.
- **Sandbox** ([[src/vercel-sandbox]]) — create, fork, snapshot, and drive Vercel Sandboxes, including processes, files, drives, and interactive PTY sessions. Entry point: [[src/vercel-sandbox/vercel/sandbox/__init__.py#create_sandbox]].
- **Workflow** ([[src/vercel-workflow]]) — durable workflows with steps, sleeps, hooks, and streams, registered on [[src/vercel-workflow/vercel/workflow/_internal/core.py#Workflows]]. It runs on Vercel Queues and can execute against the hosted service or a local world for development.
- **Runtime Cache** ([[src/vercel-cache]]) — key/value cache with tag invalidation that falls back to an in-memory cache when the runtime cache environment is unavailable.
- **Connect** ([[src/vercel-connect]]) — exchanges the deployment's OIDC token for short-lived third-party credentials so projects never store provider secrets.
- **Proxy** ([[src/vercel-proxy]]) — Python middleware that runs before a request reaches the app and continues, rewrites, redirects, or answers it, built on [[src/vercel-proxy/vercel/proxy/proxy.py#Proxy]].
- **Telemetry** ([[src/vercel-internal-telemetry]]) — internal telemetry helpers shared by other packages.

## Framework integrations

Integrations adapt existing Python task frameworks onto Vercel Queues for delivery and Runtime Cache for results or state.

They live under [[integrations]] and expose `vercel.integrations.<framework>`:

- [[integrations/vercel-celery]] — Celery broker transport and result backend.
- [[integrations/vercel-dramatiq]] — Dramatiq broker and results backend.
- [[integrations/vercel-django-tasks]] — Django tasks backend.
- [[integrations/vercel-apscheduler]] — APScheduler 3.x schedules executed through delayed queue messages on a managed job store.

On Vercel, integrations rely on push delivery: the project declares a `[[tool.vercel.subscribers]]` entrypoint in its `pyproject.toml` and the Vercel build compiles it into a queue-triggered function. Results kept in Runtime Cache expire and are not durable storage.
