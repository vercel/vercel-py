# Services

Each Vercel product has its own workspace distribution, and task-queue frameworks get thin adapters onto Vercel Queues.

Package READMEs are the user-facing reference; this page records what each package is responsible for and how they depend on one another.

## Service packages

Service packages wrap one Vercel product each and build on [[runtime]] for HTTP, sessions, and credentials.

Each entry links the distribution and its public module, the `__init__.py` that defines what users import.

- **Queues** ([[src/vercel-queue]], [[src/vercel-queue/vercel/queue/__init__.py|vercel.queue]]) — publish to topics and consume messages, either push-triggered on Vercel or via polling anywhere, with typed topics and pluggable serialization.
- **Sandbox** ([[src/vercel-sandbox]], [[src/vercel-sandbox/vercel/sandbox/__init__.py|vercel.sandbox]]) — create, fork, snapshot, and drive Vercel Sandboxes, including processes, files, drives, and interactive PTY sessions.
- **Workflow** ([[src/vercel-workflow]], [[src/vercel-workflow/vercel/workflow/__init__.py|vercel.workflow]]) — durable workflows with steps, sleeps, hooks, and streams. It runs on Vercel Queues and can execute against the hosted service or a local world for development.
- **Runtime Cache** ([[src/vercel-cache]], [[src/vercel-cache/vercel/cache/__init__.py|vercel.cache]]) — key/value cache with tag invalidation that falls back to an in-memory cache when the runtime cache environment is unavailable.
- **Connect** ([[src/vercel-connect]], [[src/vercel-connect/vercel/connect/__init__.py|vercel.connect]]) — exchanges the deployment's OIDC token for short-lived third-party credentials so projects never store provider secrets.
- **Proxy** ([[src/vercel-proxy]], [[src/vercel-proxy/vercel/proxy/__init__.py|vercel.proxy]]) — Python middleware that runs before a request reaches the app and continues, rewrites, redirects, or answers it.
- **Telemetry** ([[src/vercel-internal-telemetry]], [[src/vercel-internal-telemetry/vercel/internal/telemetry/__init__.py|vercel.internal.telemetry]]) — internal telemetry helpers shared by other packages.

## Framework integrations

Integrations adapt existing Python task frameworks onto Vercel Queues for delivery and Runtime Cache for results or state.

They live under [[integrations]] and expose `vercel.integrations.<framework>`:

- [[integrations/vercel-celery]] ([[integrations/vercel-celery/vercel/integrations/celery/__init__.py|vercel.integrations.celery]]) — Celery broker transport and result backend.
- [[integrations/vercel-dramatiq]] ([[integrations/vercel-dramatiq/vercel/integrations/dramatiq/__init__.py|vercel.integrations.dramatiq]]) — Dramatiq broker and results backend.
- [[integrations/vercel-django-tasks]] ([[integrations/vercel-django-tasks/vercel/integrations/django/__init__.py|vercel.integrations.django]]) — Django tasks backend.
- [[integrations/vercel-apscheduler]] ([[integrations/vercel-apscheduler/vercel/integrations/apscheduler/__init__.py|vercel.integrations.apscheduler]]) — APScheduler 3.x schedules executed through delayed queue messages on a managed job store.

On Vercel, integrations rely on push delivery: the project declares a `[[tool.vercel.subscribers]]` entrypoint in its `pyproject.toml` and the Vercel build compiles it into a queue-triggered function. Results kept in Runtime Cache expire and are not durable storage.
