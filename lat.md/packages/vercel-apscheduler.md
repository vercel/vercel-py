# vercel-apscheduler

Runs APScheduler 3.x schedules as one durable chain of delayed Vercel Queue messages per deployment, on a managed job store in Runtime Cache.

Lives in [[integrations/vercel-apscheduler]] and is exposed as [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/__init__.py|vercel.integrations.apscheduler]]. The full state machine and failure model are in [[integrations/vercel-apscheduler/SCHEDULER.md]]. It builds on [[vercel-queue]] and [[vercel-cache]].

## Subscriber contract

Applications declare an ordinary scheduler; the builder activates the integration, which registers internal start and wake subscriptions on it.

The project's `[[tool.vercel.subscribers]]` entry points at the scheduler object. Before importing it for discovery, the Vercel builder activates the integration ([[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_adapter.py#install_vercel_apscheduler_integration]]). The scheduler then registers two [[vercel-queue#Subscriptions|subscriptions]] through [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_subscriber.py#register_scheduler]]: *start* activates a generation and reserves its first wake, and *wake* evaluates due jobs and reserves the next wake. Topic names are implementation details that applications never configure.

The scheduler's durable identity is the builder-assigned subscriber ID, not the entrypoint, so renaming a variable or moving a module never orphans its state. The `scheduler_id` option pins an identity explicitly. Imports during discovery have no external effects.

## Wake chain

A wake message asks the scheduler to evaluate every job at one logical time, rather than representing a single job.

[[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_adapter.py#SchedulerAdapter]] processes each wake, runs due jobs on [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_executor.py#VercelInlineExecutor]], and publishes exactly one successor timed for the next due job. Chain progress travels in the messages themselves. Racing finishers compute the same canonical successor under the same idempotency key, so the queue accepts it once, and an evicted cache document cannot fork or strand the chain. There is no idle heartbeat: the chain sleeps until the next job is due.

Only the current [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_types.py#WakeToken]] can run and reserve a successor. `pause()` fences the current generation, and `resume()` creates a new one, skipping occurrences that fell inside the pause. These are chain guarantees, not exactly-once execution: delivery is at least once, so jobs must be idempotent.

## Managed job store

Runtime Cache is evictable and per-region, so each guarantee is carried by something that can actually keep it.

- Jobs declared in code are durable because the code is the backup: reconciliation rewrites them whenever the store's documents are missing.
- Jobs added at runtime and lifecycle flags are best-effort and can be lost to eviction. `pause()` also publishes a queue-borne control message so it reaches the process serving the chain.
- Under `vercel dev`, the cache falls back to process memory, which gives a zero-infrastructure development mode.

The store lives under [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_backends/cache]]. Stores under other aliases are read-only source stores whose schedules an external system owns.

## Activation and environments

Production schedulers activate on their first real request; preview schedulers are inactive unless the project opts in.

[[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_automatic.py#register_automatic_activation]] defers the first durable transition until the runtime has installed the request's OIDC credentials, so builds never enqueue messages. A deployment that has never received a request cannot start on its own.

State is scoped by environment in production and by deployment in previews ([[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_options.py#resolve_state_scope]]). Preview scheduling, when enabled under `[tool.vercel.apscheduler.previews]`, has an idle timeout renewed by incoming requests. One production deployment owns the chain at a time; taking ownership syncs the store to that code's declarations, and a demoted deployment's actions are inert.

## Restrictions

The first version deliberately narrows APScheduler's surface to what the durable chain can guarantee.

It supports APScheduler 3.x only, with the managed store as the sole `default` store and the default inline executor; other executors and third-party default stores are rejected. Jobs declared in code need explicit stable IDs. Runtime job changes require the scheduler to be active in that Function instance, either automatically or after an explicit `scheduler.start()`.
