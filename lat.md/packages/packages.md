One document per workspace distribution, covering what it owns, how it works, and the design decisions behind it.

Package READMEs remain the user-facing reference. These pages explain the behavior and constraints a contributor needs before changing a package. See [[architecture]] for how the distributions fit together.

## Foundation

Shared building blocks that service packages depend on; most applications never import them directly.

- [[vercel-internal-core]] — sessions, transports, sync/async bridging, and shared errors.
- [[vercel-headers]] — the per-request header store and geolocation helpers.
- [[vercel-oidc]] — OIDC token resolution, credentials, and inbound token verification.
- [[vercel-internal-telemetry]] — opt-out SDK usage telemetry.

## Services

Each wraps one Vercel product.

- [[vercel]] — the aggregate distribution: deployments, projects, project routes, cron, and function helpers.
- [[vercel-blob]] — Vercel Blob uploads, downloads, collections, and multipart uploads.
- [[vercel-cache]] — Runtime Cache clients and tag invalidation.
- [[vercel-queue]] — Vercel Queues publishing, push delivery, polling, and local emulation.
- [[vercel-sandbox]] — Vercel Sandbox lifecycle, processes, filesystems, drives, and PTYs.
- [[vercel-workflow]] — durable workflows replayed from an event log.
- [[vercel-connect]] — short-lived third-party credentials brokered by Vercel Connect.
- [[vercel-proxy]] — Python routing middleware that runs in front of the app.

## Framework integrations

Adapters that run existing task frameworks on Vercel Queues, keeping results or state in Runtime Cache.

- [[vercel-celery]] — Celery broker transport and result backend.
- [[vercel-dramatiq]] — Dramatiq broker and result backend.
- [[vercel-django-tasks]] — Django tasks backend.
- [[vercel-apscheduler]] — APScheduler 3.x schedules driven by delayed queue messages.
