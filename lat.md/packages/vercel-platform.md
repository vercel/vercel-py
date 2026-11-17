# vercel-platform

Clients for the Vercel REST API: deployments, projects, and project routing rules, available sync and async.

Lives in [[src/vercel-platform]] and is exposed as [[src/vercel-platform/vercel/deployments/__init__.py|vercel.deployments]], [[src/vercel-platform/vercel/projects/__init__.py|vercel.projects]], [[src/vercel-platform/vercel/project_routes/__init__.py|vercel.project_routes]], and [[src/vercel-platform/vercel/client.py|vercel.client]]. It depends on [[vercel-internal-core]] and on [[vercel-internal-telemetry]] for usage tracking.

## REST API clients

Deployments, projects, and project routing rules are thin clients over the Vercel REST API, available sync and async.

[[src/vercel-platform/vercel/client.py#Vercel]] and [[src/vercel-platform/vercel/client.py#AsyncVercel]] group the per-resource clients behind one access token. The module-level helpers ([[src/vercel-platform/vercel/deployments/__init__.py|vercel.deployments]], [[src/vercel-platform/vercel/projects/__init__.py|vercel.projects]], [[src/vercel-platform/vercel/project_routes/__init__.py|vercel.project_routes]]) take the token per call.

Project routes is the reference implementation of [[runtime#Sync and async parity]]. [[src/vercel-platform/vercel/project_routes/_internal/core.py#BaseProjectRoutesOpsClient]] holds the async business logic, and the sync and async ops clients only choose the transport. Routing changes follow the platform's staging model: rules are staged as a version, then promoted, restored, or discarded.
