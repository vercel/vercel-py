# Architecture

The Python SDK for Vercel is a uv workspace of small distributions that together populate one `vercel` import namespace.

Each distribution can be installed, versioned, and released on its own, while users still write `from vercel import ...`. Workspace members are declared in [[pyproject.toml]] as everything under `src/` and `integrations/`.

## Namespace distributions

Every workspace package ships a slice of the shared `vercel` namespace package rather than a top-level package of its own.

For example, [[vercel-oidc]] provides `vercel.oidc`, [[vercel-sandbox]] provides `vercel.sandbox`, and the framework integrations under [[integrations]] provide `vercel.integrations.<framework>`. Each distribution has its own page under [[packages]]. Because no distribution owns `vercel/__init__.py`, any combination of them can be installed side by side.

## The aggregate vercel distribution

The [[vercel]] distribution in [[src/vercel]] is the batteries-included entry point that depends on most service packages.

It owns the Vercel REST API helpers (`vercel.blob`, `vercel.deployments`, `vercel.projects`, `vercel.project_routes`), function helpers such as `vercel.cron` and `vercel.env`, and the grouped [[src/vercel/client.py#Vercel]] and [[src/vercel/client.py#AsyncVercel]] clients. Modules such as `vercel.sandbox` and `vercel.workflow` arrive through its dependencies on the separately owned packages.

Its sources sit directly in `src/vercel/` instead of a nested `vercel/` directory; the Hatch wheel config maps that directory onto the `vercel` package.

## Public API and internals

Application code imports from public modules under `vercel.*`; anything under a `_internal` package is an implementation detail without compatibility guarantees.

Service packages keep their private code in `vercel.<service>._internal`. Cross-package internals live in [[vercel-internal-core]], which is installed only as a dependency of service packages and is pinned to one minor series. It also contributes the small public `vercel.api` (session context) and `vercel.errors` (shared exceptions) modules.

## Shared dependencies

Packages share a deliberately small runtime stack so they behave consistently when installed together.

- **HTTPX2** (`httpx2`) is the only HTTP client family the SDK installs. Legacy `httpx` clients are accepted only as a runtime migration path, through [[runtime#SDK sessions]] or the [[vercel-queue]] client factory.
- **AnyIO** provides concurrency primitives in implementation code; direct `asyncio` usage is limited to tests, examples, and documentation. CI tests every package against both the lowest and the latest supported AnyIO.
- **Python 3.10** is the minimum version for every package.

Workspace dependencies are declared with lower bounds that the release tooling keeps current; see [[tooling#Releases#Dynamic dependencies]].
