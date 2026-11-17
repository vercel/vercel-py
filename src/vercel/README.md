# vercel

`vercel` is the aggregate distribution of the Vercel Python SDK. Installing it
installs the SDK's packages and the `vercel` console script, which runs the
Vercel CLI through `npx vercel`.

```sh
pip install vercel
```

## Public Modules

| Module | Distribution |
| --- | --- |
| `vercel.api` - shared API session context | `vercel-internal-core` |
| `vercel.blob` - Blob object storage | `vercel-blob` |
| `vercel.cache` - Runtime Cache clients and invalidation helpers | `vercel-cache` |
| `vercel.client` - `AsyncVercel` and `Vercel` grouped clients | `vercel-platform` |
| `vercel.cron` - cron decorators and schedule helpers | `vercel-cron` |
| `vercel.deployments` - deployment creation and file upload helpers | `vercel-platform` |
| `vercel.env` - Vercel system environment variable parsing | `vercel-context` |
| `vercel.errors` - shared exceptions | `vercel-internal-core` |
| `vercel.functions` - convenience exports for function code | `vercel-context` |
| `vercel.headers` - request header context, IP, and geolocation helpers | `vercel-headers` |
| `vercel.oidc` - OIDC token lookup, refresh, credentials, and payload decoding | `vercel-oidc` |
| `vercel.project_routes` - project-level routing rule staging, publishing, and history | `vercel-platform` |
| `vercel.projects` - project list, create, update, and delete helpers | `vercel-platform` |
| `vercel.queue` - Queue topics, publishing, and subscribers | `vercel-queue` |
| `vercel.sandbox` - Sandbox creation, commands, files, and snapshots | `vercel-sandbox` |
| `vercel.workflow` - Workflows, steps, sleeps, hooks, and run startup | `vercel-workflow` |

## Internals

`vercel._internal` contains implementation details shared by the public modules.
Do not import from it in application code or generated examples; it may change
without public API guarantees.
