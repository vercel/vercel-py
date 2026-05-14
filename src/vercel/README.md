# vercel Package Map

This package exposes small public modules for Vercel APIs and Vercel Function
helpers.

## Public Modules

- `vercel.blob` - Vercel Blob uploads, downloads, metadata, listing, copy, and delete
- `vercel.cache` - Runtime Cache clients and invalidation helpers
- `vercel.cron` - cron decorators and schedule helpers
- `vercel.deployments` - deployment creation and deployment file upload helpers
- `vercel.functions` - convenience exports for function code
- `vercel.oidc` - OIDC token lookup, refresh, credentials, and payload decoding
- `vercel.projects` - project list, create, update, and delete helpers
- `vercel.sandbox` - Sandbox creation, commands, files, snapshots, and PTY support
- `vercel.workflow` - Workflows, steps, sleeps, hooks, and run startup
- `vercel.client` - `AsyncVercel` and `Vercel` grouped clients
- `vercel.env` - Vercel system environment variable parsing
- `vercel.headers` - request header context, IP, and geolocation helpers

## Internals

`vercel._internal` contains implementation details shared by the public modules.
Do not import from it in application code or generated examples; it may change
without public API guarantees.
