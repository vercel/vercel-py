# vercel

The batteries-included distribution: the `vercel` console script, and otherwise an aggregate of its dependencies on most service packages.

Lives in [[src/vercel]]. Its sources sit directly in that directory rather than a nested `vercel/` package; the Hatch wheel config maps the directory onto `vercel`. Modules such as `vercel.blob` ([[vercel-blob]]), `vercel.cron` ([[vercel-cron]]), the function helpers `vercel.functions` and `vercel.env` ([[vercel-context]]), the REST API clients ([[vercel-platform]]), `vercel.sandbox`, `vercel.queue`, and `vercel.workflow` come from its dependencies, not from this distribution.

## Console script

Installing the package adds a `vercel` command that delegates to the Vercel CLI.

[[src/vercel/__main__.py#main]] executes `npx vercel` with the original arguments and fails with a clear message when `npx` is unavailable. The Python package does not reimplement the CLI.
