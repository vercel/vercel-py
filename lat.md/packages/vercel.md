# vercel

The batteries-included distribution: function helpers, plus dependencies on most service packages.

Lives in [[src/vercel]]. Its sources sit directly in that directory rather than a nested `vercel/` package; the Hatch wheel config maps the directory onto `vercel`. Modules such as `vercel.blob` ([[vercel-blob]]), `vercel.cron` ([[vercel-cron]]), the REST API clients ([[vercel-platform]]), `vercel.sandbox`, `vercel.queue`, and `vercel.workflow` come from its dependencies, not from this distribution.

## Function helpers

Helpers for code running inside a Vercel Function, mostly re-exported from the packages that own them.

[[src/vercel/functions/__init__.py|vercel.functions]] gathers cache, headers, geolocation, environment, and lifecycle helpers in one import:

- [[src/vercel/functions/wait_until.py#wait_until]] keeps the invocation alive for an awaitable after the response is sent, through the runtime callback registered in the [[vercel-cache#Request context]]. Outside a Function it is a no-op that closes an un-awaited coroutine rather than leaking a warning.
- [[src/vercel/functions/deadline.py#get_deadline]] returns the invocation deadline from the `vercel_runtime` module when it is present, and `None` off Vercel.
- [[src/vercel/env.py#get_env]] parses Vercel's system environment variables into a typed [[src/vercel/env.py#Env]].

## Console script

Installing the package adds a `vercel` command that delegates to the Vercel CLI.

[[src/vercel/__main__.py#main]] executes `npx vercel` with the original arguments and fails with a clear message when `npx` is unavailable. The Python package does not reimplement the CLI.
