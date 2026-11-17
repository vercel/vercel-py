# vercel-context

Helpers for code running inside a Vercel Function, plus typed access to Vercel's system environment variables.

Lives in [[src/vercel-context]] and is exposed as [[src/vercel-context/vercel/functions/__init__.py|vercel.functions]] and [[src/vercel-context/vercel/env.py|vercel.env]]. It depends on [[vercel-cache]] and [[vercel-headers]] for the helpers it re-exports.

## Function helpers

Helpers for code running inside a Vercel Function, mostly re-exported from the packages that own them.

[[src/vercel-context/vercel/functions/__init__.py|vercel.functions]] gathers cache, headers, geolocation, environment, and lifecycle helpers in one import:

- [[src/vercel-context/vercel/functions/wait_until.py#wait_until]] keeps the invocation alive for an awaitable after the response is sent, through the runtime callback registered in the [[vercel-cache#Request context]]. Outside a Function it is a no-op that closes an un-awaited coroutine rather than leaking a warning.
- [[src/vercel-context/vercel/functions/deadline.py#get_deadline]] returns the invocation deadline from the `vercel_runtime` module when it is present, and `None` off Vercel.
- [[src/vercel-context/vercel/env.py#get_env]] parses Vercel's system environment variables into a typed [[src/vercel-context/vercel/env.py#Env]].
