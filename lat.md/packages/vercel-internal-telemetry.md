# vercel-internal-telemetry

Opt-out usage telemetry for SDK operations, batched in process and flushed at exit.

Lives in [[src/vercel-internal-telemetry]] and is exposed as [[src/vercel-internal-telemetry/vercel/internal/telemetry/__init__.py|vercel.internal.telemetry]]. Used by [[vercel]] (deployments, projects) and [[vercel-cache]].

## Event collection

Operations record events through one tracker; the client buffers them and sends a batch when the process exits.

[[src/vercel-internal-telemetry/vercel/internal/telemetry/tracker.py#track]] and the `telemetry` decorator are the only entry points packages use. [[src/vercel-internal-telemetry/vercel/internal/telemetry/client.py#TelemetryClient]] attaches a per-process session ID and registers an `atexit` flush. Event fields pass through a whitelist rather than being forwarded wholesale. Team and project IDs come from [[src/vercel-internal-telemetry/vercel/internal/telemetry/credentials.py#extract_credentials]].

## Opting out

Telemetry is controlled entirely by environment variables, so it can be disabled without code changes.

`VERCEL_TELEMETRY_DISABLED=1` disables collection, `VERCEL_TELEMETRY_DEBUG=1` prints delivery results and errors, and `VERCEL_TELEMETRY_BRIDGE_URL` overrides the endpoint. Tracking and delivery failures are swallowed so telemetry can never break the operation being tracked.
