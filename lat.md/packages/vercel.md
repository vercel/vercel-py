# vercel

The aggregate distribution: it owns only the `vercel` console script and otherwise installs the SDK's packages as dependencies.

Lives in [[src/vercel]]. Its sources sit directly in that directory rather than a nested `vercel/` package; the Hatch wheel config maps the directory onto `vercel`.

Every other `vercel.*` module comes from a dependency: [[vercel-blob]], [[vercel-cache]], [[vercel-context]], [[vercel-cron]], [[vercel-headers]], [[vercel-internal-core]], [[vercel-internal-telemetry]], [[vercel-oidc]], [[vercel-platform]], [[vercel-queue]], [[vercel-sandbox]], and [[vercel-workflow]]. The remaining service packages and the framework integrations are installed separately.

## Console script

Installing the package adds a `vercel` command that delegates to the Vercel CLI.

[[src/vercel/__main__.py#main]] executes `npx vercel` with the original arguments and fails with a clear message when `npx` is unavailable. The Python package does not reimplement the CLI.
