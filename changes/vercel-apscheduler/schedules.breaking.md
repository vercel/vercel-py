Run APScheduler jobs as Vercel schedules instead of a chain of delayed queue
messages. Name the module that declares the jobs as a Python schedule in
`vercel.json`; each job deploys as a schedule named after its id. Jobs are fixed
at deploy, triggers that run more than once a minute are rejected, and the
Runtime Cache job store, source job stores, `[[tool.vercel.subscribers]]`
entrypoint and preview idle scheduling are removed.
