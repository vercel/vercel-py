# vercel-cron

Code-declared cron schedules for Vercel Python applications.

Lives in [[src/vercel-cron]] and is exposed as [[src/vercel-cron/vercel/cron/__init__.py|vercel.cron]]. It has no dependencies.

## Cron jobs

Cron schedules are declared in code with a decorator that the Vercel build discovers.

[[src/vercel-cron/vercel/cron/crontab.py#cron]] attaches a `get_crons()` hook to the decorated function, and [[src/vercel-cron/vercel/cron/crontab.py#CronTab]] collects several jobs in one registry. Either form yields `module:function` entrypoints paired with schedule strings. A schedule string without five fields fails at decoration time. Only module-level functions that resolve back to themselves by `module:name` can be registered, which is checked when the build collects the jobs.
