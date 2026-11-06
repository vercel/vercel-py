# vercel-schedules

SDK for Vercel Schedules. It reads the current firing inside a schedule command.

Lives in [[src/vercel-schedules]] and is exposed as [[src/vercel-schedules/vercel/schedules/__init__.py|vercel.schedules]].

## Firing environment

A schedule command runs once per firing, and the platform passes the firing's metadata to it in `VERCEL_SCHEDULE_*` environment variables.

[[src/vercel-schedules/vercel/schedules/_internal/metadata.py#ScheduleMetadata#from_env]] reads them into a [[src/vercel-schedules/vercel/schedules/_internal/metadata.py#ScheduleMetadata]]. Its fields match `ScheduleMetadata` in the TypeScript `@vercel/schedules`, and each variable carries one schedule dispatch header, such as `ce-vssschedulename` in `VERCEL_SCHEDULE_NAME`.

`VERCEL_SCHEDULE_EXECUTION_ID` decides whether the process is a firing. Client settings such as `VERCEL_SCHEDULE_BASE_URL` share the prefix and can be inherited by any process, but the execution ID is set only per firing. Without it, `from_env` raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#NotAScheduleInvocationError]]. Any other missing or invalid variable raises its parent, [[src/vercel-schedules/vercel/schedules/_internal/errors.py#ScheduleEventParseError]].
