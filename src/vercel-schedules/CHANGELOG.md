# Changelog

## 0.1.0 - 2026-11-18

### Features

- Add `@schedule` to declare recurring schedules in code, with the `max_duration` and
  `regions` of the function that runs them, `get_registered_schedules()`
  to read them, and `ScheduleMetadata.from_headers()` to read a firing from a schedule
  dispatch request. (#515)

- Add `vercel-schedules`, a Python SDK for Vercel Schedules. `ScheduleMetadata.from_env()`
  reads the current firing from the `VERCEL_SCHEDULE_*` environment variables
  inside a schedule command. (#514)

- Add a client for the Vercel Schedules API: `create_schedule`, `query_schedules`,
  `get_schedule`, `update_schedule`, `enable_schedule`, `disable_schedule`,
  `invoke_schedule`, and `delete_schedule` in `vercel.schedules`, with a 1:1
  `vercel.schedules.sync` mirror. Recurring schedules take a `Cron` or a crontab
  string, and one-off schedules an aware `datetime` in an IANA timezone. (#395)

