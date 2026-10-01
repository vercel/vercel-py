Add `vercel-schedules`, a Python SDK for Vercel Schedules: `create_schedule`,
`list_schedules`, `get_schedule`, `update_schedule`, `enable_schedule`,
`disable_schedule`, `invoke_schedule`, and `delete_schedule` in
`vercel.schedules` with a 1:1 `vercel.schedules.sync` mirror, plus models for
queue- and function-targeted schedules. Schedules are addressed by name and
namespace, and support IANA timezones. `ScheduleEvent.from_env()` and
`get_event()` read the current firing inside a schedule entrypoint.
