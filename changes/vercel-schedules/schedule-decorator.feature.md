Add `@schedule` to declare recurring schedules in code, with the `max_duration` and
`regions` of the function that runs them, `get_registered_schedules()`
to read them, and `ScheduleMetadata.from_headers()` to read a firing from a schedule
dispatch request.
