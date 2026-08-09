Job stores added under non-default aliases are now supported as source
stores: read-only schedules owned by an external system, typically a
database-backed `BaseJobStore` that materializes application rows into
jobs. Each wake runs their due jobs with stock APScheduler semantics
(including dispatch-on-`remove_job` stores), and the wake chain re-polls
them on a bounded interval (default 30 seconds,
`VERCEL_APSCHEDULER_SOURCE_POLL_INTERVAL_SECONDS`) so schedule changes made
out of band are picked up. The durable store remains the one named
`default`.
