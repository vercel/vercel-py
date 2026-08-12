Job stores added under non-default aliases are now supported as source
stores: read-only schedules owned by an external system, typically a
database-backed `BaseJobStore` that materializes application rows into
jobs. Each wake runs their due jobs with stock APScheduler semantics
(including dispatch-on-`remove_job` stores) and arms the successor at the
exact earliest next run time across every store. There is no polling:
schedule changes made out of band are picked up at the next
chain-scheduled wake, or immediately via an explicit `scheduler.wakeup()`
call, which recomputes the next due time across every store and pulls the
current wake in to it. The durable store remains the one named `default`.
