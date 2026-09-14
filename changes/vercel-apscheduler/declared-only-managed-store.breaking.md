The managed job store is now immutable at runtime. `add_job()` with a new
job id, `modify_job`, `reschedule_job`, `pause_job`, `resume_job`,
`remove_job`, `remove_all_jobs`, and `add_job(replace_existing=True)` of an
existing id are all rejected outside declaration time, so the store holds
only code-declared jobs plus execution progress and cache eviction can never
silently lose or resurrect an operator's intent. Change a job by deploying
its changed declaration; stop a job by removing or gating its declaration,
or by checking application-owned state inside the job. Keep dynamic
schedules in your own database, or publish a delayed queue message for
one-shot work.
