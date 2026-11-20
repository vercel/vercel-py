# vercel-apscheduler

Runs the jobs of an APScheduler 3.x scheduler as Vercel schedules: each job deploys as one cron schedule, and each firing runs the job in a function.

Lives in [[integrations/vercel-apscheduler]] and is exposed as [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/__init__.py|vercel.integrations.apscheduler]]. It builds on the `@schedule` registry of [[vercel-schedules#Declared schedules]].

## Declaring jobs as schedules

A `vercel.json` Python schedule names the module that declares the jobs; importing it registers one `@schedule` per job, named after the job's id.

The Vercel builder calls [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_install.py#install_vercel_apscheduler_integration]] before it imports the module, and the runtime calls it before importing a deployment's code. It patches `BaseScheduler.add_job`, so each job goes through [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_jobs.py#declare]], which registers a handler with the job's covering cron. The build reads the registry like any other schedule module, so the same deployment path serves both.

A job id must stay the same in every process, because it names the schedule: a job without an explicit id is rejected, and so is an id two schedulers declare. Declaring an id again replaces its schedule only with `replace_existing`, which `scheduled_job()` sets, as a stock scheduler would on start.

The scheduler never starts. `start()` and `shutdown()` do nothing, so a web app can import the same scheduler, and a stock background thread can't run the jobs a second time. `start()` also ends declaration: a later `add_job()`, or one from a running job, raises, since its schedule could never deploy.

## Covering crons

A schedule fires at most once a minute on a five-field cron, so each trigger deploys as the narrowest cron that fires in every minute it can run in.

[[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_triggers.py#cover]] maps each trigger field to the set of values it can take and renders them as a cron in the trigger's timezone. Fields a cron can't express, such as `last`, `3rd fri`, `week`, `year`, start and end dates, and intervals that don't divide an hour or a day, widen to a cron that fires more often. Vercel rejects a cron that restricts both the day of month and the day of week, so the field that fires on fewer days is kept, and the day of week is always a list, because Vercel translates a step in it wrongly. Combined triggers combine their children's fields, and any other trigger, or a timezone without an IANA name, fires every minute.

Each firing then asks [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_triggers.py#run_time_in_minute]] whether the trigger runs in the firing's scheduled minute. The answer depends only on the trigger and the minute, so every function instance agrees, without shared state. To keep it that way, the trigger is copied without jitter ([[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_triggers.py#without_jitter]]), an interval without a start date counts from the Unix epoch instead of from process start, and a date trigger without `run_date` or a calendar interval longer than a day without `start_date` is rejected. [[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_triggers.py#runs_more_than_once_a_minute]] rejects triggers no schedule can fire.

## Firings

A firing runs its job the way a stock executor would, inside the schedule's request.

[[integrations/vercel-apscheduler/vercel/integrations/apscheduler/_jobs.py#fire]] skips a minute the trigger doesn't run in and a run later than an explicit `misfire_grace_time`. Without one, a job runs however late its firing arrives, because the integration defaults the grace to `None`: stock APScheduler's one-second grace would skip most firings, which arrive up to about a minute late. A firing scheduled off the minute was invoked by hand, so it runs the job at once. `max_instances` counts runs in the function instance, and trigger jitter becomes a sleep before the run. Coroutine jobs run on the event loop and other jobs in a worker thread, and listeners receive executed, error, missed and max-instances events. A job that raises fails the firing.

## Fixed jobs

A deployment's schedules come from code, so the scheduler can't change jobs at runtime.

`modify_job()`, `reschedule_job()`, `remove_job()` and `remove_all_jobs()` raise, and so does adding a job store other than the memory store, because its jobs aren't declared in code. Executors are not used, and `next_run_time` is rejected because a job runs only when its trigger fires.
