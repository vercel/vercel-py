# vercel-schedules

Client for Vercel Schedules: manages schedules through the Schedules API, and reads the current firing inside a schedule command.

Lives in [[src/vercel-schedules]] and is exposed as [[src/vercel-schedules/vercel/schedules/__init__.py|vercel.schedules]] (async) and [[src/vercel-schedules/vercel/schedules/sync.py|vercel.schedules.sync]], which use identical names and arguments. It follows the TypeScript `@vercel/schedules` wire contract, as of version 0.0.3.

## Layering

Like [[vercel-connect]], Schedules keeps all behavior in one async-only service and adds sync and async runtimes as thin facades.

[[src/vercel-schedules/vercel/schedules/_internal/service.py#SchedulesService]] validates arguments and renders request bodies, through the pure [[src/vercel-schedules/vercel/schedules/_internal/service.py#build_create_body]] and [[src/vercel-schedules/vercel/schedules/_internal/service.py#build_update_body]]. [[src/vercel-schedules/vercel/schedules/_internal/api_client.py#SchedulesApiClient]] owns the wire models and maps error responses to [[src/vercel-schedules/vercel/schedules/_internal/errors.py#SchedulesApiError]], with the API's error `code` and `Retry-After` seconds. A 404 raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#ScheduleNotFoundError]], which is also a `LookupError`.

Responses are parsed strictly: a schedule with an unrecognized target or a cron expression [[src/vercel-schedules/vercel/schedules/_internal/models.py#Cron]] cannot parse raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#SchedulesResponseError]]. Static schedules declared in `vercel.json` target functions and paths, so those targets parse even though creation only accepts queue topics.

## Expressions

A schedule fires on a cron cadence or once at a wall-clock time in an IANA timezone.

[[src/vercel-schedules/vercel/schedules/_internal/models.py#Cron]] holds the five crontab fields, after APScheduler's `CronTrigger`: omitted fields default to `*`, except fields less significant than every field set, which default to their minimum, so `Cron(hour=3)` fires once a day. `day_of_week` always defaults to `*`. Fields accept only numbers, `*`, `,`, `-`, and `/`, which is what the service accepts. A crontab string passed as `cron=` is parsed into a `Cron` first, so a malformed one fails before any request.

[[src/vercel-schedules/vercel/schedules/_internal/service.py#resolve_one_off]] renders a one-off `at` as the wall-clock time and timezone name the service stores. An aware `at` supplies its own `ZoneInfo` name, a naive one needs `timezone=`, and a fixed offset other than UTC is rejected because it carries no name. The service has minute precision, so a time with seconds is rounded up: the firing never runs before `at`, and an `at` in the future never becomes one in the past.

## Credentials

Requests send the deployment's Vercel OIDC token, unless `SchedulesServiceOptions` names another token or `vercel dev` serves a local API.

`vercel dev` sets `VERCEL_SCHEDULE_DEV_API_VERSION` and points `VERCEL_SCHEDULE_BASE_URL` at its local Schedules API, which accepts `VERCEL_SCHEDULE_TOKEN` instead. [[src/vercel-schedules/vercel/schedules/_internal/options.py#local_credentials_factory]] selects that token only outside a deployment and only for the advertised base URL, matching `@vercel/schedules`. An unsupported local API version raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#SchedulesCredentialsError]] when the first request is made.

## Firing environment

A schedule command runs once per firing, and the platform passes the firing's metadata to it in `VERCEL_SCHEDULE_*` environment variables.

[[src/vercel-schedules/vercel/schedules/_internal/metadata.py#ScheduleMetadata#from_env]] reads them into a [[src/vercel-schedules/vercel/schedules/_internal/metadata.py#ScheduleMetadata]]. Its fields match `ScheduleMetadata` in the TypeScript `@vercel/schedules`, and each variable carries one schedule dispatch header, such as `ce-vssschedulename` in `VERCEL_SCHEDULE_NAME`.

`VERCEL_SCHEDULE_EXECUTION_ID` decides whether the process is a firing. Client settings such as `VERCEL_SCHEDULE_BASE_URL` share the prefix and can be inherited by any process, but the execution ID is set only per firing. Without it, `from_env` raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#NotAScheduleInvocationError]]. Any other missing or invalid variable raises its parent, [[src/vercel-schedules/vercel/schedules/_internal/errors.py#ScheduleEventParseError]].
