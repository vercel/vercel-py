# vercel-schedules

Client for Vercel Schedules: manages schedules through the Schedules API, and reads the current firing inside a schedule command.

Lives in [[src/vercel-schedules]] and is exposed as [[src/vercel-schedules/vercel/schedules/__init__.py|vercel.schedules]] (async) and [[src/vercel-schedules/vercel/schedules/sync.py|vercel.schedules.sync]], which use identical names and arguments. It follows the TypeScript `@vercel/schedules` wire contract, as of version 0.0.3.

## Layering

Schedules follows [[runtime#Service packages]]: one async-only service, thin sync and async runtimes, and an API client that owns the wire format.

[[src/vercel-schedules/vercel/schedules/_internal/service.py#SchedulesService]] validates arguments and makes the decisions a request depends on, such as the timezone of a one-off. It passes Python values to [[src/vercel-schedules/vercel/schedules/_internal/api_client.py#SchedulesApiClient]], whose request models, such as [[src/vercel-schedules/vercel/schedules/_internal/api_client.py#_UpdateScheduleRequest]], render field names, the crontab string, the one-off wall-clock time, jitter minutes, and `until` milliseconds. On PATCH, `None` is sent as `null` to remove a jitter or payload, and an omitted field is left unchanged. The API client also maps error responses to [[src/vercel-schedules/vercel/schedules/_internal/errors.py#SchedulesApiError]], with the API's error `code` and `Retry-After` seconds. A 404 raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#ScheduleNotFoundError]], which is also a `LookupError`.

Responses are parsed strictly: a schedule with an unrecognized target or a cron expression [[src/vercel-schedules/vercel/schedules/_internal/models.py#Cron]] cannot parse raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#SchedulesResponseError]]. Static schedules declared in `vercel.json` target functions and paths, so those targets parse even though creation only accepts queue topics.

## Expressions

A schedule fires on a cron cadence or once at a wall-clock time in an IANA timezone.

[[src/vercel-schedules/vercel/schedules/_internal/models.py#Cron]] holds the five crontab fields, after APScheduler's `CronTrigger`: omitted fields default to `*`, except fields less significant than every field set, which default to their minimum, so `Cron(hour=3)` fires once a day. `day_of_week` always defaults to `*`. Fields accept only numbers, `*`, `,`, `-`, and `/`, which is what the service accepts. Each number must be in the field's range, a range must not end before it starts, and a step must not be zero; the service rejects all of these too, so responses still parse. A crontab string passed as `cron=` is parsed into a `Cron` first, so a malformed one fails before any request.

[[src/vercel-schedules/vercel/schedules/_internal/service.py#resolve_one_off]] renders a one-off `at` as the wall-clock time and timezone name the service stores. An aware `at` supplies its own `ZoneInfo` name, a naive one needs `timezone=`, and a fixed offset other than UTC is rejected because it carries no name. The service has minute precision, so a time with seconds is rounded up: the firing never runs before `at`, and an `at` in the future never becomes one in the past.

## Credentials

Requests send the deployment's Vercel OIDC token, unless the session's options supply a `credentials_factory` or `vercel dev` serves a local API.

[[src/vercel-schedules/vercel/schedules/_internal/options.py#SchedulesServiceOptions]] takes an async factory and [[src/vercel-schedules/vercel/schedules/_internal/options.py#SyncSchedulesServiceOptions]] a plain one, which `vercel.schedules.sync` exports as `SchedulesServiceOptions`. A fixed token is `static_credentials_factory(token)`, from either module.

`vercel dev` sets `VERCEL_SCHEDULE_DEV_API_VERSION` and points `VERCEL_SCHEDULE_BASE_URL` at its local Schedules API, which accepts `VERCEL_SCHEDULE_TOKEN` instead. [[src/vercel-schedules/vercel/schedules/_internal/options.py#local_credentials]] selects that token only outside a deployment and only for the advertised base URL, matching `@vercel/schedules`. An unsupported local API version raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#SchedulesCredentialsError]] when the first request is made.

## Firing environment

A schedule command runs once per firing, and the platform passes the firing's metadata to it in `VERCEL_SCHEDULE_*` environment variables.

[[src/vercel-schedules/vercel/schedules/_internal/metadata.py#ScheduleMetadata#from_env]] reads them into a [[src/vercel-schedules/vercel/schedules/_internal/metadata.py#ScheduleMetadata]], a frozen Pydantic model. Its fields match `ScheduleMetadata` in the TypeScript `@vercel/schedules`, and each variable carries one schedule dispatch header, such as `ce-vssschedulename` in `VERCEL_SCHEDULE_NAME`.

`VERCEL_SCHEDULE_EXECUTION_ID` decides whether the process is a firing. Client settings such as `VERCEL_SCHEDULE_BASE_URL` share the prefix and can be inherited by any process, but the execution ID is set only per firing. Without it, `from_env` raises [[src/vercel-schedules/vercel/schedules/_internal/errors.py#NotAScheduleInvocationError]]. Any other missing or invalid variable raises its parent, [[src/vercel-schedules/vercel/schedules/_internal/errors.py#ScheduleEventParseError]].
