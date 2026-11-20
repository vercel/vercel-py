# vercel-drives

Client for Vercel Drives: named persistent filesystems that belong to one project and region, and that sandboxes mount.

Lives in [[src/vercel-drives]] and is exposed as [[src/vercel-drives/vercel/drives/__init__.py|vercel.drives]] (async) and [[src/vercel-drives/vercel/drives/sync.py|vercel.drives.sync]], which use identical names and arguments. It was extracted from [[vercel-sandbox]], as the TypeScript `@vercel/drives` was from `@vercel/sandbox`, and Sandbox depends on it for [[vercel-sandbox#Drives]].

## Layering

Drives follows [[runtime#Service packages]]: one async-only service, thin sync and async runtimes, and an API client that owns the wire format.

[[src/vercel-drives/vercel/drives/_internal/api_client.py#DriveApiClient]] speaks the public Drives API under `/v1/drives`: list, get by name or ID, get-or-create by name, fork, and delete. Its request models render camelCase names and turn a [[src/vercel-drives/vercel/drives/_internal/models.py#DriveQuery]] into `sortBy`, `sortOrder`, and `namePrefix`. Every request carries the team and project of its credentials, so Drives always belong to the project the credentials resolve to. The API answers get-or-create with 201 only when it created the Drive, which is how callers learn `created`.

[[src/vercel-drives/vercel/drives/_internal/service.py#DriveService]] rejects names that URL parsing would redirect to another route, then returns [[src/vercel-drives/vercel/drives/_internal/models.py#DriveState]] records. [[src/vercel-drives/vercel/drives/_internal/async_runtime.py#Drive]] and [[src/vercel-drives/vercel/drives/_internal/sync_runtime.py#SyncDrive]] wrap that state; [[src/vercel-drives/vercel/drives/_internal/handle.py#DriveHandleBase]] holds their shared read-only properties. Timestamps stay millisecond integers, matching the other Sandbox handles these were extracted from.

The snapshot and filesystem routes of the Drives API are private and feature-flagged, and are not implemented.

## Handles and mounts

A handle is a snapshot of Drive metadata bound to the service that fetched it, and is what a sandbox mount accepts.

[[src/vercel-drives/vercel/drives/_internal/async_runtime.py#Drive#fork]] copies committed data into an independent Drive with a new name, inheriting the source's region and maximum size; `fork_drive` does the same from a source name, without a lookup. Handles expose the immediate-parent and root Drive IDs for lineage.

[[src/vercel-drives/vercel/drives/_internal/async_runtime.py#Drive#delete]] deletes by project and name, not by Drive ID, then refreshes the handle from the response. If the name now refers to a replacement Drive, that Drive is deleted and the handle becomes it. A response for another name or project raises [[src/vercel-drives/vercel/drives/_internal/errors.py#DriveResponseError]] and leaves the handle unchanged.

[[src/vercel-drives/vercel/drives/_internal/models.py#DriveMount]] pairs a Drive, or a Drive name, with a `read-write` or `snapshot` mode, and [[src/vercel-drives/vercel/drives/_internal/handle.py#DriveHandleBase#snapshot]] builds the snapshot form. The type lives here because handles produce it; [[vercel-sandbox]] validates and serializes the mount map.

## Configuration

A session configures Drives with its own options, independent of the Sandbox options.

[[src/vercel-drives/vercel/drives/_internal/options.py#DriveServiceOptions]] takes an async credentials factory and [[src/vercel-drives/vercel/drives/_internal/options.py#SyncDriveServiceOptions]] a plain one, which `vercel.drives.sync` exports as `DriveServiceOptions`. Both return [[src/vercel-drives/vercel/drives/_internal/options.py#DriveCredentials]]: a token, team, and project. The default comes from [[vercel-oidc#Credentials]], and a failure to resolve it raises [[src/vercel-drives/vercel/drives/_internal/errors.py#DriveCredentialsError]].

Creation omits region unless the call or the service option requests one. [[src/vercel-internal-core/vercel/_internal/core/region.py#CURRENT_REGION]] opts into the current function's region, resolved once in the runtime entry points before the service runs.

[[src/vercel-drives/vercel/drives/_internal/service.py#create_drive_service]] builds a service on a session's transport without caching it. [[vercel-sandbox]] calls it with its own options and error types; see [[vercel-sandbox#Drives#Drive operations]].

## Errors and retries

Requests retry transient failures with the Sandbox policy, and error responses keep the API's status, code, and body.

[[src/vercel-drives/vercel/drives/_internal/retry.py#with_request_retries]] retries transport failures, HTTP 429, and HTTP 5xx twice, with randomized exponential backoff and short numeric `Retry-After` waits, including for mutations. The policy is a copy of the Sandbox one, keyed on this package's error type. Requests have no read timeout unless the options set one.

A non-2xx response raises [[src/vercel-drives/vercel/drives/_internal/errors.py#DriveApiError]], so a missing Drive is `status_code == 404`. [[src/vercel-drives/vercel/drives/_internal/api_client.py#DriveApiClient]] takes the error classes it raises, which lets Sandbox substitute subclasses from its own hierarchy.

## Tests

The public-flow tests run each scenario against both APIs over a mocked network.

[[src/vercel-drives/tests/conftest.py#DriveApi]] awaits either surface, so one test covers `vercel.drives` and `vercel.drives.sync`. [[src/vercel-drives/tests/test_drive_public_flow.py]] covers request scope and bodies, region selection, pagination, forking, deletion, error mapping, retries, malformed responses, and credentials. [[src/vercel-drives/tests/test_drive_sync_async_parity.py]] keeps the two surfaces' names and signatures identical. [[src/vercel-drives/tests/live/test_drive_live.py#test_drive_lifecycle]] runs the lifecycle against the real API when credentials are present.
