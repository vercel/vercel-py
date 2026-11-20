# vercel-board

Client for Vercel Board: channel-scoped messaging, profiles, permission narrowing, and dual-protocol support for hosted and self-hosted boards.

Lives in [[src/vercel-board]] and is exposed as [[src/vercel-board/vercel/board/__init__.py|vercel.board]] (async) and [[src/vercel-board/vercel/board/sync.py|vercel.board.sync]], which mirror identical names and arguments.

## Layering and sync/async parity

Board follows [[runtime#Service packages]]: one async-only service, thin sync and async handle runtimes, and a wire-isolated API client.

- [[src/vercel-board/vercel/board/_internal/async_runtime.py#Board]] and [[src/vercel-board/vercel/board/_internal/sync_runtime.py#SyncBoard]] are lazy, immutable handles carrying a validated [[src/vercel-board/vercel/board/_internal/service.py#BoardTarget]]. Handles created via [[src/vercel-board/vercel/board/_internal/async_runtime.py#board]] or [[src/vercel-board/vercel/board/_internal/sync_runtime.py#sync_board]] resolve the active SDK session on each call, while [[src/vercel-board/vercel/board/_internal/async_runtime.py#BoardClient]] and [[src/vercel-board/vercel/board/_internal/sync_runtime.py#SyncBoardClient]] own a pinned session.
- [[src/vercel-board/vercel/board/_internal/service.py#BoardService]] owns argument validation (channels, refs, labels, emoji normalization, idempotency keys, sync targets) and delegates to [[src/vercel-board/vercel/board/_internal/api_client.py#BoardApiClient]].
- [[src/vercel-board/vercel/board/_internal/sync_runtime.py#SyncBoard]] drives the same service coroutines through [[src/vercel-internal-core/vercel/_internal/core/iter_coroutine.py#iter_coroutine]] over a non-suspending `SyncTransport`, using `session.sleep` (`time.sleep` in sync mode).
- Public value models inherit from [[src/vercel-board/vercel/board/_internal/base.py#BoardValueModel]], which enforces `frozen=True`, `extra="forbid"`, and converts Pydantic `ValidationError` into [[src/vercel-board/vercel/board/_internal/errors.py#BoardValidationError]].

## Dual-protocol wire isolation

All wire Pydantic models, private request-header assembly, board ID resolution, and HTTP error mapping stay inside [[src/vercel-board/vercel/board/_internal/api_client.py]]; scope serialization is public via [[src/vercel-board/vercel/board/_internal/access.py#Can#to_header]].

[[src/vercel-board/vercel/board/_internal/api_client.py#BoardApiClient]] selects one of two private [[src/vercel-board/vercel/board/_internal/api_client.py#_BoardProtocol]] adapters at construction:

- [[src/vercel-board/vercel/board/_internal/api_client.py#_VercelBoardProtocol]] speaks Vercel's hosted `/v1/messageboard` API (default when `url` is `None`). It resolves and caches default or named board IDs (creating a missing named board on first use unless `ensure=False`; `ensure=False` does not prevent creating the team's default board), maps `team` to `teamId` or `slug` query parameters, attaches `operationId` UUIDv7s on writes, and formats hosted profile headers.
- [[src/vercel-board/vercel/board/_internal/api_client.py#_HttpBoardProtocol]] speaks the HTTP Board protocol when `url` or `BOARD_URL` is configured, including `sync` targets on `post`, discovery via `GET /`, and `x-board-profile` headers.

Both adapters share [[src/vercel-board/vercel/board/_internal/api_client.py#_RequestRunner]], which disables redirect following, sends a package `User-Agent`, and retries HTTP 429 responses up to [[src/vercel-board/vercel/board/_internal/api_client.py#MAX_RATE_LIMIT_RETRIES]] times using `Retry-After` or exponential backoff. Private [[src/vercel-board/vercel/board/_internal/api_client.py#_WireRequestModel]] and [[src/vercel-board/vercel/board/_internal/api_client.py#_WireModel]] subclasses never cross out of the API client.

## Credentials and Deployment Protection

Service options use a shared session registry key, resolve credentials with mode-specific precedence, and never read `VERCEL_TOKEN`.

[[src/vercel-board/vercel/board/_internal/options.py#BoardServiceOptions]] and [[src/vercel-board/vercel/board/_internal/options.py#SyncBoardServiceOptions]] share [[src/vercel-board/vercel/board/_internal/options.py#_BoardServiceOptionsKey]]. Sync factories are wrapped by [[src/vercel-board/vercel/board/_internal/options.py#adapt_sync_credentials_factory]], which rejects awaitable return values. [[src/vercel-board/vercel/board/_internal/options.py#BoardCredentials]] redacts tokens in `repr()` and `str()`.

- Hosted mode ([[src/vercel-board/vercel/board/_internal/api_client.py#_VercelBoardProtocol#_resolve_auth_and_team]]): `credentials_factory`, deployment OIDC (`VERCEL=1` or `VERCEL_OIDC_TOKEN`), `BOARD_TOKEN`, `VERCEL_AUTH_TOKEN`, GitHub Actions OIDC (restricted to [[src/vercel-board/vercel/board/_internal/options.py#github_token_allowed_origin]]), then local `vercel login` CLI auth. Team resolves from `board(team=...)`, `BoardServiceOptions(team=...)`, `VERCEL_ORG_ID`, OIDC `owner_id`, or nearest `.vercel/project.json` via [[src/vercel-board/vercel/board/_internal/options.py#find_vercel_project_org_id]].
- HTTP mode ([[src/vercel-board/vercel/board/_internal/api_client.py#_HttpBoardProtocol#_resolve_token]]): `credentials_factory`, `BOARD_TOKEN`, loopback `vb serve` token file ([[src/vercel-board/vercel/board/_internal/options.py#read_local_token]]), GitHub Actions OIDC, deployment OIDC (`VERCEL=1` only, exchanged for `discovery.audience`), bare request if the board has no Vercel project, then `VERCEL_AUTH_TOKEN` / `vercel login` exchanged and cached per `(audience, login)` via [[src/vercel-board/vercel/board/_internal/options.py#read_cached_token]].
- Deployment Protection ([[src/vercel-board/vercel/board/_internal/api_client.py#_HttpBoardProtocol#_send_guarded]]): outside Vercel, `VERCEL_AUTOMATION_BYPASS_SECRET` is sent directly; on Vercel (`VERCEL=1`), bypass is sent only to own deployment hosts ([[src/vercel-board/vercel/board/_internal/options.py#is_own_deployment_host]]) with a single fallback to Trusted Sources (`x-vercel-trusted-oidc-idp-token`). [[src/vercel-board/vercel/board/_internal/api_client.py#_is_deployment_protection_wall]] distinguishes protection walls (SSO redirects, HTML login pages, or 401 with `x-vercel-error`) from ordinary JSON board 401s and raises [[src/vercel-board/vercel/board/_internal/errors.py#BoardCredentialsError]] with `code="protected"`.

## Profiles and access narrowing

Handles carry optional profile identity and monotonic channel permission rules that serialize to request headers.

[[src/vercel-board/vercel/board/_internal/access.py#Can]] normalizes channel patterns (`*`, exact channels, and `prefix/` subtrees) with specificity precedence (exact beats longest subtree beats `*`) and closes [[src/vercel-board/vercel/board/_internal/access.py#Permission]] implications (`react`, `label`, and `sync` imply `read`; `manage` implies all; `post` implies none). [[src/vercel-board/vercel/board/_internal/access.py#Can#narrow]] computes the channel-by-channel intersection, and [[src/vercel-board/vercel/board/_internal/service.py#narrow_handle_can]] guarantees `as_profile(can=...)` never widens an existing scope.

[[src/vercel-board/vercel/board/_internal/service.py#resolve_effective_profile]] reads `BOARD_PROFILE` and `BOARD_AVATAR` only when the handle's profile is `UNSET`; calling `as_profile(None)` stores explicit `None` to suppress environment fallback.

## Errors

Board-specific failures inherit from [[src/vercel-board/vercel/board/_internal/errors.py#BoardError]] and tag their source with [[src/vercel-board/vercel/board/_internal/errors.py#ErrorOrigin]]; session lifecycle and mode-mismatch errors raise `vercel-internal-core` session errors.

- Client-side validation raises [[src/vercel-board/vercel/board/_internal/errors.py#BoardValidationError]] (also a `ValueError`). Credential/team resolution failures, Deployment Protection walls (`code="protected"`), project access failures (`code="no_access"`), and bare HTTP 401 rejections (`code="no_project"`) raise [[src/vercel-board/vercel/board/_internal/errors.py#BoardCredentialsError]]. Malformed responses raise [[src/vercel-board/vercel/board/_internal/errors.py#BoardResponseError]].
- Server error responses (including an ordinary JSON `401` with an explicit token) map to [[src/vercel-board/vercel/board/_internal/errors.py#BoardApiError]] and typed subclasses: [[src/vercel-board/vercel/board/_internal/errors.py#BoardNotFoundError]] (`LookupError`), [[src/vercel-board/vercel/board/_internal/errors.py#BoardPermissionError]] (`PermissionError`), [[src/vercel-board/vercel/board/_internal/errors.py#BoardRateLimitError]] (429 with `retry_after`), and [[src/vercel-board/vercel/board/_internal/errors.py#BoardConflictError]] (subclassed by [[src/vercel-board/vercel/board/_internal/errors.py#BoardRevisionConflictError]], [[src/vercel-board/vercel/board/_internal/errors.py#BoardLabelConflictError]], and [[src/vercel-board/vercel/board/_internal/errors.py#BoardNewerMessageError]]).
