# vercel-oidc

Resolves the Vercel OIDC token for outbound calls, combines it into credentials, and verifies inbound OIDC tokens.

Lives in [[src/vercel-oidc]] and is exposed as [[src/vercel-oidc/vercel/oidc/__init__.py|vercel.oidc]], with async twins in [[src/vercel-oidc/vercel/oidc/aio.py|vercel.oidc.aio]]. The resolution order shared by every service is summarized in [[runtime#OIDC credentials]].

## Token resolution

Lookup prefers ambient sources and refreshes from the Vercel CLI only in a linked local project.

[[src/vercel-oidc/vercel/oidc/token.py#get_vercel_oidc_token_from_context]] checks the request header, then `VERCEL_OIDC_TOKEN`, then the file named by `VERCEL_OIDC_TOKEN_FILE`. A header token is cached and reused while it is unexpired, so a later call in a different context still finds a token.

[[src/vercel-oidc/vercel/oidc/token.py#get_vercel_oidc_token]] goes further when the ambient token is missing or expired. If a `.vercel` project link exists, it exchanges the CLI's login token for a fresh OIDC token, caches it on disk per project, and exports it as `VERCEL_OIDC_TOKEN`. Without a project link it fails with an actionable error instead of guessing.

The file source is re-read shortly before the cached token expires and backs off after a failed read, so a rotating file does not cause a read on every call.

## Credentials

Services that need project scope get a token plus project and team IDs from one function.

[[src/vercel-oidc/vercel/oidc/credentials.py#get_credentials]] takes explicit values first. With an OIDC token, it reads `VERCEL_PROJECT_ID` and `VERCEL_TEAM_ID`, falling back to the token's `project_id` and `owner_id` claims. Without one, it requires `VERCEL_TOKEN` and both IDs.

## Verification

Inbound token verification is deliberately narrow, because the known JWT failures in this space are algorithm or key confusion bugs.

[[src/vercel-oidc/vercel/oidc/verify.py#verify_vercel_oidc_token]] accepts only RS256, pins the issuer to Vercel's OIDC service (including team-scoped issuers), and resolves keys by `kid` from a constant JWKS URL. Claims are checked only after the signature verifies. The JWKS is cached with a rate-limited refetch, so an attacker cycling unknown `kid` values cannot force a fetch per request.

It fails closed: if the expected project or environment cannot be resolved from arguments, `VERCEL_PROJECT_ID`, or `VERCEL_TARGET_ENV`/`VERCEL_ENV`, every token is rejected. Verification needs the `verify` extra, and `cryptography` is never imported without it.

[[src/vercel-oidc/vercel/oidc/verify.py#resolve_vercel_oidc_token_identity]] verifies a token and reduces its identity claims to an opaque digest, so callers can key state on an identity that stays stable across token refreshes. It does not authorize; [[vercel-connect]] uses it for cache keys.
