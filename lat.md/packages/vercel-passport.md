# vercel-passport

Reads and verifies the visitor identity that Vercel Passport forwards to an app after authenticating the visitor at Vercel's edge.

Lives in [[src/vercel-passport]] and is exposed as [[src/vercel-passport/vercel/passport/__init__.py|vercel.passport]] (async) and [[src/vercel-passport/vercel/passport/sync.py|vercel.passport.sync]], which use identical names and arguments, following [[vercel-connect]]. It is a port of the TypeScript `@vercel/passport` package, as of version 1.2.0.

## Layering

All behavior except verification is synchronous and shared, so the two surfaces differ only in how they call the verifier.

[[src/vercel-passport/vercel/passport/_internal/resolve.py#resolve_identity]] decides which token `get_identity` uses, where it came from, and whether it must be verified, returning either an identity or a [[src/vercel-passport/vercel/passport/_internal/resolve.py#PendingVerification]]. [[src/vercel-passport/vercel/passport/_internal/resolve.py#resolve_token]] does the same for `verify_identity`. Each surface then calls [[src/vercel-passport/vercel/passport/_internal/resolve.py#verify_async]] or [[src/vercel-passport/vercel/passport/_internal/resolve.py#verify_sync]] and finishes with [[src/vercel-passport/vercel/passport/_internal/resolve.py#verified_identity]].

## Token sources

`get_identity` prefers the trusted `x-vercel-oidc-passport-token` header, then the `_vercel_passport` cookie, and always verifies a token taken from the request.

With no request argument, the headers come from [[vercel-headers]], which the Vercel Python runtime fills for every request it serves. A request may also be passed as a header mapping or any object exposing `headers`. Header names match case-insensitively even for a plain dict.

A token the caller supplies directly, as an argument or in `VERCEL_PASSPORT_TOKEN`, is decoded without verification unless `verify=True`, for local debugging. Unlike the TypeScript package, `allow_unverified` defaults to [[src/vercel-passport/vercel/passport/_internal/development.py#is_local_development]], so on a deployment such a token raises unless the caller opts in. `verify_identity` is the verified path for explicit tokens.

### Chunked cookies

A session token too large for one cookie is split by Vercel's proxy across several, and [[src/vercel-passport/vercel/passport/_internal/resolve.py#_cookie]] reassembles it.

The `_vercel_passport` cookie then holds a manifest with the chunk count and the SHA-256 of the joined token, and each chunk's cookie name carries the checksum's first 32 hex digits as a generation, so chunks of an older session never mix in. The count is bounded to 100 and checked before any chunk is read, and a missing chunk or mismatched checksum raises [[src/vercel-passport/vercel/passport/_internal/models.py#PassportIdentityError]] rather than a verification error.

The cookie is read only when no higher-priority token exists, as in the TypeScript package, so a corrupt chunked cookie cannot break a request that also carries a valid header or bearer token.

`require_identity` resolves exactly as `get_identity` does, then [[src/vercel-passport/vercel/passport/_internal/resolve.py#required_identity]] rejects a missing identity or one without a token, which excludes every local and development identity. It mirrors `requireIdentity` from the TypeScript package's 1.2.0 release.

`verify_identity` searches an `Authorization: Bearer` header, then the Passport header, then the cookie. It never reads the request context and never falls back to a local identity, so the token it verifies is always named explicitly.

## Verification

Passport tokens are verified by [[vercel-oidc#Verification]] with the issuer pinned to Passport instead of OIDC.

Passport tokens are signed by the same global key as other Vercel OIDC tokens, so they verify against the same pinned JWKS. [[src/vercel-oidc/vercel/oidc/verify.py#verify_vercel_oidc_token]] takes `issuer="passport"` to accept `https://passport.vercel.com` and its owner-scoped issuers instead. Because the signature alone cannot tell the two token kinds apart, [[src/vercel-passport/vercel/passport/_internal/payload.py#build_identity]] also requires the Passport shape (`typ`, `sub`, `owner`, `connector_id`, and `external_sub`) on every identity, verified or not.

Time claims use `vercel-oidc`'s 60-second leeway ([[src/vercel-oidc/vercel/oidc/verify.py#DEFAULT_LEEWAY]]). Zero leeway would match `jose` on `exp` and `nbf`, but PyJWT also rejects a future `iat`, which `jose` never checks, so an edge clock even a second ahead would reject fresh tokens. A token claiming to be issued more than 60 seconds in the future is still rejected, unlike in the TypeScript package.

Unlike the TypeScript package, `"*"` is not a wildcard for the expected project or environment, and lists of accepted values are not supported. Expectations compare by equality, as everywhere else in [[vercel-oidc]], so a backend cannot accidentally accept tokens minted for any project. Only `audience` is exposed from `jose`'s verify options.

## Local identities

When the request carries no token, local development can still have an identity: one set for the context, then `VERCEL_PASSPORT_IDENTITY`, then an opt-in synthesized one.

[[src/vercel-passport/vercel/passport/_internal/development.py#local_claims]] resolves that order, and only while [[src/vercel-passport/vercel/passport/_internal/development.py#is_local_development]] holds, which excludes every Vercel deployment except `VERCEL_ENV=development`. None of these identities is verified.

[[src/vercel-passport/vercel/passport/_internal/development.py#local_identity]] sets the context's identity in a context variable, the pattern [[vercel-headers]] uses, so tests and local apps set it in code rather than through `get_identity` arguments, and it reaches tasks started inside the block and threads that copy the context, such as `anyio.to_thread`, though not a plain `threading.Thread`. It validates the claims on entry, so a malformed identity fails where it is set.

The synthesized identity is opt-in with `VERCEL_PASSPORT_DEV=1`, where the TypeScript package synthesizes one by default. A default-on identity authenticates every request in any process that merely is not on Vercel, such as a self-hosted server without `VERCEL_ENV`. It emits a warning once per process, attributed to the caller's line; an identity set in code does not.

[[src/vercel-passport/vercel/passport/_internal/models.py#PassportDevelopmentIdentity]] states its own precedence: each field is the explicit value, then its `VERCEL_PASSPORT_DEV_*` variable, then the default, applied by a before-validator, so every way of building one resolves alike. An explicit `None` clears an optional field that its variable sets, which the TypeScript package cannot express. The issuer and audience defaults derive from the resolved owner, project, and environment.

## Identity values

Identities are frozen pydantic models on [[src/vercel-passport/vercel/passport/_internal/models.py#PassportModel]], following `ConnectModel` in [[vercel-connect]], so they pickle, deep-copy, and serialize with `model_dump`, which leaves out the token.

The base forbids unknown fields and reports failures from construction, `model_validate`, `model_validate_json`, assignment, and deletion as [[src/vercel-passport/vercel/passport/_internal/models.py#PassportValidationError]], so callers never import pydantic, and the message never echoes the input, which could hold a token. The fields cannot be reassigned, but `payload` is a deep copy of the claims and mutable in place: a read-only mapping cannot be pickled or copied. So [[src/vercel-passport/vercel/passport/_internal/models.py#PassportIdentity]] leaves `payload` out of both equality and the hash, and an identity in a set or dict stays findable after its payload changes.

The token is excluded from `model_dump` as well as the repr. The Passport header is added by Vercel's edge, so the visitor's browser never otherwise sees it, and an endpoint returning an identity must not hand it a bearer credential.

An identity has a [[src/vercel-passport/vercel/passport/_internal/models.py#PassportProject]] only when its claims name a project ID, which every verified token does because verification requires it to match. The TypeScript package builds a project from either the ID or the name. [[src/vercel-passport/vercel/passport/_internal/models.py#PassportIdentity]] leaves `token` and `payload` out of its repr, so logging an identity never writes a bearer credential. Verification failures raise [[src/vercel-passport/vercel/passport/_internal/models.py#PassportVerificationError]], a subclass of [[src/vercel-passport/vercel/passport/_internal/models.py#PassportIdentityError]], so one `except` handles every rejection.
