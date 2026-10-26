# Passport

`vercel.passport` reads and verifies [Vercel Passport](https://vercel.com/docs)
identities in Python functions.

```bash
uv add vercel-passport
```

## Read the current visitor

```python
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from vercel.passport import get_identity

app = FastAPI()


@app.get("/api/me")
async def me():
    identity = await get_identity()
    if identity is None:
        return JSONResponse({"authenticated": False}, status_code=401)

    return {
        "authenticated": True,
        "external_subject": identity.external_subject,
        "subject": identity.subject,
    }
```

`get_identity()` reads the `x-vercel-oidc-passport-token` header that Vercel adds
after Passport authenticates the visitor, falling back to the `_vercel_passport`
cookie. A session too large for one cookie is split across several, and is
reassembled and checksummed before it is verified. With no arguments, it reads
the headers the Vercel Python runtime records for the current request. You can
also pass the request, or its headers, explicitly:

```python
identity = await get_identity(request)
identity = await get_identity(request.headers, cookies=request.cookies)
```

A token from the request is always verified against Vercel's OIDC JWKS. Only the
Passport issuer, `https://passport.vercel.com/<owner>`, is accepted, and the
token must have the Passport shape, so a regular Vercel OIDC token is never
accepted as a Passport identity.

By default the token must match this deployment's project and environment, read
from `VERCEL_PROJECT_ID` and `VERCEL_TARGET_ENV` (or `VERCEL_ENV`). If neither can
be determined, verification fails closed. Pass `project_id`, `environment`,
`owner_id`, or `audience` to set the expectations explicitly. Time claims allow
60 seconds of clock skew.

### Differences from `@vercel/passport`

Expectations compare by equality, so a backend cannot accidentally accept tokens
minted for any project:

- `"*"` is not a wildcard for `project_id` or `environment`; it matches only a
  token whose claim is literally `"*"`.
- Lists of accepted projects or environments are not supported. Call
  `verify_identity()` once per expected project instead.
- Of `jose`'s verify options, only `audience` is exposed, and the clock-skew
  allowance is fixed at 60 seconds. That allowance also applies to `iat`: a token
  claiming to be issued more than 60 seconds in the future is rejected, where
  `jose` does not check `iat` at all.
- An unverified debugging token is accepted only in local development by default.
  See [Local development](#local-development).
- `identity.project` is set only when the token names a project ID, which every
  verified token does, so `identity.project.id` is never `None`.
- In `PassportDevelopmentIdentity`, an explicit `None` clears a field its
  `VERCEL_PASSPORT_DEV_*` variable sets.

Identities are frozen Pydantic models. Serialize one with `identity.model_dump()`
or `identity.model_dump_json()`; `token` and `payload` are left out of its repr so
logging an identity never writes the bearer token.

Use `require_identity()` when a route needs a real Passport token. It takes the
same arguments as `get_identity()`, but raises `PassportIdentityError` instead of
returning `None`, and also rejects an identity without a token, such as the
local development identity. The returned identity's `token` is always set:

```python
from vercel.passport import PassportIdentityError, require_identity

try:
    identity = await require_identity()
except PassportIdentityError:
    raise HTTPException(status_code=401)

forward_to_backend(identity.token)
```

For synchronous code, such as Flask or Django views, use `vercel.passport.sync`,
which has the same names and arguments:

```python
from vercel.passport.sync import get_identity

identity = get_identity()
```

## Verify forwarded tokens

Use `verify_identity()` when a Passport-protected app forwards its Passport token
to another backend. It accepts the token itself, or request headers carrying an
`Authorization: Bearer <token>` header:

```python
from fastapi import FastAPI, HTTPException, Request
from vercel.passport import PassportIdentityError, verify_identity

app = FastAPI()


@app.post("/api/orders")
async def orders(request: Request):
    try:
        identity = await verify_identity(
            request,
            owner_id="team_123",
            project_id="prj_123",
            environment="production",
        )
    except PassportIdentityError:
        raise HTTPException(status_code=401)

    return {"external_subject": identity.external_subject, "email": identity.email}
```

Unlike `get_identity()`, `verify_identity()` always verifies and never falls back
to a local identity. If the token was minted by a different Vercel project than
the backend verifying it, pass the source project's `project_id` and
`environment`.

## Local development

Local development usually has no Passport header or cookie, because Passport
creates them in Vercel's edge network. So locally, `get_identity()` returns a
synthesized test identity and emits a `PassportDevelopmentIdentityWarning` the
first time it does so.

The test identity uses:

```txt
external_subject: test-user
email: test-user@passport.local
name: Test User
```

Override individual fields with environment variables:

```bash
VERCEL_PASSPORT_DEV_OWNER=acme
VERCEL_PASSPORT_DEV_CONNECTOR_ID=scl_dev
VERCEL_PASSPORT_DEV_EXTERNAL_SUB=user_dev
VERCEL_PASSPORT_DEV_EXTERNAL_ISS=https://idp.example.com
VERCEL_PASSPORT_DEV_PROJECT=my-project
VERCEL_PASSPORT_DEV_PROJECT_ID=prj_local
```

or in code:

```python
from vercel.passport import PassportDevelopmentIdentity, get_identity

identity = await get_identity(
    development=PassportDevelopmentIdentity(owner="acme", external_subject="user_dev"),
)
```

Disable the test identity with `VERCEL_PASSPORT_DEV=0`, or:

```python
identity = await get_identity(development=False)
```

Or provide the full claims yourself, as a mapping or as JSON in
`VERCEL_PASSPORT_IDENTITY`:

```python
identity = await get_identity(
    local_identity={
        "typ": "passport",
        "iss": "https://passport.vercel.com/local",
        "owner": "local",
        "connector_id": "local",
        "external_sub": "local-user",
        "sub": "owner:local:connector:local:principal:local-user",
    },
)
```

A synthesized identity is never used on a Vercel deployment unless
`VERCEL_ENV=development`. Local identities are never verified, so
`identity.verified` is `False`.

To debug with a real token copied from a deployed request, pass it directly or
set `VERCEL_PASSPORT_TOKEN`. Such a token is decoded without verification
unless you pass `verify=True`:

```python
identity = await get_identity(token=copied_token)
```

Unverified tokens are accepted only in local development. On a Vercel
deployment, other than `VERCEL_ENV=development`, an unverified token raises
`PassportIdentityError`, including one left in `VERCEL_PASSPORT_TOKEN`. In
deployed code, use `verify_identity()` or pass `verify=True` to verify explicit
tokens. Pass `allow_unverified=True` only if you really mean to trust an unsigned
token.
