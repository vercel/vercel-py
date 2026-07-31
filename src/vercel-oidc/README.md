# OIDC

`vercel.oidc` retrieves and decodes Vercel OIDC tokens.

## Async Token Lookup

```python
from vercel.oidc import decode_oidc_payload
from vercel.headers import set_headers
from vercel.oidc.aio import get_vercel_oidc_token


async def main() -> None:
    token = await get_vercel_oidc_token()
    payload = decode_oidc_payload(token)
    project_id = payload.get("project_id")
```

Token lookup prefers the `x-vercel-oidc-token` request header registered with
`vercel.headers.set_headers()`, then `VERCEL_OIDC_TOKEN`. The compatibility
alias `vercel.oidc.set_headers()` updates the same header context. In local development,
you can load a short-lived token dynamically:

```bash
VERCEL_OIDC_TOKEN=$(vc project token some-project) some-command
```

Use `vercel.oidc.get_vercel_oidc_token()` for synchronous code.

## Verification

Requires the `verify` extra, which adds `pyjwt[crypto]`:

```bash
pip install "vercel-oidc[verify]"
```

```python
from vercel.oidc import extract_bearer_token, verify_vercel_oidc_token

claims = verify_vercel_oidc_token(extract_bearer_token(request.headers))
```

Verification allows only RS256, resolves the signing key by `kid` from Vercel's
JWKS, and pins the issuer to Vercel's OIDC service, which mints both
`https://oidc.vercel.com` and the team-scoped `https://oidc.vercel.com/<team>`.
It **fails closed**: when the expected project or environment cannot be resolved
from the arguments, `VERCEL_PROJECT_ID`, or `VERCEL_TARGET_ENV`/`VERCEL_ENV`,
every token is rejected. `vercel.oidc.aio.verify_vercel_oidc_token()` is the
async twin.

## Token Identity

A token is a signature over an identity plus an expiry, so one identity is issued
many tokens over time. To key client-side state on the identity rather than on
the token, use:

```python
from vercel.oidc import verify_vercel_oidc_token_identity

identity = verify_vercel_oidc_token_identity(token)  # stable across a refresh
```

The signature, issuer and expiry are verified, and `iss`, `aud` and `sub` are
reduced to an opaque digest that carries no credential and is safe to log. This
is **not** an authorization check — it returns no claims and does not check the
project, environment, owner or audience. Use `verify_vercel_oidc_token()` for
that.
