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
