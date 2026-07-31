# vercel-connect

Python SDK for [Vercel Connect](https://vercel.com/docs), a credential broker for
third-party APIs.

You exchange your deployment's token for a short-lived credential for an upstream
service. Your project never stores provider secrets, and Connect owns the OAuth
client, PKCE, refresh, and revocation server-side.

```sh
pip install vercel-connect
```

## Usage

```python
import httpx
from vercel.connect import ConnectAppTokenSubject, get_token

token = await get_token("github/my-app", subject=ConnectAppTokenSubject())

async with httpx.AsyncClient() as client:
    await client.get(
        "https://api.github.com/user/repos",
        headers={"Authorization": f"Bearer {token}"},
    )
```

The same surface is available synchronously, with identical names and arguments:

```python
from vercel.connect.sync import ConnectAppTokenSubject, get_token

token = get_token("github/my-app", subject=ConnectAppTokenSubject())
```

Use a plain `with` block and `vercel.connect.sync` together; mixing an async call
into a sync session, or the reverse, is rejected.

## Subjects

Whose authority the credential carries:

| Subject | Authority | Needs |
| --- | --- | --- |
| `ConnectAppTokenSubject()` | The integration itself | An installation |
| `ConnectUserTokenSubject(id=...)` | One named end user | That user's consent |
| `ConnectJwtBearerTokenSubject(sub=...)` | A user asserted by your app | Pre-established trust |
| `ConnectTokenExchangeSubject(token=...)` | A credential you already hold | The inbound token |

`app` is one shared credential per installation: simple, always available, but
ambient authority. `user` preserves the provider's own permission model per
person and names them in the provider's audit log, at the cost of a consent flow.

Subjects are typed values rather than plain strings because three of the four
carry their own fields, so `subject="user"` could not say *which* user:

```python
ConnectUserTokenSubject(id="u_123", issuer="https://idp.example.com")
ConnectJwtBearerTokenSubject(sub="u_123", additional_claims={"tenant": "acme"})
```

## Value types

Every type on this surface is a frozen Pydantic model, so you get validation on
construction, autocompletion, exact `match`/`case` narrowing, `model_dump()` for
logging, and immutability, which means a subject cannot be mutated after a
credential has been cached against it:

```python
detail = ConnectGitHubAppInstallationAuthorizationDetail(permissions=["contents:read"])
detail.model_dump()                    # {'org': None, 'permissions': ('contents:read',), ...}
detail.permissions = ["admin"]         # ConnectValidationError: frozen
```

Construction is by keyword, a misspelled field is an error rather than a silently
dropped value, and every rejection raises `ConnectValidationError`, so you never
need to catch Pydantic's own error type. Containers of strings accept any
container and store a tuple; a bare string is rejected rather than expanded into
one entry per character:

```python
get_token(..., scopes="repo:read")     # ConnectValidationError, and a type error
get_token(..., scopes=["repo:read"])   # correct
```

## Authorization as control flow

The two "required" errors are not bugs, they are states with a remedy:

```python
from vercel.connect import (
    ConnectUserTokenSubject,
    UserAuthorizationRequiredError,
    get_token,
    start_authorization,
)

subject = ConnectUserTokenSubject(id=user_id)
try:
    token = await get_token("linear/my-app", subject=subject)
except UserAuthorizationRequiredError:
    authorization = await start_authorization(
        "linear/my-app", subject=subject, return_url="https://myapp.com/cb"
    )
    return redirect(authorization.url)
```

## Inbound triggers

A connector with triggers enabled forwards provider webhooks to your project with
a Vercel OIDC token attached, so you verify one thing instead of a different
signature scheme per provider:

```python
from vercel.connect import verify_connect_webhook

claims = await verify_connect_webhook(request.headers)
```

Verification pins the issuer to Vercel's OIDC service, accepting both
`https://oidc.vercel.com` and the team-scoped `https://oidc.vercel.com/<team>`,
allows only RS256, and fails closed when the expected project and environment cannot be resolved. It
accepts any valid Vercel OIDC token for this project and environment; it is not
pinned to a specific connector or deployment.

## Configuration

To configure advanced options, use a `session` context manager, and pass
`ConnectServiceOptions`:

```python
from vercel.api import session
from vercel.connect import ConnectAppTokenSubject, ConnectServiceOptions, get_token

async with session(
    service_options=[ConnectServiceOptions(base_url="https://staging.example.com")]
):
    token = await get_token("github/my-app", subject=ConnectAppTokenSubject())
```

## Local development

On Vercel the OIDC token is injected automatically. Locally:

```sh
vercel link
vercel env pull    # writes VERCEL_OIDC_TOKEN into .env.local
```

The connector must be attached to your project and enabled for the target
environment, or every call fails.
