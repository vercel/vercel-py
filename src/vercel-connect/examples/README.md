# vercel-connect examples

Runnable scripts, one per public surface. They need a linked Vercel project and a
connector attached to it.

```sh
vercel link                       # creates .vercel/project.json
vercel env pull                   # writes VERCEL_OIDC_TOKEN into .env.local
vercel connect list               # find a connector
export CONNECTOR=slack/my-bot     # id (scl_...) or UID
```

Then run any of them:

```sh
uv run python src/vercel-connect/examples/connect_01_app_token.py
```

The OIDC token is short-lived; re-run `vercel env pull` when it expires.

| Example | Shows |
| --- | --- |
| `connect_01_app_token.py` | Mint an app credential and call the provider with it |
| `connect_02_user_authorization.py` | Consent flow, device code, and polling to completion |
| `connect_03_token_cache.py` | Cache hits, `force_refresh`, scoped eviction, single-flight |
| `connect_04_connector_metadata.py` | Connector capabilities, plus untyped fields in `extra` |
| `connect_05_sync_surface.py` | The same operations with no `await` |
| `connect_06_webhook_verification.py` | Verifying an inbound trigger, and failing closed |

Nothing here mutates state: no example revokes a grant or writes to a provider.
`connect_02` is the only one that requires human interaction.

These are documentation, not tests — they are not run in CI, because they need
live credentials. The behaviour they demonstrate is covered by
`src/vercel-connect/tests/`.
