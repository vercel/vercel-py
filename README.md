# Vercel Python SDK

Python SDK for Vercel APIs and Vercel Functions.

For product-level behavior, limits, and platform setup, use the official
[Vercel docs](https://vercel.com/docs).

## Getting Started

Install the package:

```bash
pip install vercel
```

Or with uv:

```bash
uv add vercel
```

Configure the credentials via environment variables based on your app's needs:

- `BLOB_READ_WRITE_TOKEN` for Vercel Blob
- `VERCEL_TOKEN`, `VERCEL_PROJECT_ID`, and `VERCEL_TEAM_ID` for Vercel API
  clients
- `VERCEL_OIDC_TOKEN` for local OIDC testing. On Vercel, OIDC helpers can read
  the request token after you register request headers with
  `vercel.headers.set_headers()`.

For local OIDC development, you can load a short-lived token dynamically with
the Vercel CLI:

```bash
VERCEL_OIDC_TOKEN=$(vc project token some-project) some-command
```

## Usage

```python
import asyncio

from vercel.client import AsyncVercel
from vercel.oidc.credentials import get_credentials


async def main() -> None:
    credentials = get_credentials()
    vercel = AsyncVercel(access_token=credentials.token)
    deployment = await vercel.deployments.create_deployment(
        body={
            "name": "hello-python",
            "project": "hello-python",
            "target": "preview",
            "files": [
                {
                    "file": "index.html",
                    "data": "<h1>Hello from Python</h1>",
                }
            ],
        }
    )

    print(f"Deployment created: https://{deployment['url']}")


asyncio.run(main())
```

## Public API Vs Internals

Import from public modules under `vercel.*`, such as `vercel.blob`,
`vercel.cache`, `vercel.headers`, `vercel.oidc`, `vercel.projects`, and
`vercel.sandbox`. Modules under `vercel._internal.*` are implementation details
and may change without public API guarantees.

Sync counterparts are available for the main client classes and module-level
helpers when you are not running an async application.
