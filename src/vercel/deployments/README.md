# Deployments

`vercel.deployments` provides helpers for Vercel deployment creation and
deployment file uploads.

For deployment request bodies and product behavior, see the official
[Vercel deployments docs](https://vercel.com/docs).

## Async Client

```python
from vercel.client import AsyncVercel


async def main() -> None:
    vercel = AsyncVercel()
    result = await vercel.deployments.create_deployment(
        body={
            "name": "my-site",
            "project": "my-site",
            "target": "preview",
            "files": [],
        }
    )
```

```python
from vercel.client import AsyncVercel


async def upload(content: bytes, digest: str) -> dict:
    vercel = AsyncVercel()
    return await vercel.deployments.upload_file(
        content=content,
        content_length=len(content),
        x_vercel_digest=digest,
    )
```

Use sync functions in `vercel.deployments` or `Vercel().deployments` for
synchronous code.
