# Projects

`vercel.projects` provides helpers for listing, creating, updating, and deleting
Vercel projects.

For project request bodies and product behavior, see the official
[Vercel projects docs](https://vercel.com/docs).

## Credentials

Grouped project clients use `VERCEL_TOKEN` by default. Pass
`access_token=get_credentials().token` or another explicit token to `Vercel` /
`AsyncVercel` when you want to authenticate with OIDC-derived credentials.

## Async Client

```python
from vercel.client import AsyncVercel


async def main() -> None:
    vercel = AsyncVercel()
    listing = await vercel.projects.get_projects(query={"limit": 10})
    created = await vercel.projects.create_project(body={"name": "my-site"})
    updated = await vercel.projects.update_project(
        id_or_name=created["id"],
        body={"name": "my-renamed-site"},
    )
    await vercel.projects.delete_project(id_or_name=updated["id"])
```

Use sync functions in `vercel.projects` or `Vercel().projects` for synchronous
code.
