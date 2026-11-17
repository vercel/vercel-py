# Vercel Platform

Python clients for the Vercel REST API, available sync and async.

```sh
pip install vercel-platform
```

## Modules

- `vercel.deployments` - deployment creation and deployment file upload helpers
- `vercel.projects` - project list, create, update, and delete helpers
- `vercel.project_routes` - project-level routing rule staging, publishing, and history
- `vercel.client` - grouped `Vercel` / `AsyncVercel` clients for the modules above

## Usage

```python
import asyncio

from vercel.client import AsyncVercel


async def main() -> None:
    vercel = AsyncVercel()  # reads VERCEL_TOKEN
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
