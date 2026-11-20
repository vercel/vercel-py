# vercel-drives

Python SDK for Vercel Drives: named persistent filesystems that belong to one
project and one region, and mount into sandboxes created with
[`vercel-sandbox`](https://pypi.org/project/vercel-sandbox/).

```sh
pip install vercel-drives
```

## Manage Drives

```python
import asyncio

from vercel import drives


async def main() -> None:
    cache, created = await drives.get_or_create_drive(
        name="cache",
        region="sfo1",
        max_size_bytes=10 * 1024**3,
    )
    print(cache.id, cache.region, created)

    # Look up an existing Drive by project-local name or Drive ID.
    cache = await drives.get_drive(name_or_id="cache")

    async for item in drives.query_drives(query=drives.DriveQueryByName(name_prefix="cache")):
        print(item.name, item.max_size_bytes)

    await cache.delete()


asyncio.run(main())
```

`get_or_create_drive` omits `region` by default, leaving placement to the API.
Pass `CURRENT_REGION` to use the current Vercel Function's region.

A missing Drive raises `DriveApiError` with `status_code == 404`. A Drive must
be unmounted before it can be deleted.

## Fork a Drive

A fork copies a Drive's committed data into an independent Drive, inheriting
its region and maximum size.

```python
import asyncio

from vercel import drives


async def main() -> None:
    cache = await drives.get_drive(name_or_id="cache")
    forked = await cache.fork(name="cache-fork")
    print(forked.parent_drive_id, forked.root_drive_id)

    # Or fork by name, without a lookup.
    other = await drives.fork_drive(source="cache", name="cache-fork-2")


asyncio.run(main())
```

## Mount a Drive in a sandbox

`vercel-sandbox` accepts Drive handles, Drive names, and `DriveMount` values as
mounts. A mount is read-write unless it is a snapshot.

```python
import asyncio

from vercel import drives, sandbox


async def main() -> None:
    cache, _ = await drives.get_or_create_drive(name="cache")
    async with sandbox.create_sandbox(
        region=cache.region,
        mounts={
            "/cache": cache,
            # A point-in-time, read-only view of a Drive.
            "/readonly": drives.DriveMount("shared-source", mode="snapshot"),
        },
    ) as box:
        await box.fs.write_text("/cache/message.txt", "hello\n")


asyncio.run(main())
```

## Synchronous API

`vercel.drives.sync` mirrors every function without `await` and returns
`SyncDrive` handles.

```python
from vercel.drives import sync as drives

cache, _ = drives.get_or_create_drive(name="cache")
for item in drives.query_drives():
    print(item.name)
```

## Credentials

Requests use the Vercel OIDC token of the deployment, or `VERCEL_TOKEN`,
`VERCEL_TEAM_ID`, and `VERCEL_PROJECT_ID`. Locally, `vercel env pull` provides
an OIDC token for a linked project. Drives always belong to the project the
credentials resolve to.

To supply credentials yourself, configure the session:

```python
import asyncio

from vercel import drives
from vercel.api import session
from vercel.drives import DriveCredentials, DriveServiceOptions


async def credentials() -> DriveCredentials:
    return DriveCredentials(token="...", team_id="team_...", project_id="prj_...")


async def main() -> None:
    options = DriveServiceOptions(credentials_factory=credentials)
    async with session(service_options=[options]):
        async for item in drives.query_drives():
            print(item.name)


asyncio.run(main())
```
