"""
Example: List sandboxes after concurrent creation.

This example demonstrates how to:
1. Create several sandboxes concurrently with ``AsyncSandbox.create()``
2. Iterate recent sandboxes directly with ``AsyncSandbox.list()``
3. Iterate recent sandboxes directly with ``Sandbox.list()``

The list API is item-first. Use ``limit`` as the maximum number of sandboxes
you want the iterator to yield across all internally fetched pages.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox, Sandbox

load_dotenv()

SANDBOX_COUNT = 5
LIST_LIMIT = 4
TIMEOUT_MS = 120_000
PROJECT_ID = os.environ["VERCEL_PROJECT_ID"]


def _summarize_created(prefix: str, sandbox_ids: list[str], created_ids: set[str]) -> None:
    seen_created = sorted(set(sandbox_ids) & created_ids)
    print(f"{prefix} created sandboxes seen: {seen_created}")


async def _create_sandbox(index: int) -> AsyncSandbox:
    sandbox = await AsyncSandbox.create(
        project_id=PROJECT_ID,
        timeout=TIMEOUT_MS,
        env={"LIST_EXAMPLE_INDEX": str(index)},
    )
    print(f"Created sandbox {index}: {sandbox.sandbox_id}")
    return sandbox


async def async_demo(since: datetime) -> list[AsyncSandbox]:
    print("=" * 60)
    print("ASYNC SANDBOX LIST EXAMPLE")
    print("=" * 60)

    sandboxes = await asyncio.gather(*(_create_sandbox(index) for index in range(SANDBOX_COUNT)))
    created_ids = {sandbox.sandbox_id for sandbox in sandboxes}
    print(f"Created {len(created_ids)} sandboxes concurrently")

    print(f"\n[1] List recent sandboxes asynchronously (limit={LIST_LIMIT} total items)")
    async_ids: list[str] = []
    async for sandbox in AsyncSandbox.list(
        project_id=PROJECT_ID,
        limit=LIST_LIMIT,
        since=since,
    ):
        async_ids.append(sandbox.id)
        print(f"async sandbox: {sandbox.id}")
    print(f"async yielded {len(async_ids)} sandboxes: {async_ids}")
    _summarize_created("async list", async_ids, created_ids)

    return sandboxes


async def cleanup(sandboxes: list[AsyncSandbox]) -> None:
    await asyncio.gather(*(sandbox.stop() for sandbox in sandboxes), return_exceptions=True)
    await asyncio.gather(*(sandbox.client.aclose() for sandbox in sandboxes))


def sync_demo(since: datetime) -> None:
    print("\n" + "=" * 60)
    print("SYNC SANDBOX LIST EXAMPLE")
    print("=" * 60)

    print(f"\n[1] List recent sandboxes synchronously (limit={LIST_LIMIT} total items)")
    sync_ids: list[str] = []
    for sandbox in Sandbox.list(
        project_id=PROJECT_ID,
        limit=LIST_LIMIT,
        since=since,
    ):
        sync_ids.append(sandbox.id)
        print(f"sync sandbox: {sandbox.id}")
    print(f"sync yielded {len(sync_ids)} sandboxes: {sync_ids}")


if __name__ == "__main__":
    since = datetime.now(UTC) - timedelta(seconds=5)
    sandboxes = asyncio.run(async_demo(since))
    try:
        sync_demo(since)
    finally:
        asyncio.run(cleanup(sandboxes))
