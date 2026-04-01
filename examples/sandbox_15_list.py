"""
Example: List sandboxes after concurrent creation.

This example demonstrates how to:
1. Create several sandboxes concurrently with ``AsyncSandbox.create()``
2. Fetch sandbox pages explicitly with ``list()`` and ``get_next_page()``
3. Iterate directly over the sandboxes owned by each page

The list API returns typed pages. Use a small ``limit`` to paginate through
the recent results one page at a time.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox, Sandbox

load_dotenv()

SANDBOX_COUNT = 5
PAGE_SIZE = 2
MAX_PAGES = 3
TIMEOUT_MS = 120_000
PROJECT_ID = os.environ["VERCEL_PROJECT_ID"]


def _print_page(prefix: str, page_number: int, sandbox_ids: list[str]) -> None:
    print(f"{prefix} page {page_number}: {sandbox_ids}")


def _print_page_state(prefix: str, page) -> None:
    print(f"{prefix} count={page.pagination.count} next_until={page.pagination.next}")


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

    print("\n[1] Await the first page")
    first_page = await AsyncSandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    )
    _print_page_state("async first page:", first_page)
    _print_page("async", 1, [sandbox.id for sandbox in first_page.sandboxes])

    print("\n[2] Iterate the sandboxes in the first page")
    current_page_ids = [sandbox.id for sandbox in first_page]
    print(f"async current page iteration: {current_page_ids}")
    _summarize_created("async current page iteration", current_page_ids, created_ids)

    print("\n[3] Walk forward with get_next_page()")
    paged_ids: list[str] = []
    page_number = 1
    page = first_page
    while True:
        page_ids = [sandbox.id for sandbox in page]
        _print_page("async paged", page_number, page_ids)
        paged_ids.extend(page_ids)
        if page_number >= MAX_PAGES:
            break
        next_page = await page.get_next_page()
        if next_page is None:
            break
        page_number += 1
        page = next_page
    _summarize_created("async paged", paged_ids, created_ids)

    return sandboxes


async def cleanup(sandboxes: list[AsyncSandbox]) -> None:
    await asyncio.gather(*(sandbox.stop() for sandbox in sandboxes), return_exceptions=True)
    await asyncio.gather(*(sandbox.client.aclose() for sandbox in sandboxes))


def sync_demo(since: datetime) -> None:
    print("\n" + "=" * 60)
    print("SYNC SANDBOX LIST EXAMPLE")
    print("=" * 60)

    print("\n[1] Get the first page")
    first_page = Sandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    )
    _print_page_state("sync first page:", first_page)
    _print_page("sync", 1, [sandbox.id for sandbox in first_page.sandboxes])

    print("\n[2] Iterate the sandboxes in the first page")
    current_page_ids = [sandbox.id for sandbox in first_page]
    print(f"sync current page iteration: {current_page_ids}")

    print("\n[3] Walk forward with get_next_page()")
    page_number = 1
    page = first_page
    while True:
        _print_page("sync paged", page_number, [sandbox.id for sandbox in page])
        if page_number >= MAX_PAGES:
            break
        next_page = page.get_next_page()
        if next_page is None:
            break
        page_number += 1
        page = next_page


if __name__ == "__main__":
    since = datetime.now(timezone.utc) - timedelta(seconds=5)
    sandboxes = asyncio.run(async_demo(since))
    try:
        sync_demo(since)
    finally:
        asyncio.run(cleanup(sandboxes))
