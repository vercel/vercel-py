"""
Example: List sandboxes after concurrent creation.

This example demonstrates how to:
1. Create several sandboxes concurrently with ``AsyncSandbox.create()``
2. Exercise the async pager APIs, including direct async iteration
3. Exercise the sync page APIs

The list API returns typed pages. Use a small ``limit`` to paginate through
the recent results and inspect only the first few pages.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox, Sandbox

load_dotenv()

SANDBOX_COUNT = 5
PAGE_SIZE = 2
MAX_PAGES = 3
TIMEOUT_MS = 120_000
MAX_ITEMS = PAGE_SIZE * MAX_PAGES
PROJECT_ID = os.environ["VERCEL_PROJECT_ID"]


def _print_page(prefix: str, page_number: int, sandbox_ids: list[str]) -> None:
    print(f"{prefix} page {page_number}: {sandbox_ids}")


def _print_page_state(prefix: str, page) -> None:
    next_page_info = page.next_page_info()
    next_until = None if next_page_info is None else next_page_info.until
    print(
        f"{prefix} has_next={page.has_next_page()} "
        f"count={page.pagination.count} next_until={next_until}"
    )


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

    print("\n[1] Await the pager to get the first page")
    pager = AsyncSandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    )
    first_page = await pager
    _print_page_state("async first page:", first_page)
    _print_page("async", 1, [sandbox.id for sandbox in first_page.sandboxes])

    print("\n[2] Fetch the next page explicitly with get_next_page()")
    next_page = await first_page.get_next_page()
    if next_page is None:
        print("async next page: none")
    else:
        _print_page_state("async next page:", next_page)
        _print_page("async", 2, [sandbox.id for sandbox in next_page.sandboxes])

    print("\n[3] Iterate pages from the pager with iter_pages()")
    paged_ids: list[str] = []
    page_number = 0
    async for page in AsyncSandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    ).iter_pages():
        page_number += 1
        page_ids = [sandbox.id for sandbox in page.sandboxes]
        _print_page("async iter_pages", page_number, page_ids)
        paged_ids.extend(page_ids)
        if page_number >= MAX_PAGES:
            break
    _summarize_created("async iter_pages", paged_ids, created_ids)

    print("\n[4] Iterate items from the first page with page.iter_items()")
    iter_item_ids: list[str] = []
    async for sandbox in first_page.iter_items():
        iter_item_ids.append(sandbox.id)
        if len(iter_item_ids) >= MAX_ITEMS:
            break
    print(f"async iter_items: {iter_item_ids}")
    _summarize_created("async iter_items", iter_item_ids, created_ids)

    print("\n[5] Iterate items from the pager with pager.iter_items()")
    pager_item_ids: list[str] = []
    async for sandbox in AsyncSandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    ).iter_items():
        pager_item_ids.append(sandbox.id)
        if len(pager_item_ids) >= MAX_ITEMS:
            break
    print(f"async pager.iter_items: {pager_item_ids}")
    _summarize_created("async pager.iter_items", pager_item_ids, created_ids)

    print("\n[6] Iterate items directly from the pager")
    direct_item_ids: list[str] = []
    async for sandbox in AsyncSandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    ):
        direct_item_ids.append(sandbox.id)
        if len(direct_item_ids) >= MAX_ITEMS:
            break
    print(f"async direct iteration: {direct_item_ids}")
    _summarize_created("async direct iteration", direct_item_ids, created_ids)

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

    print("\n[2] Fetch the next page explicitly with get_next_page()")
    next_page = first_page.get_next_page()
    if next_page is None:
        print("sync next page: none")
    else:
        _print_page_state("sync next page:", next_page)
        _print_page("sync", 2, [sandbox.id for sandbox in next_page.sandboxes])

    print("\n[3] Iterate pages with iter_pages()")
    page_number = 0
    for current_page in Sandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    ).iter_pages():
        page_number += 1
        _print_page(
            "sync iter_pages", page_number, [sandbox.id for sandbox in current_page.sandboxes]
        )
        if page_number >= MAX_PAGES:
            break

    print("\n[4] Iterate items with iter_items()")
    iter_item_ids: list[str] = []
    for sandbox in Sandbox.list(
        project_id=PROJECT_ID,
        limit=PAGE_SIZE,
        since=since,
    ).iter_items():
        iter_item_ids.append(sandbox.id)
        if len(iter_item_ids) >= MAX_ITEMS:
            break
    print(f"sync iter_items: {iter_item_ids}")


if __name__ == "__main__":
    since = datetime.now(UTC) - timedelta(seconds=5)
    sandboxes = asyncio.run(async_demo(since))
    try:
        sync_demo(since)
    finally:
        asyncio.run(cleanup(sandboxes))
