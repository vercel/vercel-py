"""
Example: Snapshots (Sync & Async)

This example demonstrates how to:
1. Create a sandbox and set it up with files/dependencies
2. Create a snapshot of that sandbox (saves its state)
3. Create a new sandbox FROM the snapshot (inherits the saved state)
4. Verify the new sandbox has the same files as the original
5. Delete the snapshot when done

Snapshots are useful for:
- Fast startup: Pre-install dependencies once, then create sandboxes instantly
- Templates: Create base environments that can be reused
- Cost efficiency: Avoid repetitive setup tasks
"""

import asyncio

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox, AsyncSnapshot, Sandbox, Snapshot

load_dotenv()

SNAPSHOT_EXPIRATION_MS = 86_400_000
SNAPSHOT_EXPIRATION_DRIFT_MS = 1_000


def assert_time_near(*, actual: int | None, expected: int, drift: int, label: str) -> None:
    assert actual is not None, f"{label} should report an expires_at timestamp"
    assert abs(actual - expected) <= drift, f"{label} should be within {drift}ms of {expected}"


def assert_snapshot_expires_at_expected_time(
    *, created_at: int, expires_at: int | None, label: str
) -> None:
    assert_time_near(
        actual=expires_at,
        expected=created_at + SNAPSHOT_EXPIRATION_MS,
        drift=SNAPSHOT_EXPIRATION_DRIFT_MS,
        label=label,
    )


async def async_demo() -> None:
    print("=" * 60)
    print("ASYNC SNAPSHOT EXAMPLE")
    print("=" * 60)

    async_snapshot_ids: list[str] = []

    # Step 1: Create a sandbox and set it up
    print("\n[1] Creating initial sandbox...")
    sandbox1 = await AsyncSandbox.create(timeout=120_000)
    try:
        print(f"    Sandbox ID: {sandbox1.sandbox_id}")

        # Write some files to simulate a "setup" step
        print("\n[2] Writing files to simulate environment setup...")
        await sandbox1.write_files(
            [
                {"path": "config.json", "content": b'{"version": "1.0", "env": "async"}'},
                {"path": "data/users.txt", "content": b"alice\nbob\ncharlie"},
            ]
        )

        config = await sandbox1.read_file("config.json")
        print(f"    Created config.json: {config.decode()}")

        # Step 2: Create a snapshot with an explicit expiration (this STOPS the sandbox)
        print("\n[3] Creating snapshot with expiration=86400000...")
        snapshot = await sandbox1.snapshot(expiration=SNAPSHOT_EXPIRATION_MS)
        async_snapshot_ids.append(snapshot.snapshot_id)
        print(f"    Snapshot ID: {snapshot.snapshot_id}")
        print(f"    Status: {snapshot.status}")
        print(f"    Created At: {snapshot.created_at}")
        print(f"    Expires At: {snapshot.expires_at}")
        print(f"    Sandbox status after snapshot: {sandbox1.status}")
        assert_snapshot_expires_at_expected_time(
            created_at=snapshot.created_at,
            expires_at=snapshot.expires_at,
            label="expiring async snapshot",
        )

    finally:
        await sandbox1.client.aclose()

    # Step 3: Create a NEW sandbox FROM the snapshot
    print("\n[4] Creating new sandbox from snapshot...")
    sandbox2 = await AsyncSandbox.create(
        timeout=120_000,
        source={"type": "snapshot", "snapshot_id": snapshot.snapshot_id},
    )
    try:
        print(f"    New Sandbox ID: {sandbox2.sandbox_id}")
        print(f"    Source Snapshot ID: {sandbox2.source_snapshot_id}")

        # Step 4: Verify the snapshot preserved our files
        print("\n[5] Verifying snapshot preserved files...")
        config2 = await sandbox2.read_file("config.json")
        users2 = await sandbox2.read_file("data/users.txt")

        print(f"    config.json: {config2.decode()}")
        print(f"    data/users.txt: {users2.decode()}")

        assert config2 == b'{"version": "1.0", "env": "async"}', "config.json mismatch!"
        assert users2 == b"alice\nbob\ncharlie", "users.txt mismatch!"
        print("\n✓ Async restore assertions passed!")

        # Create a second snapshot with expiration=0 to preserve it indefinitely.
        print("\n[6] Creating second snapshot with expiration=0...")
        persistent_snapshot = await sandbox2.snapshot(expiration=0)
        async_snapshot_ids.append(persistent_snapshot.snapshot_id)
        print(f"    Snapshot ID: {persistent_snapshot.snapshot_id}")
        print(f"    Created At: {persistent_snapshot.created_at}")
        print(f"    Expires At: {persistent_snapshot.expires_at}")
        assert persistent_snapshot.expires_at is None, (
            "persistent async snapshot should not have an expires_at timestamp"
        )

    finally:
        await sandbox2.client.aclose()

    # Step 5: List snapshots and confirm the new snapshots are discoverable.
    print("\n[7] Listing recent snapshots...")
    since = snapshot.created_at - 1
    pager = AsyncSnapshot.list(limit=10, since=since)
    first_page = await pager
    first_page_ids = [listed.id for listed in first_page.snapshots]
    print(f"    First page snapshot IDs: {first_page_ids}")

    paged_ids: list[list[str]] = []
    found_ids: set[str] = set()
    page = first_page
    while page is not None:
        page_ids = [listed.id for listed in page.snapshots]
        paged_ids.append(page_ids)
        found_ids.update(page_ids)
        if all(snapshot_id in found_ids for snapshot_id in async_snapshot_ids):
            break
        page = await page.get_next_page()

    print(f"    Visited pages: {paged_ids}")
    assert all(snapshot_id in found_ids for snapshot_id in async_snapshot_ids), (
        "Did not find all async snapshots in AsyncSnapshot.list() results"
    )

    item_ids: list[str] = []
    page = await AsyncSnapshot.list(limit=10, since=since)
    while page is not None:
        item_ids.extend(listed.id for listed in page)
        if all(snapshot_id in item_ids for snapshot_id in async_snapshot_ids):
            break
        page = await page.get_next_page()

    print(f"    Iterated snapshot IDs: {item_ids}")
    assert all(snapshot_id in item_ids for snapshot_id in async_snapshot_ids), (
        "Did not find all async snapshots while iterating AsyncSnapshot.list() items"
    )

    # Step 6: Retrieve and delete the snapshots
    print("\n[8] Retrieving and deleting snapshots...")
    for snapshot_id in async_snapshot_ids:
        fetched = await AsyncSnapshot.get(snapshot_id=snapshot_id)
        try:
            await fetched.delete()
            print(f"    Deleted snapshot {snapshot_id}, status: {fetched.status}")
            assert fetched.status == "deleted", "async snapshot should be deleted"
        finally:
            await fetched.client.aclose()


def sync_demo() -> None:
    print("\n" + "=" * 60)
    print("SYNC SNAPSHOT EXAMPLE")
    print("=" * 60)

    sync_snapshot_ids: list[str] = []

    # Step 1: Create a sandbox and set it up
    print("\n[1] Creating initial sandbox...")
    sandbox1 = Sandbox.create(timeout=120_000)
    try:
        print(f"    Sandbox ID: {sandbox1.sandbox_id}")

        # Write some files to simulate a "setup" step
        print("\n[2] Writing files to simulate environment setup...")
        sandbox1.write_files(
            [
                {"path": "config.json", "content": b'{"version": "1.0", "env": "sync"}'},
                {"path": "data/users.txt", "content": b"alice\nbob\ncharlie"},
            ]
        )

        config = sandbox1.read_file("config.json")
        print(f"    Created config.json: {config.decode()}")

        # Step 2: Create a snapshot with an explicit expiration (this STOPS the sandbox)
        print("\n[3] Creating snapshot with expiration=86400000...")
        snapshot = sandbox1.snapshot(expiration=SNAPSHOT_EXPIRATION_MS)
        sync_snapshot_ids.append(snapshot.snapshot_id)
        print(f"    Snapshot ID: {snapshot.snapshot_id}")
        print(f"    Status: {snapshot.status}")
        print(f"    Created At: {snapshot.created_at}")
        print(f"    Expires At: {snapshot.expires_at}")
        print(f"    Sandbox status after snapshot: {sandbox1.status}")
        assert_snapshot_expires_at_expected_time(
            created_at=snapshot.created_at,
            expires_at=snapshot.expires_at,
            label="expiring sync snapshot",
        )

    finally:
        sandbox1.client.close()

    # Step 3: Create a NEW sandbox FROM the snapshot
    print("\n[4] Creating new sandbox from snapshot...")
    sandbox2 = Sandbox.create(
        timeout=120_000,
        source={"type": "snapshot", "snapshot_id": snapshot.snapshot_id},
    )
    try:
        print(f"    New Sandbox ID: {sandbox2.sandbox_id}")
        print(f"    Source Snapshot ID: {sandbox2.source_snapshot_id}")

        # Step 4: Verify the snapshot preserved our files
        print("\n[5] Verifying snapshot preserved files...")
        config2 = sandbox2.read_file("config.json")
        users2 = sandbox2.read_file("data/users.txt")

        print(f"    config.json: {config2.decode()}")
        print(f"    data/users.txt: {users2.decode()}")

        assert config2 == b'{"version": "1.0", "env": "sync"}', "config.json mismatch!"
        assert users2 == b"alice\nbob\ncharlie", "users.txt mismatch!"
        print("\n✓ Sync restore assertions passed!")

        print("\n[6] Creating second snapshot with expiration=0...")
        persistent_snapshot = sandbox2.snapshot(expiration=0)
        sync_snapshot_ids.append(persistent_snapshot.snapshot_id)
        print(f"    Snapshot ID: {persistent_snapshot.snapshot_id}")
        print(f"    Created At: {persistent_snapshot.created_at}")
        print(f"    Expires At: {persistent_snapshot.expires_at}")
        assert persistent_snapshot.expires_at is None, (
            "persistent sync snapshot should not have an expires_at timestamp"
        )

    finally:
        sandbox2.client.close()

    print("\n[7] Listing recent snapshots...")
    since = snapshot.created_at - 1
    first_page = Snapshot.list(limit=10, since=since)
    first_page_ids = [listed.id for listed in first_page.snapshots]
    print(f"    First page snapshot IDs: {first_page_ids}")

    paged_ids: list[list[str]] = []
    found_ids: set[str] = set()
    page = first_page
    while page is not None:
        page_ids = [listed.id for listed in page.snapshots]
        paged_ids.append(page_ids)
        found_ids.update(page_ids)
        if all(snapshot_id in found_ids for snapshot_id in sync_snapshot_ids):
            break
        page = page.get_next_page()

    print(f"    Visited pages: {paged_ids}")
    assert all(snapshot_id in found_ids for snapshot_id in sync_snapshot_ids), (
        "Did not find all sync snapshots in Snapshot.list() pages"
    )

    item_ids: list[str] = []
    page = Snapshot.list(limit=10, since=since)
    while page is not None:
        item_ids.extend(listed.id for listed in page)
        if all(snapshot_id in item_ids for snapshot_id in sync_snapshot_ids):
            break
        page = page.get_next_page()

    print(f"    Iterated snapshot IDs: {item_ids}")
    assert all(snapshot_id in item_ids for snapshot_id in sync_snapshot_ids), (
        "Did not find all sync snapshots while iterating Snapshot.list() items"
    )

    print("\n[8] Retrieving and deleting snapshots...")
    for snapshot_id in sync_snapshot_ids:
        fetched = Snapshot.get(snapshot_id=snapshot_id)
        try:
            fetched.delete()
            print(f"    Deleted snapshot {snapshot_id}, status: {fetched.status}")
            assert fetched.status == "deleted", "sync snapshot should be deleted"
        finally:
            fetched.client.close()


if __name__ == "__main__":
    asyncio.run(async_demo())
    sync_demo()
    print("\n" + "=" * 60)
    print("ALL SNAPSHOT TESTS COMPLETE")
    print("=" * 60)
