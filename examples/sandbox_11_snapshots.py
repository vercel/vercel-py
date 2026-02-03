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


async def async_demo() -> None:
    print("=" * 60)
    print("ASYNC SNAPSHOT EXAMPLE")
    print("=" * 60)

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

        # Step 2: Create a snapshot (this STOPS the sandbox)
        print("\n[3] Creating snapshot...")
        snapshot = await sandbox1.snapshot()
        print(f"    Snapshot ID: {snapshot.snapshot_id}")
        print(f"    Status: {snapshot.status}")
        print(f"    Sandbox status after snapshot: {sandbox1.status}")

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
        print("\n✓ Async assertions passed!")

    finally:
        await sandbox2.stop()
        await sandbox2.client.aclose()

    # Step 5: Retrieve and delete the snapshot
    print("\n[6] Retrieving and deleting snapshot...")
    fetched = await AsyncSnapshot.get(snapshot_id=snapshot.snapshot_id)
    try:
        await fetched.delete()
        print(f"    Deleted snapshot, status: {fetched.status}")
    finally:
        await fetched.client.aclose()


def sync_demo() -> None:
    print("\n" + "=" * 60)
    print("SYNC SNAPSHOT EXAMPLE")
    print("=" * 60)

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

        # Step 2: Create a snapshot (this STOPS the sandbox)
        print("\n[3] Creating snapshot...")
        snapshot = sandbox1.snapshot()
        print(f"    Snapshot ID: {snapshot.snapshot_id}")
        print(f"    Status: {snapshot.status}")
        print(f"    Sandbox status after snapshot: {sandbox1.status}")

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
        print("\n✓ Sync assertions passed!")

    finally:
        sandbox2.stop()
        sandbox2.client.close()

    # Step 5: Retrieve and delete the snapshot
    print("\n[6] Retrieving and deleting snapshot...")
    fetched = Snapshot.get(snapshot_id=snapshot.snapshot_id)
    try:
        fetched.delete()
        print(f"    Deleted snapshot, status: {fetched.status}")
    finally:
        fetched.client.close()


if __name__ == "__main__":
    asyncio.run(async_demo())
    sync_demo()
    print("\n" + "=" * 60)
    print("ALL SNAPSHOT TESTS COMPLETE")
    print("=" * 60)
