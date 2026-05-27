#!/usr/bin/env python3
"""Persist sandbox state with a snapshot and restore it into a new sandbox."""

import asyncio
from uuid import uuid4

from dotenv import load_dotenv

from vercel.unstable import sandbox
from vercel.unstable.sandbox import SnapshotSource, WriteFile

load_dotenv()


async def main() -> None:
    suffix = uuid4().hex[:12]
    base_name = f"vercel-py-snapshot-base-{suffix}"
    restored_name = f"vercel-py-snapshot-restored-{suffix}"
    restored = None
    snapshot = None

    async with sandbox.create_sandbox(name=base_name, runtime="python3.13") as base:
        try:
            await base.write_files(
                [WriteFile(path="state/message.txt", content="restored from snapshot\n")]
            )
            snapshot = await base.snapshot()

            restored = await sandbox.create_sandbox(
                name=restored_name,
                runtime="python3.13",
                source=SnapshotSource(snapshot_id=snapshot.id),
            )
            content = await restored.read_text("state/message.txt")
            assert content == "restored from snapshot\n"
            print(f"{restored_name}: restored {snapshot.id}")
        finally:
            if snapshot is not None:
                await snapshot.delete()
            if restored is not None:
                await restored.destroy()


if __name__ == "__main__":
    asyncio.run(main())
