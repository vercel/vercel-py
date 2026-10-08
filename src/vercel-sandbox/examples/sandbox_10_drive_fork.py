#!/usr/bin/env python3
"""Fork committed Drive data and modify the copy independently (private beta)."""

import asyncio
from datetime import timedelta
from uuid import uuid4

from dotenv import load_dotenv

from vercel import sandbox

load_dotenv()


async def main() -> None:
    suffix = uuid4().hex[:12]
    source, _ = await sandbox.get_or_create_drive(name=f"drive-source-{suffix}")
    forked = None
    try:
        async with sandbox.create_sandbox(
            region=source.region,
            mounts={"/data": source},
            execution_time_limit=timedelta(minutes=2),
        ) as box:
            await box.fs.write_text("/data/message.txt", "source version\n")
        # Stopping the read-write session commits the source's data.
        forked = await source.fork(name=f"drive-fork-{suffix}")
        assert forked.parent_drive_id == source.id
        async with sandbox.create_sandbox(
            region=forked.region,
            mounts={"/data": forked, "/source": source.snapshot()},
            execution_time_limit=timedelta(minutes=2),
        ) as box:
            assert await box.fs.read_text("/data/message.txt") == "source version\n"
            await box.fs.write_text("/data/message.txt", "fork version\n")
            assert await box.fs.read_text("/source/message.txt") == "source version\n"
            print(f"forked {source.name} as {forked.name}, root {forked.root_drive_id}")
    finally:
        try:
            if forked is not None:
                await forked.delete()
        finally:
            await source.delete()


if __name__ == "__main__":
    asyncio.run(main())
