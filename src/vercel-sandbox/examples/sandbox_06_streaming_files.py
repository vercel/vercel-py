#!/usr/bin/env python3
"""Demonstrate streaming file upload and download on a Sandbox session."""

from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import anyio
from dotenv import load_dotenv

from vercel import sandbox

load_dotenv()

DATA_SIZE = 1024 * 1024  # 1 MiB
CHUNK_SIZE = 64 * 1024


async def main() -> None:
    name = f"vercel-py-streaming-{uuid4().hex[:12]}"
    with TemporaryDirectory() as directory:
        source_path = anyio.Path(directory) / "source.bin"
        target_path = anyio.Path(directory) / "target.bin"
        sync_source_path = Path(directory) / "sync-source.bin"
        sync_target_path = Path(directory) / "sync-target.bin"
        await source_path.write_bytes(b"\x01" * DATA_SIZE)
        sync_source_path.write_bytes(b"\x02" * DATA_SIZE)

        async with sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=timedelta(minutes=2),
        ) as box:
            async with (
                await anyio.open_file(source_path, "rb") as source,
                box.fs.open("workspace/reference.bin", "wb", permissions=0o600) as target,
            ):
                while chunk := await source.read(CHUNK_SIZE):
                    await target.write(chunk)

            copied = 0
            async with (
                box.fs.open("workspace/reference.bin", "rb") as source,
                await anyio.open_file(target_path, "wb") as target,
            ):
                while chunk := await source.read(CHUNK_SIZE):
                    await target.write(chunk)
                    copied += len(chunk)
            print(f"Downloaded {copied} bytes")

            assert await target_path.read_bytes() == b"\x01" * DATA_SIZE

            with sync_source_path.open("rb") as source:
                async with box.fs.open(
                    "workspace/sync-reference.bin",
                    "wb",
                    permissions=0o600,
                ) as target:
                    while chunk := source.read(CHUNK_SIZE):
                        await target.write(chunk)

            sync_copied = 0
            async with box.fs.open("workspace/sync-reference.bin", "rb") as source:
                with sync_target_path.open("wb") as target:
                    while chunk := await source.read(CHUNK_SIZE):
                        target.write(chunk)
                        sync_copied += len(chunk)
            print(f"Downloaded {sync_copied} bytes with sync local files")

            assert sync_target_path.read_bytes() == b"\x02" * DATA_SIZE

    print("Streaming transfer complete")


if __name__ == "__main__":
    anyio.run(main)
