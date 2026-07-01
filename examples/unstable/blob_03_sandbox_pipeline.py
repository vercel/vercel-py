#!/usr/bin/env python3
"""Stream a Blob through a Sandbox and publish the result."""

from uuid import uuid4

import anyio
from dotenv import load_dotenv

from vercel.unstable import blob, sandbox

load_dotenv()

CHUNK_SIZE = 64 * 1024


async def copy_blob_to_sandbox(
    blob_path: str,
    box: sandbox.Sandbox,
    sandbox_path: str,
) -> None:
    """Copy exactly the ETag-pinned Blob size into a bounded Sandbox upload."""
    async with blob.open(blob_path, "rb") as source:
        remaining = source.stat.size
        async with box.fs.open(sandbox_path, "wb", size=source.stat.size) as target:
            while remaining:
                chunk = await source.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    raise EOFError("Blob ended before its stated size")
                await target.write(chunk)
                remaining -= len(chunk)


async def copy_sandbox_to_blob(
    box: sandbox.Sandbox,
    sandbox_path: str,
    blob_path: str,
    *,
    content_type: str,
) -> blob.BlobStatResult:
    """Stream a Sandbox file into a new Blob object."""
    async with (
        box.fs.open(sandbox_path, "rb") as source,
        blob.open(blob_path, "wb", content_type=content_type) as target,
    ):
        while chunk := await source.read(CHUNK_SIZE):
            await target.write(chunk)
    return target.stat


async def main() -> None:
    prefix = f"examples/unstable-blob/{uuid4().hex}/"
    input_path = f"{prefix}incoming/orders.csv"
    output_path = f"{prefix}processed/orders-summary.json"
    try:
        async with blob.open(input_path, "w", content_type="text/csv") as source:
            await source.write("id,status\n1,paid\n2,pending\n3,paid\n")

        async with sandbox.create_sandbox(runtime="python3.13") as box:
            await box.fs.mkdir("job")
            await copy_blob_to_sandbox(input_path, box, "job/orders.csv")
            await box.fs.write_text(
                "job/summarize.py",
                """
import csv
import json
from collections import Counter
from pathlib import Path

orders = list(csv.DictReader(Path("job/orders.csv").open()))
summary = {
    "orders": len(orders),
    "status_counts": Counter(order["status"] for order in orders),
}
Path("job/summary.json").write_text(json.dumps(summary, indent=2))
""".strip(),
            )
            await box.run_process("python", ["job/summarize.py"], check=True)
            published = await copy_sandbox_to_blob(
                box,
                "job/summary.json",
                output_path,
                content_type="application/json",
            )

        print(f"Published {published.pathname} ({published.size} bytes)")
    finally:
        await blob.rmtree(prefix, missing_ok=True)


if __name__ == "__main__":
    anyio.run(main)
