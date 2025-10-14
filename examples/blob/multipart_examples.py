import asyncio
import os
from dotenv import load_dotenv

from vercel import blob


load_dotenv()


async def uncontrolled(token: str) -> None:
    print("--- uncontrolled multipart ---")
    # Simulate a large payload
    body = b"A" * (10 * 1024 * 1024)  # 10MB
    res = await blob.uncontrolled_multipart_upload(
        "examples/mpu/large.bin",
        body,
        access="public",
        content_type="application/octet-stream",
        token=token,
    )
    print("uploaded:", res["pathname"])


async def manual(token: str) -> None:
    print("--- manual multipart ---")
    pathname = "examples/mpu/manual.bin"
    # 1) create
    mpu = await blob.create_multipart_upload(
        pathname,
        access="public",
        content_type="application/octet-stream",
        token=token,
    )
    upload_id, key = mpu["uploadId"], mpu["key"]
    # 2) upload parts
    part1 = await blob.upload_part(
        pathname,
        b"X" * (8 * 1024 * 1024),
        access="public",
        token=token,
        upload_id=upload_id,
        key=key,
        part_number=1,
        content_type="application/octet-stream",
    )
    part2 = await blob.upload_part(
        pathname,
        b"Y" * (2 * 1024 * 1024),
        access="public",
        token=token,
        upload_id=upload_id,
        key=key,
        part_number=2,
        content_type="application/octet-stream",
    )
    # 3) complete
    res = await blob.complete_multipart_upload(
        pathname,
        [
            {"etag": part1["etag"], "partNumber": 1},
            {"etag": part2["etag"], "partNumber": 2},
        ],
        access="public",
        content_type="application/octet-stream",
        token=token,
        upload_id=upload_id,
        key=key,
    )
    print("completed:", res["pathname"])


async def main() -> None:
    token = os.getenv("BLOB_READ_WRITE_TOKEN")
    assert token, "Set BLOB_READ_WRITE_TOKEN"
    await uncontrolled(token)
    await manual(token)


if __name__ == "__main__":
    asyncio.run(main())
