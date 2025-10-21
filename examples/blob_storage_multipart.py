"""
Example demonstrating the multipart uploader pattern.

This pattern provides a middle-ground between:
- Fully automatic: auto_multipart_upload() handles everything
- Fully manual: create_multipart_upload() + upload_part() + complete_multipart_upload()

The uploader pattern:
- Creates the multipart upload once
- Returns an object with upload_part() and complete() methods
- No need to pass upload_id, key, pathname to every call
- You control part creation and concurrency
"""

import os
import asyncio
from dotenv import load_dotenv

from vercel.blob import BlobClient, AsyncBlobClient

load_dotenv()

token = os.getenv("BLOB_READ_WRITE_TOKEN")
assert token, "Set BLOB_READ_WRITE_TOKEN"

client = BlobClient(token)
aclient = AsyncBlobClient(token)


def sync_example():
    """Synchronous multipart uploader example."""
    print("=== Sync Multipart Uploader Example ===\n")

    # Create some sample data (simulate a large file in chunks)
    chunks = [
        b"Part 1: " + b"A" * (5 * 1024 * 1024),  # 5 MB
        b"Part 2: " + b"B" * (5 * 1024 * 1024),  # 5 MB
        b"Part 3: " + b"C" * (5 * 1024 * 1024),  # 5 MB
    ]

    # Create the uploader (Phase 1: Initialize)
    uploader = client.create_multipart_uploader(
        "examples/large-file.bin",
        content_type="application/octet-stream",
        add_random_suffix=True,
    )

    print(f"Created uploader with:")
    print(f"  Upload ID: {uploader.upload_id}")
    print(f"  Key: {uploader.key}\n")

    # Upload parts (Phase 2: Upload)
    parts = []
    for i, chunk in enumerate(chunks, start=1):
        print(f"Uploading part {i}...")
        part = uploader.upload_part(i, chunk)
        parts.append(part)
        print(f"  Part {i} uploaded: {part['etag'][:20]}...\n")

    # Complete the upload (Phase 3: Complete)
    print("Completing multipart upload...")
    result = uploader.complete(parts)

    print(f"\nUpload completed!")
    print(f"  URL: {result['url']}")
    print(f"  Pathname: {result['pathname']}")
    print(f"  Size: {result.get('size', 'N/A')} bytes")


async def async_example():
    """Asynchronous multipart uploader example with concurrent uploads."""
    print("\n\n=== Async Multipart Uploader Example ===\n")

    # Create some sample data
    chunks = [
        b"Part 1: " + b"X" * (5 * 1024 * 1024),  # 5 MB
        b"Part 2: " + b"Y" * (5 * 1024 * 1024),  # 5 MB
        b"Part 3: " + b"Z" * (5 * 1024 * 1024),  # 5 MB
    ]

    # Create the uploader
    uploader = await aclient.create_multipart_uploader(
        "examples/large-file-async.bin",
        content_type="application/octet-stream",
        add_random_suffix=True,
    )

    print(f"Created uploader with:")
    print(f"  Upload ID: {uploader.upload_id}")
    print(f"  Key: {uploader.key}\n")

    # Upload parts concurrently
    print("Uploading parts concurrently...")
    tasks = [uploader.upload_part(i, chunk) for i, chunk in enumerate(chunks, start=1)]
    parts = await asyncio.gather(*tasks)

    for part in parts:
        print(f"  Part {part['partNumber']} uploaded: {part['etag'][:20]}...")

    # Complete the upload
    print("\nCompleting multipart upload...")
    result = await uploader.complete(parts)

    print(f"\nUpload completed!")
    print(f"  URL: {result['url']}")
    print(f"  Pathname: {result['pathname']}")
    print(f"  Size: {result.get('size', 'N/A')} bytes")


async def async_with_file_example():
    """Example with actual file upload using async uploader."""
    print("\n\n=== Async File Upload with Uploader ===\n")

    # Create a test file
    test_file_path = "/tmp/test_multipart_upload.bin"
    part_size = 5 * 1024 * 1024  # 5 MB per part

    # Create a 16 MB test file
    print(f"Creating test file: {test_file_path}")
    with open(test_file_path, "wb") as f:
        f.write(b"0" * (16 * 1024 * 1024))

    # Create the uploader
    uploader = await aclient.create_multipart_uploader(
        "examples/file-from-disk.bin",
        content_type="application/octet-stream",
    )

    print(f"\nCreated uploader for file upload")
    print(f"  Upload ID: {uploader.upload_id}")
    print(f"  Key: {uploader.key}\n")

    # Read and upload file in parts
    parts = []
    with open(test_file_path, "rb") as f:
        part_number = 1
        while True:
            chunk = f.read(part_size)
            if not chunk:
                break

            print(f"Uploading part {part_number} ({len(chunk)} bytes)...")
            part = await uploader.upload_part(part_number, chunk)
            parts.append(part)
            part_number += 1

    # Complete the upload
    print("\nCompleting multipart upload...")
    result = await uploader.complete(parts)

    print(f"\nFile upload completed!")
    print(f"  URL: {result['url']}")
    print(f"  Pathname: {result['pathname']}")
    print(f"  Size: {result.get('size', 'N/A')} bytes")

    # Clean up test file
    os.remove(test_file_path)
    print(f"\nCleaned up test file: {test_file_path}")


def comparison_example():
    """Compare the three approaches: automatic, uploader, and manual."""
    print("\n\n=== API Comparison ===\n")

    print("1. AUTOMATIC (Simplest - no control):")
    print("   from vercel.blob import auto_multipart_upload")
    print("   result = auto_multipart_upload('file.bin', data)")
    print("   # SDK handles everything\n")

    print("2. UPLOADER (Middle ground - clean API with control):")
    print("   from vercel.blob import create_multipart_uploader")
    print("   uploader = create_multipart_uploader('file.bin')")
    print("   part1 = uploader.upload_part(1, chunk1)")
    print("   part2 = uploader.upload_part(2, chunk2)")
    print("   result = uploader.complete([part1, part2])")
    print("   # You control parts, but API is clean\n")

    print("3. MANUAL (Most control - verbose):")
    print("   from vercel.blob import (")
    print("       create_multipart_upload,")
    print("       upload_part,")
    print("       complete_multipart_upload,")
    print("   )")
    print("   resp = create_multipart_upload('file.bin')")
    print("   part1 = upload_part('file.bin', chunk1,")
    print("                       upload_id=resp['uploadId'],")
    print("                       key=resp['key'], part_number=1)")
    print("   part2 = upload_part('file.bin', chunk2,")
    print("                       upload_id=resp['uploadId'],")
    print("                       key=resp['key'], part_number=2)")
    print("   result = complete_multipart_upload('file.bin', [part1, part2],")
    print("                                       upload_id=resp['uploadId'],")
    print("                                       key=resp['key'])")
    print("   # Most verbose, but most control\n")


if __name__ == "__main__":
    # Check for token
    if not os.getenv("BLOB_READ_WRITE_TOKEN"):
        print("ERROR: BLOB_READ_WRITE_TOKEN environment variable not set!")
        print("Please set it to run these examples.")
        exit(1)

    # Show comparison
    comparison_example()

    # Run sync example
    try:
        sync_example()
    except Exception as e:
        print(f"Sync example error: {e}")

    # Run async examples
    try:
        asyncio.run(async_example())
    except Exception as e:
        print(f"Async example error: {e}")

    try:
        asyncio.run(async_with_file_example())
    except Exception as e:
        print(f"Async file example error: {e}")
