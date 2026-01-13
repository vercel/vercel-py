import asyncio
import os
import tempfile

from dotenv import load_dotenv

from vercel.blob import AsyncBlobClient, BlobClient, UploadProgressEvent

load_dotenv()


def on_progress(e: UploadProgressEvent) -> None:
    print(f"progress: {e.loaded}/{e.total} bytes ({e.percentage}%)")


def on_download_progress(bytes_read: int, total: int | None) -> None:
    pct = int(bytes_read / total * 100) if total else None
    if pct is not None:
        print(f"download: {bytes_read}/{total} bytes ({pct}%)")
    else:
        print(f"download: {bytes_read} bytes")


async def main() -> None:
    token = os.getenv("BLOB_READ_WRITE_TOKEN")
    assert token, "Set BLOB_READ_WRITE_TOKEN"

    # Instantiate clients
    client = AsyncBlobClient(token)
    client_sync = BlobClient(token)

    # 1) Create a folder entry (async client)
    folder = await client.create_folder("examples/assets", overwrite=True)
    print("folder:", folder.pathname)

    # 2) Upload a text file via put() (async client)
    data = b"hello from python" * 1024
    uploaded = await client.put(
        "examples/assets/hello.txt",
        data,
        access="public",
        content_type="text/plain",
        add_random_suffix=True,
        on_upload_progress=on_progress,
    )
    print("uploaded (put):", uploaded.pathname)

    # 3) List and head (async client)
    listing = await client.list_objects(prefix="examples/assets/", limit=5)
    print("hasMore:", listing.has_more)
    for b in listing.blobs:
        meta = await client.head(b.url)
        print(" -", b.pathname, b.size, meta.content_type)

    # 3b) Get object bytes via get() (async client)
    content = await client.get(uploaded.url)
    print("get():", len(content), "bytes")

    # 4) Copy (async client)
    copied = await client.copy(
        uploaded.pathname,
        "examples/assets/hello-copy.txt",
        access="public",
        overwrite=True,
    )
    print("copied:", copied.pathname)

    # 5) Upload a local file via upload_file() (async client)
    tmp_local_path: str
    with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
        tmp.write(b"this was uploaded using upload_file()\n")
        tmp_local_path = tmp.name
    uploaded_file = await client.upload_file(
        tmp_local_path,
        "examples/assets/uploaded-from-file.txt",
        access="public",
        content_type="text/plain",
        add_random_suffix=True,
        on_upload_progress=on_progress,
    )
    print("uploaded (upload_file):", uploaded_file.pathname)
    try:
        os.remove(tmp_local_path)
    except OSError:
        pass

    # 6) Download a file to disk via download_file() (async client)
    download_path = os.path.join(tempfile.gettempdir(), "downloaded-hello.txt")
    saved_path = await client.download_file(
        uploaded.url,
        download_path,
        overwrite=True,
        create_parents=True,
        progress=on_download_progress,
    )
    print("downloaded to:", saved_path)
    try:
        os.remove(saved_path)
    except OSError:
        pass

    # 7) Demonstrate synchronous BlobClient: head + download + cleanup
    meta_sync = client_sync.head(uploaded.url)
    print("sync head content_type:", meta_sync.content_type)

    sync_download_path = os.path.join(tempfile.gettempdir(), "downloaded-hello-sync.txt")
    saved_sync_path = client_sync.download_file(
        uploaded.url,
        sync_download_path,
        overwrite=True,
        create_parents=True,
        progress=on_download_progress,
    )
    print("sync downloaded to:", saved_sync_path)
    try:
        os.remove(saved_sync_path)
    except OSError:
        pass

    # Cleanup using sync client
    client_sync.delete([uploaded.url, copied.url, uploaded_file.url])
    print("deleted uploaded, copy, and file-upload objects (sync client)")


if __name__ == "__main__":
    asyncio.run(main())
