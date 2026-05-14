# Vercel Blob

`vercel.blob` provides Vercel Blob upload, download, metadata, listing, copy,
folder, delete, and multipart helpers.

## Credentials

By default, Blob clients and module-level helpers read `BLOB_READ_WRITE_TOKEN`
from the environment when making a request. To pin a token to a client, pass
`token=` to `BlobClient(...)` or `AsyncBlobClient(...)`. To override the client
or environment token for one request, pass `token=` to that operation.

`BlobClient` and `AsyncBlobClient` keep a long-lived HTTP transport for the life
of the client instance. Prefer `with BlobClient()` or
`async with AsyncBlobClient()`, or call `close()` / `aclose()` explicitly when
done.

Multipart APIs follow the same credential rules. Operation-level tokens override
client tokens.

## Async Client

```python
from vercel.blob import AsyncBlobClient


async def main() -> None:
    async with AsyncBlobClient() as client:  # or AsyncBlobClient(token="...")
        uploaded = await client.put(
            "avatars/user-123.txt",
            b"hello",
            access="public",
            content_type="text/plain",
        )
        metadata = await client.head(uploaded.url)
        content = await client.get(uploaded.url)
        listing = await client.list_objects(prefix="avatars/")
        await client.delete([item.url for item in listing.blobs])
```

## Files

```python
from vercel.blob import AsyncBlobClient


async def main() -> None:
    async with AsyncBlobClient() as client:  # or AsyncBlobClient(token="...")
        uploaded = await client.upload_file(
            "./avatar.png",
            "avatars/user-123.png",
            access="public",
            content_type="image/png",
        )
        await client.download_file(uploaded.url, "/tmp/user-123.png", overwrite=True)
```

## Multipart Uploads

For large files, use the automatic helper when you want the SDK to handle part
creation, upload, and completion:

```python
from vercel.blob import auto_multipart_upload, auto_multipart_upload_async

result = auto_multipart_upload(
    "large-file.bin",
    large_data,
    part_size=8 * 1024 * 1024,
    # token="vercel_blob_rw_...",  # optional; otherwise uses the env token
)

result = await auto_multipart_upload_async(
    "large-file.bin",
    large_data,
    # token="vercel_blob_rw_...",  # optional; otherwise uses the env token
)
```

Use the uploader pattern when you want to control how parts are created and
scheduled:

```python
from vercel.blob import BlobClient


with BlobClient() as client:  # or BlobClient(token="...")
    uploader = client.create_multipart_uploader(
        "large-file.bin",
        content_type="application/octet-stream",
        # token="vercel_blob_rw_...",  # optional per operation
    )
    parts = [uploader.upload_part(i, chunk) for i, chunk in enumerate(chunks, start=1)]
    result = uploader.complete(parts)
```

For manual multipart uploads, pass `token=` to create, each part upload, and
complete when using explicit credentials:

```python
from vercel.blob import create_multipart_upload, upload_part, complete_multipart_upload

created = create_multipart_upload("large-file.bin")
part = upload_part(
    "large-file.bin",
    chunk,
    upload_id=created.upload_id,
    key=created.key,
    part_number=1,
)
result = complete_multipart_upload(
    "large-file.bin",
    [part],
    upload_id=created.upload_id,
    key=created.key,
)
```

## Sync Client

```python
from vercel.blob import BlobClient


with BlobClient() as client:  # or BlobClient(token="...")
    uploaded = client.put(
        "avatars/user-123.txt",
        b"hello",
        access="public",
    )
    metadata = client.head(uploaded.url)
    content = client.get(uploaded.url)
    listing = client.list_objects(prefix="avatars/")
    client.delete([item.url for item in listing.blobs])
```

Use `BlobClient` or sync functions in `vercel.blob` for synchronous code. Pass
`token=` to a client or individual operations when you need to use a token other
than the environment token.
