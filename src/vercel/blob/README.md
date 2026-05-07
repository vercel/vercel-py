# Vercel Blob

`vercel.blob` provides Vercel Blob upload, download, metadata, listing, copy,
folder, delete, and multipart helpers.

## Credentials

By default, Blob clients and module-level helpers read `BLOB_READ_WRITE_TOKEN`
from the environment when making a request. To override the environment token,
pass `token=` to the operation that needs it.

`BlobClient` and `AsyncBlobClient` keep a long-lived HTTP transport for the life
of the client instance. Prefer `with BlobClient()` or
`async with AsyncBlobClient()`, or call `close()` / `aclose()` explicitly when
done.

## Async Client

```python
from vercel.blob import AsyncBlobClient


async def main() -> None:
    async with AsyncBlobClient() as client:
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
    async with AsyncBlobClient() as client:
        uploaded = await client.upload_file(
            "./avatar.png",
            "avatars/user-123.png",
            access="public",
            content_type="image/png",
        )
        await client.download_file(uploaded.url, "/tmp/user-123.png", overwrite=True)
```

## Sync Client

```python
from vercel.blob import BlobClient


with BlobClient() as client:
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
`token=` to individual operations when you need to use a token other than the
environment token.
