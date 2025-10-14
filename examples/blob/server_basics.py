import asyncio
import os

from vercel.blob import (
    put,
    list_blobs,
    head,
    copy,
    del_blob,
    create_folder,
)
from vercel.blob._helpers import UploadProgressEvent


def on_progress(e: UploadProgressEvent) -> None:
    print(f"progress: {e.loaded}/{e.total} bytes ({e.percentage}%)")


async def main() -> None:
    token = os.getenv('BLOB_READ_WRITE_TOKEN')
    assert token, 'Set BLOB_READ_WRITE_TOKEN'

    # 1) Create a folder entry
    folder = await create_folder('examples/assets', token=token)
    print('folder:', folder)

    # 2) Upload a text file
    data = b"hello from python" * 1024
    uploaded = await put(
        'examples/assets/hello.txt',
        data,
        access='public',
        content_type='text/plain',
        token=token,
        add_random_suffix=True,
        on_upload_progress=on_progress,
    )
    print('uploaded:', uploaded.pathname)

    # 3) List and head
    listing = await list_blobs(prefix='examples/assets/', limit=5, token=token)
    print('hasMore:', listing.has_more)
    for b in listing.blobs:
        meta = await head(b.url, token=token)
        print(' -', b.pathname, b.size, meta.content_type)

    # 4) Copy
    copied = await copy(
        uploaded.pathname,
        'examples/assets/hello-copy.txt',
        access='public',
        token=token,
        allow_overwrite=True,
    )
    print('copied:', copied.pathname)

    # 5) Clean up
    await del_blob([uploaded.url, copied.url], token=token)
    print('deleted original and copy')


if __name__ == '__main__':
    asyncio.run(main())


