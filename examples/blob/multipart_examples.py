import asyncio
import os

from vercel.blob import put
from vercel.blob.multipart import (
    create_multipart_upload,
    upload_part,
    complete_multipart_upload,
    uncontrolled_multipart_upload,
)


async def uncontrolled(token: str) -> None:
    print('--- uncontrolled multipart ---')
    # Simulate a large payload
    body = b"A" * (10 * 1024 * 1024)  # 10MB
    headers = {}
    opts = {'access': 'public', 'token': token, 'contentType': 'application/octet-stream'}
    res = await uncontrolled_multipart_upload('examples/mpu/large.bin', body, headers, opts)
    print('uploaded:', res['pathname'])


async def manual(token: str) -> None:
    print('--- manual multipart ---')
    pathname = 'examples/mpu/manual.bin'
    opts = {'access': 'public', 'token': token, 'contentType': 'application/octet-stream'}
    # 1) create
    mpu = await create_multipart_upload(pathname, opts)
    upload_id, key = mpu['uploadId'], mpu['key']
    # 2) upload parts
    part1 = await upload_part(pathname, b"X" * (8 * 1024 * 1024), {**opts, 'uploadId': upload_id, 'key': key, 'partNumber': 1})
    part2 = await upload_part(pathname, b"Y" * (2 * 1024 * 1024), {**opts, 'uploadId': upload_id, 'key': key, 'partNumber': 2})
    # 3) complete
    res = await complete_multipart_upload(pathname, [
        {'etag': part1['etag'], 'partNumber': 1},
        {'etag': part2['etag'], 'partNumber': 2},
    ], {**opts, 'uploadId': upload_id, 'key': key})
    print('completed:', res['pathname'])


async def main() -> None:
    token = os.getenv('BLOB_READ_WRITE_TOKEN')
    assert token, 'Set BLOB_READ_WRITE_TOKEN'
    await uncontrolled(token)
    await manual(token)


if __name__ == '__main__':
    asyncio.run(main())


