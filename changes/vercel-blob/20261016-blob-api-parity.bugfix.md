Match the Blob service more closely:

- Requests send Blob API version 13, so the service reads object keys from
  delivery URLs as written. A URL path such as `a\..\b.txt` now addresses that
  literal object instead of being resolved to `b.txt` by the service.
- Delivery URLs with malformed percent-encodings are rejected before any
  request, as the service cannot decode them.
- Pathnames and delivery URLs containing C1 control characters are rejected
  by every operation, like other control characters. The CDN cannot serve
  such objects.
- Network failures while reading a download's body raise `BlobUnknownError`
  or `BlobTimeoutError` instead of raw HTTPX exceptions, and a body that cannot
  be decoded raises `BlobStreamError`.
- `DownloadMetadata.size` is `None` for compressed responses, whose
  `content-length` counts the encoded bytes, and `get_blob` reports the length
  of the body it returns.
- Default credentials look up the Vercel OIDC token only when `BLOB_STORE_ID`
  is set, so local development with only a read-write token no longer refreshes
  OIDC tokens over the network or writes them to disk.
