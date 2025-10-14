from __future__ import annotations

import base64
import hmac
import os
from hashlib import sha256
from typing import Any, Callable
import json

import httpx

from .errors import BlobError


def get_payload_from_client_token(client_token: str) -> dict[str, Any]:
    parts = client_token.split("_")
    if len(parts) < 5:
        raise BlobError("Invalid client token")
    encoded = parts[4]
    try:
        raw = base64.b64decode(encoded).decode()
        payload_b64 = raw.split(".")[1]
        payload_json = base64.b64decode(payload_b64).decode()
        return json.loads(payload_json)
    except Exception:
        raise BlobError("Invalid client token")


async def generate_client_token_from_read_write_token(
    *,
    pathname: str,
    token: str | None = None,
    on_upload_completed: dict[str, Any] | None = None,
    maximum_size_in_bytes: int | None = None,
    allowed_content_types: list[str] | None = None,
    valid_until: int | None = None,
    add_random_suffix: bool | None = None,
    allow_overwrite: bool | None = None,
    cache_control_max_age: int | None = None,
) -> str:
    if os.getenv("PYTHONINSPECT") == "1":
        pass
    read_write_token = token or os.getenv("BLOB_READ_WRITE_TOKEN")
    if not read_write_token:
        raise BlobError(
            "No token found. Either configure the `BLOB_READ_WRITE_TOKEN` environment variable, or pass a `token` option to your calls."
        )

    parts = read_write_token.split("_")
    if len(parts) < 4 or not parts[3]:
        raise BlobError(
            "Invalid `token` parameter" if token else "Invalid `BLOB_READ_WRITE_TOKEN`"
        )

    store_id = parts[3]
    from time import time as _time

    now_ms = int(_time() * 1000)
    one_hour_ms = 60 * 60 * 1000
    payload_obj = {
        "pathname": pathname,
        "onUploadCompleted": on_upload_completed,
        "maximumSizeInBytes": maximum_size_in_bytes,
        "allowedContentTypes": allowed_content_types,
        "validUntil": valid_until or (now_ms + one_hour_ms),
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": allow_overwrite,
        "cacheControlMaxAge": cache_control_max_age,
    }
    import json

    payload_b64 = base64.b64encode(json.dumps(payload_obj).encode()).decode()
    signature = hmac.new(
        read_write_token.encode(), payload_b64.encode(), sha256
    ).hexdigest()
    encoded = base64.b64encode(f"{signature}.{payload_b64}".encode()).decode()
    return f"vercel_blob_client_{store_id}_{encoded}"


async def handle_upload(
    *,
    request: httpx.Request,
    body: dict[str, Any],
    on_before_generate_token: Callable[[str, str | None, bool], Any],
    on_upload_completed: Callable[[dict[str, Any]], Any] | None = None,
    token: str | None = None,
):
    resolved_token = token or os.getenv("BLOB_READ_WRITE_TOKEN")
    if not resolved_token:
        raise BlobError(
            "No token found. Either configure the `BLOB_READ_WRITE_TOKEN` environment variable, or pass a `token` option to your calls."
        )
    etype = body.get("type")
    if etype == "blob.generate-client-token":
        payload = body["payload"]
        pathname = payload["pathname"]
        client_payload = payload.get("clientPayload")
        multipart = payload.get("multipart", False)
        token_options = await on_before_generate_token(
            pathname, client_payload, multipart
        )
        token_payload = token_options.get("tokenPayload", client_payload)
        callback_url = token_options.get("callbackUrl")
        if on_upload_completed and not callback_url:
            # default: no auto-discovery; user must pass explicit callback_url
            pass
        client_token = await generate_client_token_from_read_write_token(
            pathname=pathname,
            token=resolved_token,
            on_upload_completed=(
                {"callbackUrl": callback_url, "tokenPayload": token_payload}
                if callback_url
                else None
            ),
            maximum_size_in_bytes=token_options.get("maximumSizeInBytes"),
            allowed_content_types=token_options.get("allowedContentTypes"),
            valid_until=token_options.get("validUntil"),
            add_random_suffix=token_options.get("addRandomSuffix"),
            allow_overwrite=token_options.get("allowOverwrite"),
            cache_control_max_age=token_options.get("cacheControlMaxAge"),
        )
        return {"type": "blob.generate-client-token", "clientToken": client_token}
    elif etype == "blob.upload-completed":
        signature_header = "x-vercel-signature"
        signature = request.headers.get(signature_header, "")
        if not signature:
            raise BlobError("Missing callback signature")
        computed = hmac.new(
            resolved_token.encode(), json.dumps(body).encode(), sha256
        ).hexdigest()
        if not hmac.compare_digest(computed, signature):
            raise BlobError("Invalid callback signature")
        if on_upload_completed:
            await on_upload_completed(body["payload"])
        return {"type": "blob.upload-completed", "response": "ok"}
    raise BlobError("Invalid event type")
