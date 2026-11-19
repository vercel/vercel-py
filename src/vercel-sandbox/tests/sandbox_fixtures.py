"""Shared helpers for Sandbox tests."""

import cbor2
import httpx2 as httpx
import sniffio

from vercel._internal.core.options import ServiceOptions
from vercel.sandbox import (
    RegionInput,
    SandboxCredentials,
    SandboxServiceOptions,
    sync as sandbox_sync,
)
from vercel.sandbox._internal.log_stream import COMMAND_STREAM_MEDIA_TYPE


def sandbox_service_options(
    *,
    base_url: str = "https://sandbox.test",
    token: str = "token",
    team_id: str = "team_123",
    project_id: str = "prj_123",
    region: RegionInput = None,
    sync: bool | None = None,
) -> list[ServiceOptions]:
    """Build Sandbox options for the test's current session mode."""
    credentials = SandboxCredentials(token=token, team_id=team_id, project_id=project_id)

    if sync is None:
        try:
            sniffio.current_async_library()
        except sniffio.AsyncLibraryNotFoundError:
            sync = True
        else:
            sync = False

    if sync:

        def sync_credentials_factory() -> SandboxCredentials:
            return credentials

        return [
            sandbox_sync.SandboxServiceOptions(
                base_url=base_url,
                credentials_factory=sync_credentials_factory,
                region=region,
            )
        ]

    async def async_credentials_factory() -> SandboxCredentials:
        return credentials

    return [
        SandboxServiceOptions(
            base_url=base_url,
            credentials_factory=async_credentials_factory,
            region=region,
        )
    ]


COMMAND_STREAM_HEADERS = {"content-type": COMMAND_STREAM_MEDIA_TYPE}

_OUTPUT_ITEM_TYPES = {"stdout": 1, "stderr": 2}


def command_stream_item(record: object) -> bytes:
    """Encode an NDJSON-shaped test record as a command stream CBOR item.

    ``{"command": ...}`` becomes command metadata, ``{"stream": "stdout" |
    "stderr", "data": str | bytes}`` becomes output, and ``{"stream": "error",
    "data": ...}`` becomes a stream error. Anything else is encoded unchanged.
    """
    if isinstance(record, dict):
        if record.keys() == {"command"}:
            return cbor2.dumps([0x80, record["command"]])
        stream = record.get("stream")
        data = record.get("data")
        if stream in _OUTPUT_ITEM_TYPES and isinstance(data, str | bytes):
            payload = data.encode() if isinstance(data, str) else data
            return cbor2.dumps([_OUTPUT_ITEM_TYPES[stream], payload])
        if stream == "error":
            return cbor2.dumps([3, data])
    return cbor2.dumps(record)


def command_stream_body(*records: object) -> bytes:
    return b"".join(command_stream_item(record) for record in records)


def command_stream_response(*records: object, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code, content=command_stream_body(*records), headers=COMMAND_STREAM_HEADERS
    )
