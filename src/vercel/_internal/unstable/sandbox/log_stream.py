"""Shared command log record decoding for Sandbox streams."""

import json

from vercel._internal.unstable.sandbox.errors import SandboxStreamError
from vercel._internal.unstable.sandbox.models import SandboxCommandLog


def _parse_command_log_record(line: str) -> SandboxCommandLog | None:
    """Decode one wire log record, skipping unsupported or malformed input."""
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(record, dict):
        return None

    stream = record.get("stream")
    data = record.get("data")
    if stream in {"stdout", "stderr"} and isinstance(data, str):
        return SandboxCommandLog(stream=stream, data=data)
    if stream != "error" or not isinstance(data, dict):
        return None

    code = data.get("code")
    message = data.get("message")
    if isinstance(code, str) and isinstance(message, str):
        raise SandboxStreamError(message, code=code)
    return None
