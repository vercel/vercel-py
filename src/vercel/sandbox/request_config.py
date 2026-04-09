from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.http import RetryPolicy

DEFAULT_SANDBOX_REQUEST_TIMEOUT = 180.0


@dataclass(frozen=True)
class SandboxRequestConfig:
    """Transport-level request defaults for sandbox API calls.

    This config applies to the HTTP requests issued by the sandbox ops client.
    Workflow-level controls such as ``wait_for_status(timeout=...)`` and
    ``stop(blocking=True, timeout=...)`` remain separate per-call settings.
    """

    timeout: float | None = None
    retry: RetryPolicy | None = None


def resolve_sandbox_request_timeout(request_config: SandboxRequestConfig | None) -> float:
    if request_config is None or request_config.timeout is None:
        return DEFAULT_SANDBOX_REQUEST_TIMEOUT
    return request_config.timeout


__all__ = [
    "DEFAULT_SANDBOX_REQUEST_TIMEOUT",
    "SandboxRequestConfig",
    "resolve_sandbox_request_timeout",
]
