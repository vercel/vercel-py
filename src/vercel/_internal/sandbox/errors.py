"""Sandbox API error types."""

from __future__ import annotations

from typing import Any

import httpx


class APIError(Exception):
    def __init__(self, response: httpx.Response, message: str, *, data: Any | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.data = data


__all__ = ["APIError"]
