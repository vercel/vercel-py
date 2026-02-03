from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx


class APIError(Exception):
    def __init__(self, response: httpx.Response, message: str, *, data: Any | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.data = data


class BaseClient:
    def __init__(self, *, host: str, token: str | None = None):
        self._host = host.rstrip("/")
        self._token = token
        self._client = httpx.Client(base_url=self._host, timeout=httpx.Timeout(None))

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        query: Mapping[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> httpx.Response:
        req_headers: dict[str, str] = {"content-type": "application/json"}
        if self._token:
            req_headers["authorization"] = f"Bearer {self._token}"
        if headers:
            req_headers.update(headers)

        response = self._client.request(
            method,
            path,
            params={k: v for k, v in (query or {}).items() if v is not None},
            json=json_body,
            headers=req_headers,
        )

        if 200 <= response.status_code < 300:
            return response

        # Attempt to parse a helpful error message from the API
        parsed: Any | None = None
        message = f"HTTP {response.status_code}"
        try:
            parsed = response.json()
            # Vercel errors commonly include fields like error, message, code
            # Try a few shapes for a concise message
            if isinstance(parsed, dict):
                if "message" in parsed and isinstance(parsed["message"], str):
                    message = f"{message}: {parsed['message']}"
                elif "error" in parsed:
                    err = parsed["error"]
                    if isinstance(err, dict):
                        code = err.get("code")
                        msg = err.get("message") or err.get("msg")
                        if msg:
                            message = f"{message}: {msg}"
                        if code:
                            message = f"{message} (code={code})"
        except Exception:
            parsed = None

        # If still not informative, include a small slice of the text body
        if parsed is None:
            try:
                text = response.text
                if text:
                    snippet = text if len(text) <= 500 else text[:500] + "…"
                    message = f"{message}: {snippet}"
            except Exception:
                pass

        raise APIError(response, message, data=parsed)

    def request_json(self, *args, **kwargs):
        r = self.request(*args, **kwargs)
        return r.json()


class AsyncBaseClient:
    def __init__(self, *, host: str, token: str | None = None):
        self._host = host.rstrip("/")
        self._token = token
        self._client = httpx.AsyncClient(base_url=self._host, timeout=httpx.Timeout(None))

    async def aclose(self):
        await self._client.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        query: Mapping[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> httpx.Response:
        req_headers: dict[str, str] = {"content-type": "application/json"}
        if self._token:
            req_headers["authorization"] = f"Bearer {self._token}"
        if headers:
            req_headers.update(headers)

        response = await self._client.request(
            method,
            path,
            params={k: v for k, v in (query or {}).items() if v is not None},
            json=json_body,
            headers=req_headers,
        )

        if 200 <= response.status_code < 300:
            return response

        # Attempt to parse a helpful error message from the API
        parsed: Any | None = None
        message = f"HTTP {response.status_code}"
        try:
            parsed = response.json()
            # Vercel errors commonly include fields like error, message, code
            # Try a few shapes for a concise message
            if isinstance(parsed, dict):
                if "message" in parsed and isinstance(parsed["message"], str):
                    message = f"{message}: {parsed['message']}"
                elif "error" in parsed:
                    err = parsed["error"]
                    if isinstance(err, dict):
                        code = err.get("code")
                        msg = err.get("message") or err.get("msg")
                        if msg:
                            message = f"{message}: {msg}"
                        if code:
                            message = f"{message} (code={code})"
        except Exception:
            parsed = None

        # If still not informative, include a small slice of the text body
        if parsed is None:
            try:
                text = response.text
                if text:
                    snippet = text if len(text) <= 500 else text[:500] + "…"
                    message = f"{message}: {snippet}"
            except Exception:
                pass

        raise APIError(response, message, data=parsed)

    async def request_json(self, *args, **kwargs):
        r = await self.request(*args, **kwargs)
        return r.json()
