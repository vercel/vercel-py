from __future__ import annotations

import dataclasses
import json
from typing import Any, Generic, TypeVar

import httpx
import pydantic

from . import world as w

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class WebhookRequest(Generic[T]):
    """An HTTP request delivered through a workflow webhook."""

    method: str
    url: str
    headers: httpx.Headers
    raw_body: bytes
    body: T


class _FixedResponse(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    status: int
    body: bytes
    headers: dict[str, str]

    @classmethod
    def from_response(cls, response: w.HTTPResponse) -> _FixedResponse:
        return cls(status=response.status, body=response.body, headers=response.headers)

    def into_response(self) -> w.HTTPResponse:
        return w.HTTPResponse(status=self.status, body=self.body, headers=dict(self.headers))


class _MetadataEnvelope(pydantic.BaseModel):
    """Private webhook configuration stored in the hook metadata payload.

    Webhook metadata is known to have this shape because ``isWebhook`` is a
    separate wire flag. User metadata therefore stays nested and cannot
    collide with SDK configuration keys.
    """

    model_config = pydantic.ConfigDict(extra="forbid", populate_by_name=True)

    user_metadata: Any = pydantic.Field(default=None, alias="userMetadata")
    respond_with: _FixedResponse | None = pydantic.Field(default=None, alias="respondWith")


class _RequestEnvelope(pydantic.BaseModel):
    """Serializable HTTP request representation carried by ``hook_received``."""

    model_config = pydantic.ConfigDict(extra="forbid")

    method: str
    url: str
    headers: list[tuple[str, str]]
    raw_body: bytes = pydantic.Field(alias="rawBody")


def encode_metadata(metadata: Any, respond_with: w.HTTPResponse | None) -> dict[str, Any] | None:
    if metadata is None and respond_with is None:
        return None
    envelope = _MetadataEnvelope(
        user_metadata=metadata,
        respond_with=None if respond_with is None else _FixedResponse.from_response(respond_with),
    )
    return envelope.model_dump(by_alias=True, exclude_none=True)


def decode_metadata(value: Any) -> tuple[Any, w.HTTPResponse | None]:
    if value is None:
        return None, None
    envelope = _MetadataEnvelope.model_validate(value)
    response = None if envelope.respond_with is None else envelope.respond_with.into_response()
    return envelope.user_metadata, response


def encode_request(
    *, method: str, url: str, headers: list[tuple[str, str]], raw_body: bytes
) -> dict[str, Any]:
    return _RequestEnvelope(
        method=method,
        url=url,
        headers=headers,
        rawBody=raw_body,
    ).model_dump(by_alias=True)


def decode_request(value: Any, hook_cls: type[T]) -> WebhookRequest[T]:
    envelope = _RequestEnvelope.model_validate(value)
    decoded_body = json.loads(envelope.raw_body)
    body: T
    if dataclasses.is_dataclass(hook_cls):
        if not isinstance(decoded_body, dict):
            raise TypeError("a dataclass webhook body must decode to a JSON object")
        body = hook_cls(**decoded_body)
    elif issubclass(hook_cls, pydantic.BaseModel):
        body = hook_cls.model_validate(decoded_body)
    else:
        raise RuntimeError(f"Invalid hook type for {hook_cls}")
    return WebhookRequest(
        method=envelope.method,
        url=envelope.url,
        headers=httpx.Headers(envelope.headers),
        raw_body=envelope.raw_body,
        body=body,
    )
