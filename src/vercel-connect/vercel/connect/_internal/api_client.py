"""Internal Connect API client."""

import platform
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from typing import Any, TypeVar
from urllib.parse import quote

from httpx import Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from vercel._internal.core.http import (
    BaseTransport,
    JSONBody,
    ReadResponsePolicy,
    RequestBody,
    extract_structured_error,
)
from vercel._internal.core.time import to_ms_int
from vercel.connect._internal.errors import (
    ConnectApiError,
    ConnectorInstallationRequiredError,
    ConnectResponseError,
    NoValidTokenError,
    UserAuthorizationRequiredError,
)
from vercel.connect._internal.models import (
    ConnectAuthorizationDetail,
    ConnectCustomAuthorizationDetail,
    ConnectGitHubAppInstallationAuthorizationDetail,
    ConnectJwtBearerTokenSubject,
    ConnectTokenSubject,
    JSONObject,
)
from vercel.connect._internal.options import ConnectCredentialsFactory
from vercel.connect._internal.state import (
    ConnectAuthorizationState,
    ConnectorMetadataState,
    ConnectorRefState,
    ConnectTokenState,
)

try:
    VERSION = _pkg_version("vercel-connect")
except PackageNotFoundError:  # pragma: no cover - bundled distribution
    try:
        VERSION = _pkg_version("vercel-connect-bundle")
    except PackageNotFoundError:
        VERSION = "development"

PLATFORM = platform.uname()
USER_AGENT = (
    f"vercel-connect/{VERSION} (Python/{sys.version}; {PLATFORM.system}/{PLATFORM.machine})"
)

ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)

_ERROR_CLASSES: Mapping[str, type[ConnectApiError]] = {
    "no_token": NoValidTokenError,
    "user_authorization_required": UserAuthorizationRequiredError,
    "client_installation_required": ConnectorInstallationRequiredError,
    "connector_installation_required": ConnectorInstallationRequiredError,
}


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)


class _ConnectorRefModel(_ApiModel):
    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    service_name: str | None = Field(default=None, alias="serviceName")

    def to_state(self) -> ConnectorRefState:
        return ConnectorRefState(
            id=self.id,
            uid=self.uid,
            type=self.type,
            name=self.name,
            service=self.service,
            service_name=self.service_name,
        )


class _TokenResponseModel(_ApiModel):
    token: str
    expires_at: int = Field(alias="expiresAt")
    connector: _ConnectorRefModel
    token_id: str | None = Field(default=None, alias="tokenId")
    name: str | None = None
    installation_id: str | None = Field(default=None, alias="installationId")
    tenant_id: str | None = Field(default=None, alias="tenantId")
    external_subject: str | None = Field(default=None, alias="externalSubject")
    metadata: JSONObject | None = None
    claims: JSONObject | None = None

    def to_state(self) -> ConnectTokenState:
        return ConnectTokenState(
            token=self.token,
            expires_at=_from_epoch_ms(self.expires_at),
            connector=self.connector.to_state(),
            token_id=self.token_id,
            name=self.name,
            installation_id=self.installation_id,
            tenant_id=self.tenant_id,
            external_subject=self.external_subject,
            metadata=self.metadata,
            claims=self.claims,
        )


class _AuthorizationResponseModel(_ApiModel):
    url: str
    request: str
    verifier: str
    device_code: str | None = Field(default=None, alias="deviceCode")
    expires_at: int | None = Field(default=None, alias="expiresAt")
    connector: _ConnectorRefModel | None = None

    def to_state(self) -> ConnectAuthorizationState:
        return ConnectAuthorizationState(
            url=self.url,
            request=self.request,
            verifier=self.verifier,
            device_code=self.device_code,
            expires_at=None if self.expires_at is None else _from_epoch_ms(self.expires_at),
            connector=None if self.connector is None else self.connector.to_state(),
        )


class _ConnectorMetadataModel(_ApiModel):
    model_config = ConfigDict(extra="allow", frozen=True, populate_by_name=True)

    id: str
    uid: str
    type: str
    name: str | None = None
    service: str | None = None
    client_url: str | None = Field(default=None, alias="clientUrl")
    created_at: int | None = Field(default=None, alias="createdAt")
    updated_at: int | None = Field(default=None, alias="updatedAt")
    data: JSONObject | None = None

    def to_state(self) -> ConnectorMetadataState:
        # Unknown top-level fields are preserved rather than dropped: the server
        # returns more than the documented subset, including the connector
        # capabilities callers are told to read instead of hardcoding.
        declared = {"clientUrl", "createdAt", "updatedAt", "data"}
        extra = {
            name: value for name, value in (self.model_extra or {}).items() if name not in declared
        }
        return ConnectorMetadataState(
            id=self.id,
            uid=self.uid,
            type=self.type,
            name=self.name,
            service=self.service,
            client_url=self.client_url,
            created_at=None if self.created_at is None else _from_epoch_ms(self.created_at),
            updated_at=None if self.updated_at is None else _from_epoch_ms(self.updated_at),
            vendor=dict(self.data or {}),
            extra=extra,
        )


def _from_epoch_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _encode_connector(connector: str) -> str:
    # Connector UIDs contain `/`, so every reserved character must be escaped
    # rather than merging into the path.
    return quote(connector, safe="")


def _serialize_subject(subject: ConnectTokenSubject) -> JSONObject:
    body: dict[str, Any] = {"type": subject.type}
    match subject:
        case ConnectJwtBearerTokenSubject():
            body["sub"] = subject.sub
            if subject.iss is not None:
                body["iss"] = subject.iss
            if subject.aud is not None:
                body["aud"] = subject.aud
            if subject.additional_claims is not None:
                body["additionalClaims"] = dict(subject.additional_claims)
        case _:
            for name, wire_name in (("id", "id"), ("issuer", "issuer"), ("token", "token")):
                value = getattr(subject, name, None)
                if value is not None:
                    body[wire_name] = value
    return body


def _serialize_authorization_detail(detail: ConnectAuthorizationDetail) -> JSONObject:
    match detail:
        case ConnectGitHubAppInstallationAuthorizationDetail():
            body: dict[str, Any] = {"type": detail.type}
            if detail.org is not None:
                body["org"] = detail.org
            if detail.permissions is not None:
                body["permissions"] = list(detail.permissions)
            if detail.repositories is not None:
                body["repositories"] = list(detail.repositories)
            return body
        case ConnectCustomAuthorizationDetail():
            return {"type": detail.type, **dict(detail.details)}


def _raise_api_error(response: Response) -> None:
    message, data = extract_structured_error(response)
    # The shared extractor only understands the `error` envelope, but Connect also
    # answers with `err`, so recover that message rather than losing it.
    envelope_message = _error_message(data)
    if envelope_message and envelope_message not in message:
        message = f"{message}: {envelope_message}"
    code = _error_code(data)
    error_class = _ERROR_CLASSES.get(code or "", ConnectApiError)
    raise error_class(
        response,
        message,
        code=code,
        vendor=_error_vendor(data),
        data=data,
    )


def _error_envelope(data: object) -> Mapping[str, Any] | None:
    if not isinstance(data, Mapping):
        return None
    # The key is `error` or `err` depending on the endpoint.
    for name in ("error", "err"):
        envelope = data.get(name)
        if isinstance(envelope, Mapping):
            return envelope
    return None


def _error_message(data: object) -> str | None:
    envelope = _error_envelope(data)
    if envelope is None:
        return None
    for name in ("message", "msg"):
        message = envelope.get(name)
        if isinstance(message, str) and message:
            return message
    return None


def _error_code(data: object) -> str | None:
    envelope = _error_envelope(data)
    if envelope is None:
        return None
    code = envelope.get("code")
    return code if isinstance(code, str) and code else None


def _error_vendor(data: object) -> JSONObject | None:
    envelope = _error_envelope(data)
    # `vendor` appears at `error.vendor`, `error.meta.vendor`, or the top level.
    if envelope is not None:
        vendor = envelope.get("vendor")
        if isinstance(vendor, Mapping):
            return dict(vendor)
        meta = envelope.get("meta")
        if isinstance(meta, Mapping) and isinstance(meta.get("vendor"), Mapping):
            return dict(meta["vendor"])
    if isinstance(data, Mapping) and isinstance(data.get("vendor"), Mapping):
        return dict(data["vendor"])
    return None


class ConnectApiClient:
    """Wire-level access to the Vercel Connect API.

    Owns request/response models, camelCase aliasing, the user agent, and the
    mapping of non-2xx responses onto the `ConnectApiError` taxonomy.
    """

    def __init__(
        self,
        *,
        base_url: str,
        credentials_factory: ConnectCredentialsFactory,
        transport: BaseTransport,
        timeout: timedelta,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._credentials_factory = credentials_factory
        self._transport = transport
        self._timeout = timeout

    async def create_token(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        vercel_token: str,
        scopes: Sequence[str] | None = None,
        installation_id: str | None = None,
        audience: Sequence[str] | None = None,
        resources: Sequence[str] | None = None,
        authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
    ) -> ConnectTokenState:
        """POST /v1/connect/token/:connector."""
        body: dict[str, Any] = {"subject": _serialize_subject(subject)}
        if scopes is not None:
            body["scopes"] = list(scopes)
        if installation_id is not None:
            body["installationId"] = installation_id
        if audience is not None:
            body["audience"] = list(audience)
        if resources is not None:
            body["resources"] = list(resources)
        if authorization_details is not None:
            body["authorizationDetails"] = [
                _serialize_authorization_detail(detail) for detail in authorization_details
            ]

        response = await self._request(
            "POST",
            f"/v1/connect/token/{_encode_connector(connector)}",
            vercel_token=vercel_token,
            body=JSONBody(body),
        )
        return self._parse(response, _TokenResponseModel).to_state()

    async def revoke_token(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        vercel_token: str,
        installation_id: str | None = None,
    ) -> None:
        """DELETE /v1/connect/connectors/:connector/tokens."""
        body: dict[str, Any] = {"subject": _serialize_subject(subject)}
        if installation_id is not None:
            body["installationId"] = installation_id

        # Revocation may answer with an empty body, so nothing is parsed.
        await self._request(
            "DELETE",
            f"/v1/connect/connectors/{_encode_connector(connector)}/tokens",
            vercel_token=vercel_token,
            body=JSONBody(body),
        )

    async def create_authorization(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        vercel_token: str,
        scopes: Sequence[str] | None = None,
        installation_id: str | None = None,
        return_url: str | None = None,
        webhook: str | None = None,
        device_code: bool | None = None,
        expires_in: timedelta | None = None,
    ) -> ConnectAuthorizationState:
        """POST /v1/connect/authorize/:connector."""
        body: dict[str, Any] = {"subject": _serialize_subject(subject)}
        if scopes is not None:
            body["scopes"] = list(scopes)
        if installation_id is not None:
            body["installationId"] = installation_id
        if return_url is not None:
            body["returnUrl"] = return_url
        if webhook is not None:
            body["webhook"] = webhook
        if device_code is not None:
            body["deviceCode"] = device_code
        if expires_in is not None:
            body["expiresInMs"] = to_ms_int(expires_in)

        response = await self._request(
            "POST",
            f"/v1/connect/authorize/{_encode_connector(connector)}",
            vercel_token=vercel_token,
            body=JSONBody(body),
        )
        return self._parse(response, _AuthorizationResponseModel).to_state()

    async def get_connector(
        self,
        connector: str,
        *,
        vercel_token: str,
    ) -> ConnectorMetadataState:
        """GET /v1/connect/connectors/:connector."""
        response = await self._request(
            "GET",
            f"/v1/connect/connectors/{_encode_connector(connector)}",
            vercel_token=vercel_token,
        )
        return self._parse(response, _ConnectorMetadataModel).to_state()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        vercel_token: str,
        body: RequestBody = None,
    ) -> Response:
        response = await self._transport.send(
            method,
            f"{self._base_url}{path}",
            token=vercel_token,
            body=body,
            headers={"user-agent": USER_AGENT},
            timeout=self._timeout,
            read_response=ReadResponsePolicy.ALWAYS,
        )
        if not response.is_success:
            _raise_api_error(response)
        return response

    def _parse(self, response: Response, model: type[ResponseModelT]) -> ResponseModelT:
        try:
            payload = response.json()
        except Exception as exc:
            raise ConnectResponseError("Connect API returned a non-JSON success response") from exc
        try:
            return model.model_validate(payload)
        except ValidationError as exc:
            raise ConnectResponseError(
                "Connect API returned a malformed success response", data=payload
            ) from exc


__all__ = ["ConnectApiClient", "USER_AGENT"]
