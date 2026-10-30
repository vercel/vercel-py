"""Public value types for the Sandbox API."""

from collections.abc import Iterable, Mapping
from datetime import timedelta
from enum import IntEnum
from pathlib import PurePosixPath
from subprocess import CalledProcessError
from types import MappingProxyType
from typing import (
    Annotated,
    Any,
    ClassVar,
    Final,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
)

from pydantic import (
    AfterValidator,
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue as PydanticJsonValue,
    ModelWrapValidatorHandler,
    SkipValidation,
    StrictBytes,
    TypeAdapter,
    ValidationError,
    ValidationInfo,
    WrapSerializer,
    field_serializer,
    field_validator,
    model_validator,
)

from vercel._internal.core.polyfills import Self, StrEnum
from vercel._internal.core.time import SECOND, coerce_duration, to_ms_int
from vercel.sandbox._internal.value_model import SandboxValueModel

JSONValue: TypeAlias = PydanticJsonValue
JSONObject: TypeAlias = dict[str, JSONValue]
PrivateSandboxParameters: TypeAlias = Mapping[str, JSONValue]
NO_PRIVATE_PARAMETERS: Final[PrivateSandboxParameters] = MappingProxyType({})
DurationInput: TypeAlias = int | float | timedelta | None
FailoverRegionsInput: TypeAlias = Iterable[str] | None
RemotePath: TypeAlias = str | PurePosixPath
_MIN_SNAPSHOT_EXPIRATION = timedelta(days=1)
_MAX_SNAPSHOT_EXPIRATION = timedelta(days=365 * 10)
_ZERO_DELTA = timedelta(0)


def normalize_private_parameters(
    operation: str, parameters: PrivateSandboxParameters
) -> JSONObject:
    """Validate and return private Sandbox API parameters."""
    for name in parameters:
        if not name.startswith("__"):
            raise TypeError(f"{operation}() got an unexpected keyword argument {name!r}")
    return dict(parameters)


def normalize_failover_regions(regions: FailoverRegionsInput) -> tuple[str, ...] | None:
    """Normalize sandbox failover regions while preserving explicit emptiness."""
    if regions is None:
        return None
    normalized = tuple(regions)
    if any(not isinstance(region, str) or not region for region in normalized):
        raise ValueError("failover_regions must contain non-empty strings")
    if len(set(normalized)) != len(normalized):
        raise ValueError("failover_regions must not contain duplicates")
    return normalized


_MappingValueT = TypeVar("_MappingValueT")
_FrozenMapping: TypeAlias = Annotated[
    Mapping[str, _MappingValueT],
    # Pydantic's Mapping validation already yields a fresh dict; just freeze it.
    AfterValidator(MappingProxyType),
    WrapSerializer(lambda value, handler: handler(dict(value))),
]


class NetworkPolicyMatcher(SandboxValueModel):
    """Match one request value using one comparison strategy."""

    kind: Literal["exact", "starts_with", "regex"]
    value: str
    __hash__ = None  # type: ignore[assignment]

    def __init__(self, kind: Literal["exact", "starts_with", "regex"], value: str) -> None:
        super().__init__(kind=kind, value=value)

    def to_api_dict(self) -> JSONObject:
        key = "startsWith" if self.kind == "starts_with" else self.kind
        return {key: self.value}

    @classmethod
    def exact(cls, value: str) -> "NetworkPolicyMatcher":
        return cls("exact", value)

    @classmethod
    def starts_with(cls, value: str) -> "NetworkPolicyMatcher":
        return cls("starts_with", value)

    @classmethod
    def regex(cls, value: str) -> "NetworkPolicyMatcher":
        return cls("regex", value)


class NetworkPolicyKeyValueMatcher(SandboxValueModel):
    """Match a request header or query-string key/value pair."""

    key: NetworkPolicyMatcher | None = None
    value: NetworkPolicyMatcher | None = None
    __hash__ = None  # type: ignore[assignment]

    def to_api_dict(self) -> JSONObject:
        result: JSONObject = {}
        if self.key is not None:
            result["key"] = self.key.to_api_dict()
        if self.value is not None:
            result["value"] = self.value.to_api_dict()
        return result


class NetworkPolicyRequestMatcher(SandboxValueModel):
    """Match selected dimensions of an outbound HTTP request."""

    path: NetworkPolicyMatcher | None = None
    method: tuple[str, ...] | None = None
    query: tuple[NetworkPolicyKeyValueMatcher, ...] | None = None
    headers: tuple[NetworkPolicyKeyValueMatcher, ...] | None = None
    __hash__ = None  # type: ignore[assignment]

    def to_api_dict(self) -> JSONObject:
        result: JSONObject = {}
        if self.path is not None:
            result["path"] = self.path.to_api_dict()
        if self.method is not None:
            result["method"] = list(self.method)
        if self.query is not None:
            result["queryString"] = [item.to_api_dict() for item in self.query]
        if self.headers is not None:
            result["headers"] = [item.to_api_dict() for item in self.headers]
        return result


class NetworkPolicyTransform(SandboxValueModel):
    """Inject authored headers into matching requests."""

    headers: _FrozenMapping[str] | None = None
    __hash__ = None  # type: ignore[assignment]

    def to_api_dict(self) -> JSONObject:
        return {} if self.headers is None else {"headers": dict(self.headers)}


class NetworkPolicyResponse(SandboxValueModel):
    """Response returned by the network proxy without contacting the origin."""

    status_code: int
    headers: _FrozenMapping[str] | None = None
    body: str | None = None
    content_type: str | None = None
    __hash__ = None  # type: ignore[assignment]

    @field_validator("status_code", mode="before")
    @classmethod
    def _validate_status_code(cls, value: object) -> int:
        if type(value) is not int or not 200 <= value <= 599:
            raise ValueError("response status_code must be an integer between 200 and 599")
        return value

    # Pydantic lets TypeError propagate un-wrapped, preserving the pre-migration error type
    # (and rejecting bytes that lax ``str`` validation would otherwise decode).
    @field_validator("body", "content_type", mode="before")
    @classmethod
    def _require_str(cls, value: object, info: ValidationInfo) -> object:
        if value is not None and not isinstance(value, str):
            raise TypeError(f"response {info.field_name} must be a string")
        return value

    @field_validator("headers", mode="before")
    @classmethod
    def _require_str_headers(cls, value: object) -> object:
        if value is not None and any(
            not isinstance(name, str) or not isinstance(item, str)
            for name, item in dict(value).items()  # type: ignore[call-overload]
        ):
            raise TypeError("response headers must map strings to strings")
        return value

    @model_validator(mode="after")
    def _validate(self) -> Self:
        if self.body is not None and self.status_code in {204, 205, 304}:
            raise ValueError("response body is not allowed for status codes 204, 205, and 304")
        if self.body is not None and self.content_type is None:
            raise ValueError("response content_type is required when body is set")
        names = [name.lower() for name in self.headers or ()]
        if len(names) != len(set(names)):
            raise ValueError("response headers cannot contain duplicate names")
        proxy_managed = {"connection", "content-length", "transfer-encoding"}
        if any(name in proxy_managed for name in names):
            raise ValueError("response headers cannot set proxy-managed headers")
        return self

    def to_api_dict(self) -> JSONObject:
        result: JSONObject = {"statusCode": self.status_code}
        if self.headers is not None:
            result["headers"] = dict(self.headers)
        if self.body is not None:
            result["body"] = self.body
        if self.content_type is not None:
            result["contentType"] = self.content_type
        return result


class NetworkPolicyRule(SandboxValueModel):
    """Configure one matched request action for a domain."""

    transform: tuple[NetworkPolicyTransform, ...] = ()
    match: NetworkPolicyRequestMatcher | None = None
    forward_url: str | None = None
    response: NetworkPolicyResponse | None = None
    __hash__ = None  # type: ignore[assignment]

    @model_validator(mode="after")
    def _validate(self) -> Self:
        handlers = sum(
            (
                bool(self.transform)
                or (
                    "transform" in self.model_fields_set
                    and self.forward_url is None
                    and self.response is None
                ),
                self.forward_url is not None,
                self.response is not None,
            )
        )
        if handlers != 1:
            raise ValueError(
                "network policy rule requires exactly one of transform, forward_url, or response"
            )
        return self

    def to_api_dict(self) -> JSONObject:
        result: JSONObject = {}
        if self.match is not None:
            result["match"] = self.match.to_api_dict()
        if self.transform or (self.forward_url is None and self.response is None):
            result["transform"] = [item.to_api_dict() for item in self.transform]
        if self.forward_url is not None:
            result["forwardURL"] = self.forward_url
        if self.response is not None:
            result["response"] = self.response.to_api_dict()
        return result


NetworkPolicyMatchDimension: TypeAlias = Literal["path", "method", "query", "headers"]


class _NetworkPolicyRuleSummary(SandboxValueModel):
    """Common redacted response metadata, including API field aliases."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    domain: str
    match_dimensions: tuple[NetworkPolicyMatchDimension, ...] = Field(
        default=(), alias="matchDimensions"
    )

    @field_validator("match_dimensions", mode="before")
    @classmethod
    def _normalize_dimensions(cls, value: object) -> object:
        if value is None:
            return ()
        if not isinstance(value, (list, tuple)):
            raise ValueError("matchDimensions must be an array")
        dimensions = tuple("query" if item == "queryString" else item for item in value)
        if len(dimensions) != len(set(dimensions)):
            raise ValueError("matchDimensions must not contain duplicates")
        return dimensions


class NetworkPolicyInjectionRuleSummary(_NetworkPolicyRuleSummary):
    """Redacted header-injection rule metadata returned by the Sandbox API."""

    header_names: tuple[str, ...] = Field(default=(), alias="headerNames")

    @model_validator(mode="before")
    @classmethod
    def _redact_headers(cls, value: Any) -> Any:
        if not isinstance(value, Mapping) or "headers" not in value:
            return value
        if "headerNames" in value or "header_names" in value:
            raise ValueError("injection rule cannot set headers and headerNames")
        if not isinstance(value["headers"], Mapping):
            raise ValueError("injection rule headers must be an object")
        return {**value, "headerNames": tuple(value["headers"])}


class NetworkPolicyForwardRuleSummary(_NetworkPolicyRuleSummary):
    """Redacted forward rule metadata returned by the Sandbox API."""

    forward_url: str = Field(
        validation_alias=AliasChoices("forward_url", "forwardURL", "forwardUrl")
    )


class NetworkPolicyResponseRuleSummary(_NetworkPolicyRuleSummary):
    """Redacted direct-response rule metadata returned by the Sandbox API."""

    status_code: int = Field(alias="statusCode")

    @field_validator("status_code", mode="before")
    @classmethod
    def _validate_status_code(cls, value: object) -> int:
        if type(value) is not int or not 200 <= value <= 599:
            raise ValueError("response rule summary status_code must be between 200 and 599")
        return value


class NetworkPolicySubnets(SandboxValueModel):
    """Configure allowed and denied network ranges."""

    allow: tuple[str, ...] | None = None
    deny: tuple[str, ...] | None = None
    __hash__ = None  # type: ignore[assignment]

    def to_api_dict(self) -> JSONObject:
        result: JSONObject = {}
        if self.allow is not None:
            result["allow"] = list(self.allow)
        if self.deny is not None:
            result["deny"] = list(self.deny)
        return result


class NetworkPolicy(SandboxValueModel):
    """Complete outbound policy authored for submission to the Sandbox API."""

    mode: Literal["allow-all", "deny-all", "custom"]
    allow: _FrozenMapping[tuple[NetworkPolicyRule, ...]]
    subnets: NetworkPolicySubnets | None = None
    __hash__ = None  # type: ignore[assignment]

    @classmethod
    def allow_all(cls) -> "NetworkPolicy":
        return cls(mode="allow-all", allow=MappingProxyType({}))

    @classmethod
    def deny_all(cls) -> "NetworkPolicy":
        return cls(mode="deny-all", allow=MappingProxyType({}))

    @classmethod
    def custom(
        cls,
        allow: (
            Mapping[str, Iterable[NetworkPolicyRule]]
            | Iterable[tuple[str, Iterable[NetworkPolicyRule]]]
        ) = (),
        subnets: NetworkPolicySubnets | None = None,
    ) -> "NetworkPolicy":
        # The field validator copies and freezes; only pair iterables need converting.
        mapping = allow if isinstance(allow, Mapping) else dict(allow)
        return cls(mode="custom", allow=mapping, subnets=subnets)

    @model_validator(mode="after")
    def _validate(self) -> Self:
        if self.mode != "custom" and (self.allow or self.subnets is not None):
            raise ValueError("simple network policy modes cannot include custom rules")
        return self

    @classmethod
    def from_json_value(cls, value: object) -> "NetworkPolicy":
        """Validate decoded SDK/CLI policy JSON, rejecting unknown keys recursively.

        Unlike model_validate(), this consumes the shared JSON format rather
        than Python field names. Validation errors may retain input values;
        use parse_network_policy() for credential-safe diagnostics.
        """
        if isinstance(value, str):
            if value == "allow-all":
                return cls.allow_all()
            if value == "deny-all":
                return cls.deny_all()
        return _NetworkPolicyObject.model_validate(
            value, context={"network_policy_request": True}
        ).to_value()

    def to_api_dict(self) -> JSONObject:
        """Serialize an authored policy for Sandbox API requests."""
        if self.mode != "custom":
            return {"mode": self.mode}
        result: JSONObject = {
            "allow": {
                domain: [rule.to_api_dict() for rule in rules]
                for domain, rules in self.allow.items()
            }
        }
        if self.subnets is not None:
            result["subnets"] = self.subnets.to_api_dict()
        return result


class NetworkPolicyReadback(SandboxValueModel):
    """Incomplete policy metadata; cannot be submitted as a network policy."""

    mode: Literal["allow-all", "deny-all", "custom"]
    allowed_domains: tuple[str, ...] = ()
    subnets: NetworkPolicySubnets | None = None
    injection_rules: tuple[NetworkPolicyInjectionRuleSummary, ...] = ()
    forward_rules: tuple[NetworkPolicyForwardRuleSummary, ...] = ()
    response_rules: tuple[NetworkPolicyResponseRuleSummary, ...] = ()

    @classmethod
    def from_api_dict(cls, value: object) -> "NetworkPolicyReadback":
        """Parse API response metadata, tolerating additional response fields.

        Readbacks intentionally contain neither credential values nor enough
        information to recreate an authored request policy.
        """
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            if "allow" in value or "subnets" in value:
                policy = _NetworkPolicyObject.model_validate(value).to_value()
                return cls(
                    mode="custom", allowed_domains=tuple(policy.allow), subnets=policy.subnets
                )
            if value.get("mode") == "allow-all":
                return cls(mode="allow-all")
            if value.get("mode") == "deny-all":
                return cls(mode="deny-all")
        return _NormalizedNetworkPolicyReadback.model_validate(value).to_value()


class _NetworkPolicyInput(BaseModel):
    # Requests and domain-map readbacks share this schema. Response parsing
    # accepts additional fields; requests reject them, including legacy aliases.
    _response_nullable_fields: ClassVar[frozenset[str]] = frozenset()
    model_config = ConfigDict(extra="ignore", strict=True, hide_input_in_errors=True)

    @model_validator(mode="wrap")
    @classmethod
    def _validate_keys(
        cls, value: Any, handler: ModelWrapValidatorHandler[Self], info: ValidationInfo
    ) -> Self:
        if not (info.context or {}).get("network_policy_request"):
            return handler(value)
        errors: list[Any] = []
        result = None
        try:
            result = handler(value)
        except ValidationError as exc:
            errors.extend(exc.errors(include_url=False))
        if isinstance(value, Mapping):
            names = {field.alias or name for name, field in cls.model_fields.items()}
            errors.extend(
                {"type": "extra_forbidden", "loc": (key,), "input": None}
                for key in value
                if key not in names
            )
        if errors:
            raise ValidationError.from_exception_data(cls.__name__, errors)
        assert result is not None
        return result

    @field_validator("*", mode="before")
    @classmethod
    def _reject_null(cls, value: object, info: ValidationInfo) -> object:
        # Requests distinguish omission from null. Responses retain the API's
        # historical nullable fields without weakening validation elsewhere.
        request = (info.context or {}).get("network_policy_request")
        if value is None and (request or info.field_name not in cls._response_nullable_fields):
            raise ValueError("must not be null")
        return value

    @model_validator(mode="after")
    def _validate_value(self) -> Self:
        # Reuse authored-model invariants; Pydantic preserves the nested wire path.
        self.to_value()
        return self

    def to_value(self) -> object:
        raise NotImplementedError


class _NetworkPolicyMatcherInput(_NetworkPolicyInput):
    exact: str | None = None
    starts_with: str | None = Field(default=None, alias="startsWith")
    regex: str | None = None

    def to_value(self) -> NetworkPolicyMatcher:
        if sum(v is not None for v in (self.exact, self.starts_with, self.regex)) != 1:
            raise ValueError("matcher requires exactly one of exact, startsWith, or regex")
        if self.exact is not None:
            return NetworkPolicyMatcher.exact(self.exact)
        if self.starts_with is not None:
            return NetworkPolicyMatcher.starts_with(self.starts_with)
        assert self.regex is not None
        return NetworkPolicyMatcher.regex(self.regex)


class _NetworkPolicyPairInput(_NetworkPolicyInput):
    key: _NetworkPolicyMatcherInput | None = None
    value: _NetworkPolicyMatcherInput | None = None

    def to_value(self) -> NetworkPolicyKeyValueMatcher:
        return NetworkPolicyKeyValueMatcher(
            key=None if self.key is None else self.key.to_value(),
            value=None if self.value is None else self.value.to_value(),
        )


class _NetworkPolicyMatchInput(_NetworkPolicyInput):
    _response_nullable_fields = frozenset({"method", "query", "headers"})

    path: _NetworkPolicyMatcherInput | None = None
    method: list[str] | None = None
    query: list[_NetworkPolicyPairInput] | None = Field(
        default=None, alias="queryString", validation_alias=AliasChoices("queryString", "query")
    )
    headers: list[_NetworkPolicyPairInput] | None = None

    def to_value(self) -> NetworkPolicyRequestMatcher:
        return NetworkPolicyRequestMatcher(
            path=None if self.path is None else self.path.to_value(),
            method=None if self.method is None else tuple(self.method),
            query=None if self.query is None else tuple(pair.to_value() for pair in self.query),
            headers=(
                None if self.headers is None else tuple(pair.to_value() for pair in self.headers)
            ),
        )


class _NetworkPolicyTransformInput(_NetworkPolicyInput):
    headers: dict[str, str] | None = None

    def to_value(self) -> NetworkPolicyTransform:
        return NetworkPolicyTransform(headers=self.headers)


class _NetworkPolicyResponseInput(_NetworkPolicyInput):
    _response_nullable_fields = frozenset({"headers"})

    status_code: int = Field(alias="statusCode", ge=200, le=599)
    headers: dict[str, str] | None = None
    body: str | None = None
    content_type: str | None = Field(default=None, alias="contentType")

    @field_validator("status_code", mode="before")
    @classmethod
    def _normalize_integer(cls, value: object) -> object:
        # JSON numbers such as 403.0 are integers to the JS SDK's validator.
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    def to_value(self) -> NetworkPolicyResponse:
        return NetworkPolicyResponse(
            status_code=self.status_code,
            headers=self.headers,
            body=self.body,
            content_type=self.content_type,
        )


class _NetworkPolicyRuleInput(_NetworkPolicyInput):
    _response_nullable_fields = frozenset({"match", "forward_url", "response"})

    match: _NetworkPolicyMatchInput | None = None
    transform: list[_NetworkPolicyTransformInput] | None = None
    forward_url: str | None = Field(
        default=None, alias="forwardURL", validation_alias=AliasChoices("forwardURL", "forwardUrl")
    )
    response: _NetworkPolicyResponseInput | None = None

    def to_value(self) -> NetworkPolicyRule:
        if sum(v is not None for v in (self.transform, self.forward_url, self.response)) != 1:
            raise ValueError("rule requires exactly one of transform, forwardURL, or response")
        return NetworkPolicyRule(
            match=None if self.match is None else self.match.to_value(),
            forward_url=self.forward_url,
            response=None if self.response is None else self.response.to_value(),
            **(
                {}
                if self.transform is None
                else {"transform": tuple(t.to_value() for t in self.transform)}
            ),
        )


class _NetworkPolicySubnetsInput(_NetworkPolicyInput):
    _response_nullable_fields = frozenset({"allow", "deny"})

    allow: list[str] | None = None
    deny: list[str] | None = None

    def to_value(self) -> NetworkPolicySubnets:
        return NetworkPolicySubnets(
            allow=None if self.allow is None else tuple(self.allow),
            deny=None if self.deny is None else tuple(self.deny),
        )


class _NetworkPolicyObject(_NetworkPolicyInput):
    allow: dict[str, list[_NetworkPolicyRuleInput]] | None = None
    subnets: _NetworkPolicySubnetsInput | None = None

    @field_validator("allow", mode="before")
    @classmethod
    def _normalize_domains(cls, value: object) -> object:
        if isinstance(value, list):
            domains = TypeAdapter(list[str]).validate_python(value, strict=True)
            return {domain: [] for domain in domains}
        return value

    def to_value(self) -> NetworkPolicy:
        return NetworkPolicy.custom(
            allow={
                domain: tuple(rule.to_value() for rule in rules)
                for domain, rules in (self.allow or {}).items()
            },
            subnets=None if self.subnets is None else self.subnets.to_value(),
        )


class _NormalizedNetworkPolicyReadback(BaseModel):
    """Normalized API response fields, whose rules are redacted summaries."""

    model_config = ConfigDict(extra="ignore", strict=True)

    mode: Literal["custom"]
    allowed_domains: list[str] | None = Field(default=None, alias="allowedDomains")
    allowed_cidrs: list[str] | None = Field(default=None, alias="allowedCIDRs")
    denied_cidrs: list[str] | None = Field(default=None, alias="deniedCIDRs")
    injection_rules: list[NetworkPolicyInjectionRuleSummary] = Field(
        default_factory=list, alias="injectionRules"
    )
    forward_rules: list[NetworkPolicyForwardRuleSummary] = Field(
        default_factory=list, validation_alias=AliasChoices("forwardRules", "forwardingRules")
    )
    response_rules: list[NetworkPolicyResponseRuleSummary] = Field(
        default_factory=list, alias="responseRules"
    )

    def to_value(self) -> NetworkPolicyReadback:
        domains = dict.fromkeys(self.allowed_domains or ())
        for rule in (*self.injection_rules, *self.forward_rules, *self.response_rules):
            domains.setdefault(rule.domain, None)
        subnets = None
        if self.allowed_cidrs is not None or self.denied_cidrs is not None:
            subnets = NetworkPolicySubnets(allow=self.allowed_cidrs, deny=self.denied_cidrs)
        return NetworkPolicyReadback(
            mode=self.mode,
            allowed_domains=tuple(domains),
            subnets=subnets,
            injection_rules=tuple(self.injection_rules),
            forward_rules=tuple(self.forward_rules),
            response_rules=tuple(self.response_rules),
        )


class SnapshotExpiration(SandboxValueModel):
    """Represent a platform-side snapshot lifetime.

    Args:
        value: Lifetime in seconds or as a ``timedelta``. Zero disables
            expiration; nonzero values must be between one day and ten years.

    Raises:
        ValueError: If the lifetime is outside the supported range.
    """

    value: timedelta

    def __init__(self, value: int | float | timedelta) -> None:
        super().__init__(value=value)

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_value(cls, value: object) -> object:
        if isinstance(value, (int, float, timedelta)):
            return coerce_duration(value, SECOND)
        return value

    @field_validator("value")
    @classmethod
    def _validate_value(cls, value: timedelta) -> timedelta:
        if value != _ZERO_DELTA and not (
            _MIN_SNAPSHOT_EXPIRATION <= value <= _MAX_SNAPSHOT_EXPIRATION
        ):
            raise ValueError(
                "Snapshot expiration must be 0 or between one day and ten years inclusive"
            )
        return value


SnapshotExpirationInput: TypeAlias = int | float | timedelta | SnapshotExpiration | None


def _parse_snapshot_expiration(value: object) -> SnapshotExpiration | None:
    match value:
        case None | SnapshotExpiration():
            return value
        case int() | float() | timedelta():
            return SnapshotExpiration(value)
        case _:
            raise TypeError(
                "snapshot expiration must be an int, float, timedelta, SnapshotExpiration, or None"
            )


class _Omitted:
    __slots__ = ()


_OMITTED = _Omitted()


class _WriteFile(SandboxValueModel):
    path: str
    # Strict so bytes-like buffers are kept as-is (no copy) and ``str`` is rejected.
    content: StrictBytes | bytearray | memoryview
    # Validated by ``_validate_file_mode`` so callers keep its TypeError/ValueError.
    mode: SkipValidation[int | None] = None


class DirectoryEntry(SandboxValueModel):
    """Describe one entry returned by a sandbox directory listing.

    Attributes:
        path: Entry name relative to the listed directory, not a full path.
        kind: Filesystem entry type.
    """

    path: str
    kind: Literal["file", "directory", "symlink", "other"]


class DriveHandle(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def project_id(self) -> str: ...

    @property
    def region(self) -> str: ...


DriveReference: TypeAlias = str | DriveHandle


DriveMountMode: TypeAlias = Literal["snapshot", "read-write"]


class DriveMount(SandboxValueModel):
    """Configure how a Drive is mounted in a sandbox."""

    drive: SkipValidation[DriveReference]
    mode: DriveMountMode = "read-write"

    def __init__(self, drive: DriveReference, mode: DriveMountMode = "read-write") -> None:
        super().__init__(drive=drive, mode=mode)

    @field_serializer("drive")
    def _serialize_drive(self, drive: DriveReference) -> str:
        return drive if isinstance(drive, str) else drive.name

    def to_api_dict(self) -> JSONObject:
        return self.model_dump()


class SandboxMount(SandboxValueModel):
    """Drive mount returned as part of a sandbox's actual configuration."""

    drive: str
    mode: DriveMountMode


DriveMountInput: TypeAlias = DriveMount | DriveReference
_RemotePathT = TypeVar("_RemotePathT", bound=RemotePath)
DriveMountsInput: TypeAlias = Mapping[_RemotePathT, DriveMountInput]
_RESERVED_DRIVE_MOUNT_PATHS = frozenset({"/run/cell", "/run/vercel/share"})


def _canonicalize_drive_mount_path(path: RemotePath) -> PurePosixPath:
    if not isinstance(path, (str, PurePosixPath)):
        raise TypeError("Drive mount paths must be strings or PurePosixPath instances")
    path = str(path)
    if not path.startswith("/"):
        raise ValueError("Drive mount paths must be absolute")
    if "\x00" in path:
        raise ValueError("Drive mount paths cannot contain NUL characters")

    parts = path.split("/")
    if ".." in parts:
        raise ValueError("Drive mount paths cannot contain '..' components")
    canonical = "/" + "/".join(part for part in parts if part not in {"", "."})
    if canonical == "/":
        raise ValueError("Drive mount paths cannot target the filesystem root")
    if len(canonical) > 256:
        raise ValueError("Drive mount paths cannot exceed 256 characters")
    if canonical in _RESERVED_DRIVE_MOUNT_PATHS:
        raise ValueError(f"Drive mount path {canonical!r} is reserved")
    return PurePosixPath(canonical)


def _serialize_drive_mounts(
    mounts: DriveMountsInput[_RemotePathT] | None,
    *,
    project_id: str,
    region: str | None,
    failover_regions: tuple[str, ...] | None,
) -> dict[str, JSONObject] | None:
    if mounts is None:
        return None
    mount_entries = tuple(mounts.items())
    if len(mount_entries) > 4:
        raise ValueError("A sandbox can mount at most 4 Drives")

    paths = [_canonicalize_drive_mount_path(path) for path, _ in mount_entries]
    if any(
        left == right or left in right.parents or right in left.parents
        for index, left in enumerate(paths)
        for right in paths[index + 1 :]
    ):
        raise ValueError("Drive mount paths cannot overlap")

    serialized: dict[str, JSONObject] = {}
    drive_names: set[str] = set()
    drive_regions: set[str] = set()
    for path, (_, value) in zip(paths, mount_entries, strict=True):
        mount = value if isinstance(value, DriveMount) else DriveMount(value)
        drive = mount.drive
        name = drive if isinstance(drive, str) else drive.name
        if name in drive_names:
            raise ValueError(f"Drive {name!r} can be mounted only once")
        drive_names.add(name)
        if not isinstance(drive, str):
            if project_id.startswith("prj_") and drive.project_id != project_id:
                raise ValueError(
                    f"Drive {drive.name!r} belongs to project {drive.project_id!r}, "
                    f"not sandbox project {project_id!r}"
                )
            drive_regions.add(drive.region)
        serialized[str(path)] = mount.to_api_dict()

    if len(drive_regions) > 1:
        raise ValueError("All mounted Drives must use the same region")
    if region is not None and drive_regions and region != next(iter(drive_regions)):
        drive_region = next(iter(drive_regions))
        raise ValueError(f"Sandbox region {region!r} does not match Drive region {drive_region!r}")
    return serialized


class SandboxStatus(StrEnum):
    """Lifecycle status reported for a sandbox or runtime session."""

    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


class CommandStatus(StrEnum):
    """Lifecycle status derived for a sandbox command."""

    RUNNING = "running"
    EXITED = "exited"


class CommandSignal(IntEnum):
    """Signals supported by remote Linux sandbox commands."""

    SIGHUP = 1
    SIGINT = 2
    SIGQUIT = 3
    SIGILL = 4
    SIGTRAP = 5
    SIGABRT = 6
    SIGIOT = 6
    SIGBUS = 7
    SIGFPE = 8
    SIGKILL = 9
    SIGUSR1 = 10
    SIGSEGV = 11
    SIGUSR2 = 12
    SIGPIPE = 13
    SIGALRM = 14
    SIGTERM = 15
    SIGSTKFLT = 16
    SIGCHLD = 17
    SIGCLD = 17
    SIGCONT = 18
    SIGSTOP = 19
    SIGTSTP = 20
    SIGTTIN = 21
    SIGTTOU = 22
    SIGURG = 23
    SIGXCPU = 24
    SIGXFSZ = 25
    SIGVTALRM = 26
    SIGPROF = 27
    SIGWINCH = 28
    SIGIO = 29
    SIGPOLL = 29
    SIGPWR = 30
    SIGSYS = 31


class CompletedCommand(SandboxValueModel):
    """The captured result of one completed remote command."""

    id: str
    name: str
    args: tuple[str, ...]
    cwd: str
    session_id: str
    started_at: int
    returncode: int
    stdout: str | None
    stderr: str | None

    def check_returncode(self) -> None:
        """Raise an error when the process exited unsuccessfully.

        Raises:
            subprocess.CalledProcessError: If ``returncode`` is nonzero.
        """
        if self.returncode:
            raise CalledProcessError(
                self.returncode,
                list(self.args),
                output=self.stdout,
                stderr=self.stderr,
            )


class _InputModel(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        populate_by_name=True,
        serialize_by_alias=True,
    )

    def to_api_dict(self, *, exclude: set[str] | None = None) -> JSONObject:
        """Serialize this input model using Sandbox API field names."""
        return cast(
            JSONObject,
            self.model_dump(by_alias=True, exclude_none=True, exclude=exclude or set()),
        )


class GitSource(_InputModel):
    """Configure a Git repository as sandbox source.

    ``username`` and ``password`` may be used for repositories that require
    HTTP basic authentication.
    """

    type: Literal["git"] = "git"
    url: str
    depth: int | None = None
    revision: str | None = None
    username: str | None = None
    password: str | None = None


class TarballSource(_InputModel):
    """Configure a remotely accessible tarball as sandbox source."""

    type: Literal["tarball"] = "tarball"
    url: str


class SnapshotSource(_InputModel):
    """Configure an existing snapshot as sandbox source."""

    type: Literal["snapshot"] = "snapshot"
    snapshot_id: str = Field(serialization_alias="snapshotId")


SandboxSource: TypeAlias = GitSource | TarballSource | SnapshotSource


class SandboxResources(_InputModel):
    """Configure CPU and memory requested for a sandbox."""

    vcpus: int | None = None
    memory: int | None = None


class SnapshotRetention(_InputModel):
    """Configure automatic snapshot retention.

    Attributes:
        count: Maximum number of retained snapshots, between 1 and 100.
        expiration: Lifetime applied to retained snapshots.
        delete_evicted: Whether snapshots removed from the retention window are
            deleted from the project.
    """

    count: int
    expiration: SnapshotExpirationInput = None
    delete_evicted: bool = Field(default=True, alias="deleteEvicted")

    @field_validator("expiration", mode="before")
    @classmethod
    def _coerce_expiration(cls, value: object) -> SnapshotExpiration | None:
        if isinstance(value, Mapping):
            return SnapshotExpiration.model_validate(value)
        return _parse_snapshot_expiration(value)

    def to_api_dict(self, *, exclude: set[str] | None = None) -> JSONObject:
        """Serialize the retention policy for the Sandbox API."""
        data = super().to_api_dict(exclude=exclude)
        expiration = _parse_snapshot_expiration(self.expiration)
        if expiration is not None:
            data["expiration"] = to_ms_int(expiration.value)
        return data


SnapshotRetentionUpdate: TypeAlias = SnapshotRetention | None | _Omitted
NetworkIdUpdate: TypeAlias = str | None | _Omitted


class TagFilter(_InputModel):
    """Filter sandbox queries by an exact tag key and value."""

    key: str
    value: str

    def to_query_value(self) -> str:
        """Serialize the filter for use as an API query parameter."""
        return f"{self.key}:{self.value}"


class SandboxQueryByCreatedAt(_InputModel):
    """Order sandbox results by creation time, optionally filtering by tag."""

    sort_order: Literal["asc", "desc"] = "desc"
    tag: TagFilter | None = None


class SandboxQueryByName(_InputModel):
    """Order sandbox results by name with optional prefix and tag filters."""

    sort_order: Literal["asc", "desc"] = "desc"
    name_prefix: str | None = None
    tag: TagFilter | None = None


class SandboxQueryByStatusUpdatedAt(_InputModel):
    """Order sandbox results by their latest status update time."""

    sort_order: Literal["asc", "desc"] = "desc"


class SandboxQueryByCurrentSnapshotId(_InputModel):
    """Order sandbox results by their current snapshot identifier."""

    sort_order: Literal["asc", "desc"] = "desc"


SandboxQuery: TypeAlias = (
    SandboxQueryByCreatedAt
    | SandboxQueryByName
    | SandboxQueryByStatusUpdatedAt
    | SandboxQueryByCurrentSnapshotId
)


class ProcessLogStream(StrEnum):
    """Output stream represented by a process log event."""

    STDOUT = "stdout"
    STDERR = "stderr"


class DriveQueryByCreatedAt(_InputModel):
    """Drive listing ordered by creation time."""

    sort_order: Literal["asc", "desc"] = "desc"


class DriveQueryByUpdatedAt(_InputModel):
    """Drive listing ordered by update time."""

    sort_order: Literal["asc", "desc"] = "desc"


class DriveQueryByName(_InputModel):
    """Drive listing ordered by name."""

    sort_order: Literal["asc", "desc"] = "desc"
    name_prefix: str | None = None


DriveQuery: TypeAlias = DriveQueryByCreatedAt | DriveQueryByUpdatedAt | DriveQueryByName


class ProcessLog(BaseModel):
    """One streamed process output event."""

    data: str
    stream: ProcessLogStream


# Compatibility aliases for the former public process names.
CompletedProcess = CompletedCommand
ProcessStatus = CommandStatus
ProcessSignal = CommandSignal
