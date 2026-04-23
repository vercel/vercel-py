from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Annotated, Any, Literal, TypeAlias, TypedDict, cast

from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic_core import InitErrorDetails

from vercel._internal.polyfills import StrEnum
from vercel._internal.sandbox.errors import SandboxError
from vercel._internal.sandbox.time import MILLISECOND, parse_duration, to_ms_int

# Source types for Sandbox.create()
_REDACTED_HEADER_VALUE = "<redacted>"


class SandboxStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


class SandboxValidationIssue:
    """One local validation issue for sandbox create inputs."""

    __slots__ = ("path", "message")

    def __init__(self, path: str, message: str) -> None:
        self.path = path
        self.message = message


class SandboxValidationError(SandboxError):
    """Local sandbox input validation failed before the API request was sent."""

    def __init__(self, issues: list[SandboxValidationIssue]) -> None:
        self.issues = tuple(issues)
        message = ";\n\n".join(f"{issue.path}: {issue.message}" for issue in self.issues)
        super().__init__(message or "Sandbox validation failed")


class _CreateModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True, serialize_by_alias=True)


class NetworkTransformer(_CreateModel):
    """Header transforms applied to a network policy rule."""

    headers: dict[str, str] | None = None


class NetworkPolicyRule(_CreateModel):
    """Rule configuration for a network policy domain."""

    transform: list[NetworkTransformer] | None = None


class NetworkPolicySubnets(_CreateModel):
    """CIDR allow/deny configuration for a network policy."""

    allow: list[str] | None = None
    deny: list[str] | None = None


NetworkPolicyAllow: TypeAlias = list[str] | dict[str, list[NetworkPolicyRule]]


class NetworkPolicyCustom(_CreateModel):
    """Custom network policy with domain allow lists and subnet rules."""

    allow: NetworkPolicyAllow
    subnets: NetworkPolicySubnets | None = None


NetworkPolicy: TypeAlias = Literal["allow-all", "deny-all"] | NetworkPolicyCustom
_NETWORK_POLICY_ADAPTER: TypeAdapter[NetworkPolicy] = TypeAdapter(NetworkPolicy)


def parse_network_policy(payload: Mapping[str, Any]) -> NetworkPolicy:
    """Parse sandbox API network-policy JSON into the public NetworkPolicy type."""
    mode = payload.get("mode")
    if mode in ("allow-all", "deny-all"):
        return cast(Literal["allow-all", "deny-all"], mode)
    if mode != "custom":
        raise ValueError("networkPolicy.mode must be 'allow-all', 'deny-all', or 'custom'")

    allowed_domains = _get_optional_str_list(payload, "allowed_domains", "allowedDomains")
    injection_rules = _get_optional_mapping_list(payload, "injection_rules", "injectionRules")
    subnets = _subnets_from_payload(payload)

    if not injection_rules:
        return NetworkPolicyCustom(allow=allowed_domains or [], subnets=subnets)

    allow: dict[str, list[NetworkPolicyRule]] = {domain: [] for domain in allowed_domains or []}
    for rule in injection_rules:
        domain = rule.get("domain")
        if not isinstance(domain, str):
            raise TypeError("networkPolicy.injectionRules[].domain must be a string")

        allow.setdefault(domain, [])
        headers = _redacted_headers_from_rule(rule)
        if not headers:
            continue
        allow[domain].append(NetworkPolicyRule(transform=[NetworkTransformer(headers=headers)]))

    return NetworkPolicyCustom(allow=allow, subnets=subnets)


def serialize_network_policy(network_policy: NetworkPolicy) -> dict[str, Any]:
    """Serialize a public NetworkPolicy into sandbox API JSON."""
    network_policy = _NETWORK_POLICY_ADAPTER.validate_python(network_policy)
    if isinstance(network_policy, str):
        return {"mode": network_policy}

    if isinstance(network_policy.allow, list):
        payload: dict[str, Any] = {
            "mode": "custom",
            "allowedDomains": list(network_policy.allow),
        }
        _set_subnet_payload(payload, network_policy.subnets)
        return payload

    injection_rules: list[dict[str, Any]] = []
    for domain, rules in network_policy.allow.items():
        headers = _merge_rule_headers(rules)
        if not headers:
            continue
        injection_rules.append({"domain": domain, "headers": headers})

    payload = {
        "mode": "custom",
        "allowedDomains": list(network_policy.allow.keys()),
    }
    if injection_rules:
        payload["injectionRules"] = injection_rules
    _set_subnet_payload(payload, network_policy.subnets)
    return payload


def _coerce_network_policy_value(value: object) -> NetworkPolicy:
    if isinstance(value, Mapping):
        return parse_network_policy(value)
    return _NETWORK_POLICY_ADAPTER.validate_python(value)


NetworkPolicyCodec: TypeAlias = Annotated[
    NetworkPolicy,
    BeforeValidator(_coerce_network_policy_value),
    PlainSerializer(serialize_network_policy, return_type=dict[str, Any]),
]


def _merge_headers_case_insensitively(
    headers: Sequence[Mapping[str, str] | None],
) -> dict[str, str]:
    merged: dict[str, str] = {}
    lower_to_names: dict[str, set[str]] = {}
    for header_map in headers:
        if not header_map:
            continue

        current_lower_to_names: dict[str, set[str]] = {}
        for name, value in header_map.items():
            merged[name] = value
            current_lower_to_names.setdefault(name.lower(), set()).add(name)

        for lower_name, current_names in current_lower_to_names.items():
            for previous_name in lower_to_names.get(lower_name, set()) - current_names:
                merged.pop(previous_name, None)
            lower_to_names[lower_name] = current_names

    return merged


def _merge_rule_headers(rules: Sequence[NetworkPolicyRule]) -> dict[str, str]:
    return _merge_headers_case_insensitively(
        [_merge_rule_transform_headers(rule) for rule in rules]
    )


def _merge_rule_transform_headers(rule: NetworkPolicyRule) -> dict[str, str]:
    merged: dict[str, str] = {}
    for transform in rule.transform or []:
        merged.update(transform.headers or {})
    return merged


def _redacted_headers_from_rule(rule: Mapping[str, Any]) -> dict[str, str]:
    header_names = _get_optional_str_list(rule, "header_names", "headerNames")
    if header_names:
        redacted: dict[str, str] = {}
        lower_to_name: dict[str, str] = {}
        for name in header_names:
            lower_name = name.lower()
            previous_name = lower_to_name.get(lower_name)
            if previous_name is not None and previous_name != name:
                redacted.pop(previous_name, None)
            lower_to_name[lower_name] = name
            redacted[name] = _REDACTED_HEADER_VALUE
        return redacted

    headers = _get_optional_str_mapping(rule, "headers")
    return dict.fromkeys(headers, _REDACTED_HEADER_VALUE)


def _get_optional_str_list(payload: Mapping[str, Any], *keys: str) -> list[str] | None:
    value = _get_alias_value(payload, *keys)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        joined = "/".join(keys)
        raise TypeError(f"networkPolicy.{joined} must be a list of strings")
    return list(value)


def _get_optional_mapping_list(
    payload: Mapping[str, Any], *keys: str
) -> list[Mapping[str, Any]] | None:
    value = _get_alias_value(payload, *keys)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        joined = "/".join(keys)
        raise TypeError(f"networkPolicy.{joined} must be a list of objects")
    return [cast(Mapping[str, Any], item) for item in value]


def _get_optional_str_mapping(payload: Mapping[str, Any], *keys: str) -> dict[str, str]:
    value = _get_alias_value(payload, *keys)
    if value is None:
        return {}
    if not isinstance(value, Mapping) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        joined = "/".join(keys)
        raise TypeError(f"networkPolicy.{joined} must be a string map")
    return dict(cast(Mapping[str, str], value))


def _get_alias_value(payload: Mapping[str, Any], *keys: str) -> object | None:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _subnets_from_payload(payload: Mapping[str, Any]) -> NetworkPolicySubnets | None:
    allowed_cidrs = _get_optional_str_list(payload, "allowed_cidrs", "allowedCIDRs")
    denied_cidrs = _get_optional_str_list(payload, "denied_cidrs", "deniedCIDRs")
    if allowed_cidrs is None and denied_cidrs is None:
        return None

    return NetworkPolicySubnets(
        allow=allowed_cidrs,
        deny=denied_cidrs,
    )


def _set_subnet_payload(payload: dict[str, Any], subnets: NetworkPolicySubnets | None) -> None:
    if subnets is None:
        return
    if subnets.allow is not None:
        payload["allowedCIDRs"] = subnets.allow
    if subnets.deny is not None:
        payload["deniedCIDRs"] = subnets.deny


def _value_error(loc: tuple[str, ...], message: str, input_value: object) -> InitErrorDetails:
    return cast(
        InitErrorDetails,
        {
            "type": "value_error",
            "loc": loc,
            "input": input_value,
            "ctx": {"error": ValueError(message)},
        },
    )


def _normalize_issue_message(error: Any) -> str:
    error_type = error.get("type")
    if error_type == "missing":
        return "is required"
    if error_type == "string_type":
        return "must be a string"
    if error_type == "int_type":
        return "must be an integer"

    message = str(error.get("msg", "invalid value"))
    if message.startswith("Value error, "):
        return message.removeprefix("Value error, ")
    return message


def _build_issue_path(prefix: str, loc: tuple[object, ...]) -> str:
    if not loc:
        return prefix
    parts = [str(part) for part in loc if part not in ("__root__",)]
    if prefix == "source" and parts[:1] and parts[0] in {"git", "tarball", "snapshot"}:
        parts = parts[1:]
    if not parts:
        return prefix
    return ".".join((prefix, *parts))


def _raise_sandbox_validation_error(prefix: str, exc: ValidationError) -> None:
    raise SandboxValidationError(
        [
            SandboxValidationIssue(
                path=_build_issue_path(prefix, tuple(error.get("loc", ()))),
                message=_normalize_issue_message(error),
            )
            for error in exc.errors(include_url=False)
        ]
    ) from None


def _combined_line_errors(
    exc: ValidationError | None, extra_errors: list[InitErrorDetails]
) -> list[InitErrorDetails]:
    line_errors: list[InitErrorDetails] = []
    if exc is not None:
        line_errors.extend(cast(list[InitErrorDetails], exc.errors(include_url=False)))
    line_errors.extend(extra_errors)
    return line_errors


class GitSource(_CreateModel):
    """Git repository source for creating a sandbox."""

    type: Literal["git"] = "git"
    url: StrictStr
    depth: StrictInt | None = None
    revision: StrictStr | None = None
    username: StrictStr | None = None
    password: StrictStr | None = None

    @field_validator("depth")
    @classmethod
    def _validate_depth(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("must be a positive integer")
        return value

    @model_validator(mode="wrap")
    @classmethod
    def _validate_credentials(
        cls,
        value: Any,
        handler: Any,
    ) -> GitSource:
        raw = value if isinstance(value, Mapping) else {}
        extra_errors: list[InitErrorDetails] = []
        if (raw.get("username") is None) != (raw.get("password") is None):
            extra_errors.append(
                _value_error((), "git username and password must be provided together", value)
            )

        try:
            model = handler(value)
        except ValidationError as exc:
            if not extra_errors:
                raise
            raise ValidationError.from_exception_data(
                cls.__name__,
                _combined_line_errors(exc, extra_errors),
            ) from None

        if extra_errors:
            raise ValidationError.from_exception_data(
                cls.__name__,
                _combined_line_errors(None, extra_errors),
            )

        return model


class TarballSource(_CreateModel):
    """Tarball URL source for creating a sandbox."""

    type: Literal["tarball"] = "tarball"
    url: StrictStr


class SnapshotSource(_CreateModel):
    """Snapshot source for creating a sandbox."""

    type: Literal["snapshot"] = "snapshot"
    snapshot_id: StrictStr = Field(
        validation_alias=AliasChoices("snapshot_id", "snapshotId"),
        serialization_alias="snapshotId",
    )


Source = GitSource | TarballSource | SnapshotSource
_SOURCE_ADAPTER: TypeAdapter[Source] = TypeAdapter(Annotated[Source, Field(discriminator="type")])
SourceInput: TypeAlias = Source | Mapping[str, Any]


class Resources(_CreateModel):
    """Optional sandbox resource requests."""

    vcpus: StrictInt | None = None
    memory: StrictInt | None = None

    @field_validator("vcpus")
    @classmethod
    def _validate_vcpus(cls, value: int | None) -> int | None:
        if value is not None and value != 1 and value % 2 != 0:
            raise ValueError("must be even")
        return value

    @model_validator(mode="wrap")
    @classmethod
    def _validate_memory_relationship(
        cls,
        value: Any,
        handler: Any,
    ) -> Resources:
        raw = value if isinstance(value, Mapping) else {}
        extra_errors: list[InitErrorDetails] = []

        vcpus = raw.get("vcpus")
        memory = raw.get("memory")
        if isinstance(vcpus, int) and not isinstance(vcpus, bool):
            if isinstance(memory, int) and not isinstance(memory, bool):
                expected_memory = vcpus * 2048
                if memory != expected_memory:
                    extra_errors.append(
                        _value_error(
                            ("memory",),
                            f"must equal resources.vcpus * 2048 ({expected_memory})",
                            memory,
                        )
                    )

        try:
            model = handler(value)
        except ValidationError as exc:
            if not extra_errors:
                raise
            raise ValidationError.from_exception_data(
                cls.__name__,
                _combined_line_errors(exc, extra_errors),
            ) from None

        if extra_errors:
            raise ValidationError.from_exception_data(
                cls.__name__,
                _combined_line_errors(None, extra_errors),
            )

        return model


ResourcesInput: TypeAlias = Resources | Mapping[str, Any]


class CreateSandboxRequest(_CreateModel):
    project_id: StrictStr = Field(serialization_alias="projectId")
    ports: list[int] | None = None
    source: Source | None = None
    timeout: int | timedelta | None = None
    resources: Resources | None = None
    runtime: StrictStr | None = None
    network_policy: NetworkPolicyCodec | None = Field(
        default=None,
        serialization_alias="networkPolicy",
    )
    interactive: bool | None = Field(default=None, serialization_alias="__interactive")
    env: dict[str, str] | None = None

    @field_validator("timeout", mode="before")
    @classmethod
    def _coerce_timeout(cls, value: object) -> timedelta | None:
        return parse_duration(value, MILLISECOND)

    @field_serializer("timeout")
    def _serialize_timeout(self, value: timedelta | None) -> int | None:
        if value is None:
            return None
        return to_ms_int(value)


def parse_source(value: SourceInput | None) -> Source | None:
    if value is None:
        return None
    if isinstance(value, (GitSource, TarballSource, SnapshotSource)):
        return value
    if not isinstance(value, Mapping):
        raise SandboxValidationError(
            [SandboxValidationIssue(path="source", message="must be a mapping or source dataclass")]
        )

    source_type = value.get("type")
    if not isinstance(source_type, str):
        raise SandboxValidationError([SandboxValidationIssue("source.type", "is required")])
    if source_type not in {"git", "tarball", "snapshot"}:
        raise SandboxValidationError(
            [
                SandboxValidationIssue(
                    "source.type",
                    "must be one of 'git', 'tarball', or 'snapshot'",
                )
            ]
        )

    try:
        return _SOURCE_ADAPTER.validate_python(value)
    except ValidationError as exc:
        _raise_sandbox_validation_error("source", exc)
    raise AssertionError("unreachable")


def parse_resources(value: ResourcesInput | None) -> Resources | None:
    if value is None:
        return None
    if isinstance(value, Resources):
        return value
    if not isinstance(value, Mapping):
        raise SandboxValidationError(
            [
                SandboxValidationIssue(
                    path="resources",
                    message="must be a mapping or Resources dataclass",
                )
            ]
        )

    try:
        return Resources.model_validate(value)
    except ValidationError as exc:
        _raise_sandbox_validation_error("resources", exc)
    raise AssertionError("unreachable")


class Sandbox(BaseModel):
    """Sandbox metadata from the API."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    memory: int
    vcpus: int
    region: str
    runtime: str
    timeout: int
    status: SandboxStatus
    requested_at: int = Field(alias="requestedAt")
    started_at: int | None = Field(default=None, alias="startedAt")
    requested_stop_at: int | None = Field(default=None, alias="requestedStopAt")
    stopped_at: int | None = Field(default=None, alias="stoppedAt")
    duration: int | None = None
    source_snapshot_id: str | None = Field(default=None, alias="sourceSnapshotId")
    snapshotted_at: int | None = Field(default=None, alias="snapshottedAt")
    created_at: int = Field(alias="createdAt")
    cwd: str
    updated_at: int = Field(alias="updatedAt")
    interactive_port: int | None = Field(default=None, alias="interactivePort")
    network_policy: NetworkPolicyCodec | None = Field(default=None, alias="networkPolicy")

    @field_validator("network_policy", mode="before")
    @classmethod
    def _parse_network_policy(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, Mapping):
            return parse_network_policy(value)
        return value


class SandboxRoute(BaseModel):
    """Route mapping for a sandbox port."""

    url: str
    subdomain: str
    port: int


class Pagination(BaseModel):
    """Pagination metadata for list responses."""

    count: int
    next: int | None = None
    prev: int | None = None


class Command(BaseModel):
    """Command metadata from the API."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str
    args: list[str]
    cwd: str
    sandbox_id: str = Field(alias="sandboxId")
    exit_code: int | None = Field(default=None, alias="exitCode")
    started_at: int = Field(alias="startedAt")


class CommandFinished(Command):
    """Completed command with exit code."""

    exit_code: int = Field(alias="exitCode")


class SandboxResponse(BaseModel):
    """API response containing a sandbox."""

    sandbox: Sandbox


class SandboxAndRoutesResponse(SandboxResponse):
    """API response containing a sandbox and its routes."""

    routes: list[SandboxRoute]


class CommandResponse(BaseModel):
    """API response containing a command."""

    command: Command


class CommandFinishedResponse(BaseModel):
    """API response containing a finished command."""

    command: CommandFinished


class EmptyResponse(BaseModel):
    """Empty API response."""

    pass


class LogLine(BaseModel):
    """Log line from command output."""

    stream: Literal["stdout", "stderr"]
    data: str


class SandboxesResponse(BaseModel):
    """API response containing a list of sandboxes."""

    sandboxes: list[Sandbox]
    pagination: Pagination


class _WriteFileRequired(TypedDict):
    """File to write to the sandbox."""

    path: str
    content: bytes


class WriteFile(_WriteFileRequired, total=False):
    """File to write to the sandbox."""

    mode: int


class Snapshot(BaseModel):
    """Snapshot metadata from the API."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    source_sandbox_id: str = Field(alias="sourceSandboxId")
    region: str
    status: Literal["created", "deleted", "failed"]
    size_bytes: int = Field(alias="sizeBytes")
    expires_at: int | None = Field(default=None, alias="expiresAt")
    created_at: int = Field(alias="createdAt")
    updated_at: int = Field(alias="updatedAt")


class SnapshotsResponse(BaseModel):
    """API response containing a list of snapshots."""

    snapshots: list[Snapshot]
    pagination: Pagination


class SnapshotResponse(BaseModel):
    """API response containing a snapshot."""

    snapshot: Snapshot


class CreateSnapshotResponse(BaseModel):
    """API response containing a snapshot and the stopped sandbox."""

    snapshot: Snapshot
    sandbox: Sandbox
