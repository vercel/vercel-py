from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Annotated, Any, Literal, TypeAlias, TypedDict, cast

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
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


class ApiNetworkInjectionRule(_CreateModel):
    """Wire-format injection rule for a single domain."""

    domain: str
    headers: dict[str, str] | None = None
    header_names: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices("header_names", "headerNames"),
        serialization_alias="headerNames",
    )

    def to_redacted_headers(self) -> dict[str, str]:
        if self.header_names:
            redacted: dict[str, str] = {}
            lower_to_name: dict[str, str] = {}
            for name in self.header_names:
                lower_name = name.lower()
                previous_name = lower_to_name.get(lower_name)
                if previous_name is not None and previous_name != name:
                    redacted.pop(previous_name, None)
                lower_to_name[lower_name] = name
                redacted[name] = _REDACTED_HEADER_VALUE
            return redacted
        return dict.fromkeys(self.headers or {}, _REDACTED_HEADER_VALUE)


class ApiNetworkPolicy(_CreateModel):
    """Wire-format network policy returned by the Sandbox API."""

    mode: Literal["allow-all", "deny-all", "custom"]
    allowed_domains: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices("allowed_domains", "allowedDomains"),
        serialization_alias="allowedDomains",
    )
    injection_rules: list[ApiNetworkInjectionRule] | None = Field(
        default=None,
        validation_alias=AliasChoices("injection_rules", "injectionRules"),
        serialization_alias="injectionRules",
    )
    allowed_cidrs: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices("allowed_cidrs", "allowedCIDRs"),
        serialization_alias="allowedCIDRs",
    )
    denied_cidrs: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices("denied_cidrs", "deniedCIDRs"),
        serialization_alias="deniedCIDRs",
    )

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ApiNetworkPolicy:
        return cls.model_validate(payload)

    @classmethod
    def from_network_policy(cls, network_policy: NetworkPolicy) -> ApiNetworkPolicy:
        if isinstance(network_policy, str):
            return cls(mode=network_policy)

        if isinstance(network_policy.allow, list):
            allowed_cidrs = None
            denied_cidrs = None
            if network_policy.subnets is not None:
                allowed_cidrs = network_policy.subnets.allow
                denied_cidrs = network_policy.subnets.deny
            return cls(
                mode="custom",
                allowed_domains=list(network_policy.allow),
                allowed_cidrs=allowed_cidrs,
                denied_cidrs=denied_cidrs,
            )

        injection_rules: list[ApiNetworkInjectionRule] = []
        for domain, rules in network_policy.allow.items():
            headers = _merge_rule_headers(rules)
            if not headers:
                continue
            injection_rules.append(ApiNetworkInjectionRule(domain=domain, headers=headers))

        allowed_cidrs = None
        denied_cidrs = None
        if network_policy.subnets is not None:
            allowed_cidrs = network_policy.subnets.allow
            denied_cidrs = network_policy.subnets.deny

        return cls(
            mode="custom",
            allowed_domains=list(network_policy.allow.keys()),
            injection_rules=injection_rules or None,
            allowed_cidrs=allowed_cidrs,
            denied_cidrs=denied_cidrs,
        )

    def to_network_policy(self) -> NetworkPolicy:
        if self.mode in ("allow-all", "deny-all"):
            return cast(Literal["allow-all", "deny-all"], self.mode)

        allowed_domains = list(self.allowed_domains or [])
        injection_rules = list(self.injection_rules or [])
        subnets = _subnets_from_api(self)

        if not injection_rules:
            return NetworkPolicyCustom(allow=allowed_domains, subnets=subnets)

        allow: dict[str, list[NetworkPolicyRule]] = {domain: [] for domain in allowed_domains}
        for rule in injection_rules:
            allow.setdefault(rule.domain, [])
            headers = rule.to_redacted_headers()
            if not headers:
                continue
            allow[rule.domain].append(
                NetworkPolicyRule(transform=[NetworkTransformer(headers=headers)])
            )

        return NetworkPolicyCustom(allow=allow, subnets=subnets)


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


def _subnets_from_api(network_policy: ApiNetworkPolicy) -> NetworkPolicySubnets | None:
    if network_policy.allowed_cidrs is None and network_policy.denied_cidrs is None:
        return None

    return NetworkPolicySubnets(
        allow=(
            list(network_policy.allowed_cidrs) if network_policy.allowed_cidrs is not None else None
        ),
        deny=(
            list(network_policy.denied_cidrs) if network_policy.denied_cidrs is not None else None
        ),
    )


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
    network_policy: (
        ApiNetworkPolicy | NetworkPolicyCustom | Literal["allow-all", "deny-all"] | None
    ) = Field(
        default=None,
        serialization_alias="networkPolicy",
    )
    interactive: bool | None = Field(default=None, serialization_alias="__interactive")
    env: dict[str, str] | None = None

    @field_validator("network_policy", mode="before")
    @classmethod
    def _coerce_network_policy(cls, value: object) -> ApiNetworkPolicy | None:
        if value is None:
            return None
        if isinstance(value, ApiNetworkPolicy):
            return value
        if isinstance(value, dict):
            return ApiNetworkPolicy.model_validate(value)
        return ApiNetworkPolicy.from_network_policy(cast(NetworkPolicy, value))

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
    network_policy_data: ApiNetworkPolicy | None = Field(default=None, alias="networkPolicy")
    _network_policy: NetworkPolicy | None = PrivateAttr(default=None)

    @field_validator("network_policy_data", mode="before")
    @classmethod
    def _parse_network_policy_data(cls, value: object) -> ApiNetworkPolicy | None:
        if value is None:
            return None
        if isinstance(value, ApiNetworkPolicy):
            return value
        if isinstance(value, dict):
            return ApiNetworkPolicy.from_payload(value)
        raise TypeError("networkPolicy must be a mapping")

    def model_post_init(self, __context: object) -> None:
        if self.network_policy_data is None:
            self._network_policy = None
            return
        self._network_policy = self.network_policy_data.to_network_policy()

    @property
    def network_policy(self) -> NetworkPolicy | None:
        return self._network_policy


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
