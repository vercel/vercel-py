"""Public value types for the Sandbox API."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from subprocess import CalledProcessError
from types import MappingProxyType
from typing import Any, Literal, TypeAlias, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue as PydanticJsonValue,
    field_validator,
)

from vercel.internal.core.polyfills import StrEnum
from vercel.internal.core.time import SECOND, coerce_duration, to_ms_int

JSONValue: TypeAlias = PydanticJsonValue
JSONObject: TypeAlias = dict[str, JSONValue]
DurationInput: TypeAlias = int | float | timedelta | None
_MIN_SNAPSHOT_EXPIRATION = timedelta(days=1)
_MAX_SNAPSHOT_EXPIRATION = timedelta(days=365 * 10)
_ZERO_DELTA = timedelta(0)


@dataclass(frozen=True, slots=True)
class NetworkPolicyMatcher:
    """Match one request value using one comparison strategy."""

    kind: Literal["exact", "starts_with", "regex"]
    value: str
    __hash__ = None  # type: ignore[assignment]

    @classmethod
    def exact(cls, value: str) -> "NetworkPolicyMatcher":
        return cls("exact", value)

    @classmethod
    def starts_with(cls, value: str) -> "NetworkPolicyMatcher":
        return cls("starts_with", value)

    @classmethod
    def regex(cls, value: str) -> "NetworkPolicyMatcher":
        return cls("regex", value)


@dataclass(frozen=True, slots=True)
class NetworkPolicyKeyValueMatcher:
    """Match a request header or query-string key/value pair."""

    key: NetworkPolicyMatcher | None = None
    value: NetworkPolicyMatcher | None = None
    __hash__ = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.key is None and self.value is None:
            raise ValueError("key-value matcher requires a key or value matcher")


@dataclass(frozen=True, slots=True)
class NetworkPolicyRequestMatcher:
    """Match selected dimensions of an outbound HTTP request."""

    path: NetworkPolicyMatcher | None = None
    method: tuple[str, ...] | None = None
    query: tuple[NetworkPolicyKeyValueMatcher, ...] | None = None
    headers: tuple[NetworkPolicyKeyValueMatcher, ...] | None = None
    __hash__ = None  # type: ignore[assignment]

    def __init__(
        self,
        *,
        path: NetworkPolicyMatcher | None = None,
        method: Iterable[str] | None = None,
        query: Iterable[NetworkPolicyKeyValueMatcher] | None = None,
        headers: Iterable[NetworkPolicyKeyValueMatcher] | None = None,
    ) -> None:
        normalized_method = None if method is None else tuple(method)
        normalized_query = None if query is None else tuple(query)
        normalized_headers = None if headers is None else tuple(headers)
        if (
            path is None
            and normalized_method is None
            and normalized_query is None
            and normalized_headers is None
        ):
            raise ValueError("request matcher requires at least one matching dimension")
        if normalized_method == ():
            raise ValueError("request matcher method must not be empty")
        if normalized_query == ():
            raise ValueError("request matcher query must not be empty")
        if normalized_headers == ():
            raise ValueError("request matcher headers must not be empty")
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "method", normalized_method)
        object.__setattr__(self, "query", normalized_query)
        object.__setattr__(self, "headers", normalized_headers)


@dataclass(frozen=True, slots=True)
class NetworkPolicyTransform:
    """Inject authored headers or describe redacted response header names."""

    headers: Mapping[str, str] | None = None
    header_names: tuple[str, ...] | None = None
    __hash__ = None  # type: ignore[assignment]

    def __init__(
        self,
        *,
        headers: Mapping[str, str] | None = None,
        header_names: Iterable[str] | None = None,
    ) -> None:
        normalized_headers = None if headers is None else MappingProxyType(dict(headers))
        normalized_header_names = None if header_names is None else tuple(header_names)
        if normalized_headers is not None and normalized_header_names is not None:
            raise ValueError("network policy transform cannot set headers and header_names")
        object.__setattr__(self, "headers", normalized_headers)
        object.__setattr__(self, "header_names", normalized_header_names)


@dataclass(frozen=True, slots=True)
class NetworkPolicyRule:
    """Configure request matching, transforms, and forwarding for one domain."""

    transform: tuple[NetworkPolicyTransform, ...] = ()
    match: NetworkPolicyRequestMatcher | None = None
    forward_url: str | None = None
    __hash__ = None  # type: ignore[assignment]

    def __init__(
        self,
        *,
        transform: Iterable[NetworkPolicyTransform] = (),
        match: NetworkPolicyRequestMatcher | None = None,
        forward_url: str | None = None,
    ) -> None:
        object.__setattr__(self, "transform", tuple(transform))
        object.__setattr__(self, "match", match)
        object.__setattr__(self, "forward_url", forward_url)


@dataclass(frozen=True, slots=True)
class NetworkPolicySubnets:
    """Configure allowed and denied network ranges."""

    allow: tuple[str, ...] | None = None
    deny: tuple[str, ...] | None = None
    __hash__ = None  # type: ignore[assignment]

    def __init__(
        self,
        *,
        allow: Iterable[str] | None = None,
        deny: Iterable[str] | None = None,
    ) -> None:
        object.__setattr__(self, "allow", None if allow is None else tuple(allow))
        object.__setattr__(self, "deny", None if deny is None else tuple(deny))


@dataclass(frozen=True, slots=True)
class NetworkPolicy:
    """Immutable outbound network access policy."""

    mode: Literal["allow-all", "deny-all", "custom"]
    allow: Mapping[str, tuple[NetworkPolicyRule, ...]]
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
        copied = MappingProxyType({domain: tuple(rules) for domain, rules in dict(allow).items()})
        return cls(mode="custom", allow=copied, subnets=subnets)

    def __post_init__(self) -> None:
        if not isinstance(self.allow, MappingProxyType):
            object.__setattr__(
                self,
                "allow",
                MappingProxyType({domain: tuple(rules) for domain, rules in self.allow.items()}),
            )
        if self.mode != "custom" and (self.allow or self.subnets is not None):
            raise ValueError("simple network policy modes cannot include custom rules")


def _serialize_network_policy(network_policy: NetworkPolicy) -> JSONObject:
    if not isinstance(network_policy, NetworkPolicy):
        raise TypeError("network_policy must be a NetworkPolicy")
    if network_policy.mode != "custom":
        return {"mode": network_policy.mode}

    allow: dict[str, JSONValue] = {}
    for domain, rules in network_policy.allow.items():
        allow[domain] = [_serialize_network_policy_rule(rule) for rule in rules]
    result: JSONObject = {"allow": allow}
    if network_policy.subnets is not None:
        subnets: JSONObject = {}
        if network_policy.subnets.allow is not None:
            subnets["allow"] = list(network_policy.subnets.allow)
        if network_policy.subnets.deny is not None:
            subnets["deny"] = list(network_policy.subnets.deny)
        result["subnets"] = subnets
    return result


def _serialize_network_policy_rule(rule: NetworkPolicyRule) -> JSONObject:
    result: JSONObject = {}
    if rule.match is not None:
        result["match"] = _serialize_request_matcher(rule.match)
    if rule.transform:
        transforms: list[JSONValue] = []
        for transform in rule.transform:
            if transform.header_names is not None:
                raise ValueError(
                    "redacted network policy transforms cannot be submitted to the API"
                )
            transform_data: JSONObject = {}
            if transform.headers is not None:
                transform_data["headers"] = dict(transform.headers)
            transforms.append(transform_data)
        result["transform"] = transforms
    if rule.forward_url is not None:
        result["forwardURL"] = rule.forward_url
    return result


def _serialize_request_matcher(matcher: NetworkPolicyRequestMatcher) -> JSONObject:
    result: JSONObject = {}
    if matcher.path is not None:
        result["path"] = _serialize_matcher(matcher.path)
    if matcher.method is not None:
        result["method"] = list(matcher.method)
    if matcher.query is not None:
        result["queryString"] = [_serialize_key_value_matcher(item) for item in matcher.query]
    if matcher.headers is not None:
        result["headers"] = [_serialize_key_value_matcher(item) for item in matcher.headers]
    return result


def _serialize_key_value_matcher(matcher: NetworkPolicyKeyValueMatcher) -> JSONObject:
    result: JSONObject = {}
    if matcher.key is not None:
        result["key"] = _serialize_matcher(matcher.key)
    if matcher.value is not None:
        result["value"] = _serialize_matcher(matcher.value)
    return result


def _serialize_matcher(matcher: NetworkPolicyMatcher) -> JSONObject:
    key = "startsWith" if matcher.kind == "starts_with" else matcher.kind
    return {key: matcher.value}


def _parse_network_policy(value: object) -> NetworkPolicy | None:
    if value is None or isinstance(value, NetworkPolicy):
        return value
    data = _mapping(value, "network policy")
    if "allow" in data or "subnets" in data:
        return _parse_domain_map_network_policy(data)

    mode = data.get("mode")
    if mode == "allow-all":
        return NetworkPolicy.allow_all()
    if mode == "deny-all":
        return NetworkPolicy.deny_all()
    if mode != "custom":
        raise ValueError("network policy mode must be allow-all, deny-all, or custom")
    return _parse_normalized_network_policy(data)


def _parse_domain_map_network_policy(data: Mapping[str, Any]) -> NetworkPolicy:
    raw_allow = data.get("allow", {})
    allow: list[tuple[str, tuple[NetworkPolicyRule, ...]]] = []
    if isinstance(raw_allow, Mapping):
        for domain, raw_rules in raw_allow.items():
            rules = tuple(
                _parse_network_policy_rule(item)
                for item in _iterable(raw_rules, f"network policy rules for {domain!r}")
            )
            allow.append((_string(domain, "network policy domain"), rules))
    else:
        allow.extend(
            (_string(domain, "network policy domain"), ())
            for domain in _iterable(raw_allow, "network policy allow")
        )

    subnets = None
    if "subnets" in data:
        raw_subnets = _mapping(data["subnets"], "network policy subnets")
        subnets = NetworkPolicySubnets(
            allow=_optional_string_iterable(raw_subnets.get("allow"), "subnet allow"),
            deny=_optional_string_iterable(raw_subnets.get("deny"), "subnet deny"),
        )
    return NetworkPolicy.custom(allow, subnets=subnets)


def _parse_normalized_network_policy(data: Mapping[str, Any]) -> NetworkPolicy:
    grouped: dict[str, list[NetworkPolicyRule]] = {}
    for domain in _optional_string_iterable(data.get("allowedDomains"), "allowedDomains") or ():
        grouped.setdefault(domain, [])

    for raw_rule in _iterable(data.get("injectionRules", ()), "injectionRules"):
        rule_data = _mapping(raw_rule, "injection rule")
        domain = _string(rule_data.get("domain"), "injection rule domain")
        transform = _parse_normalized_transform(rule_data)
        grouped.setdefault(domain, []).append(
            NetworkPolicyRule(
                transform=(transform,),
                match=_parse_optional_request_matcher(rule_data.get("match")),
            )
        )

    for raw_rule in _iterable(
        data.get("forwardRules", data.get("forwardingRules", ())),
        "forwardRules",
    ):
        rule_data = _mapping(raw_rule, "forward rule")
        domain = _string(rule_data.get("domain"), "forward rule domain")
        grouped.setdefault(domain, []).append(
            NetworkPolicyRule(
                forward_url=_string(
                    rule_data.get("forwardURL", rule_data.get("forwardUrl")),
                    "forward rule forwardURL",
                ),
                match=_parse_optional_request_matcher(rule_data.get("match")),
            )
        )

    allowed_cidrs = _optional_string_iterable(data.get("allowedCIDRs"), "allowedCIDRs")
    denied_cidrs = _optional_string_iterable(data.get("deniedCIDRs"), "deniedCIDRs")
    subnets = (
        None
        if allowed_cidrs is None and denied_cidrs is None
        else NetworkPolicySubnets(allow=allowed_cidrs, deny=denied_cidrs)
    )
    return NetworkPolicy.custom(grouped, subnets=subnets)


def _parse_normalized_transform(data: Mapping[str, Any]) -> NetworkPolicyTransform:
    if "headers" in data and "headerNames" in data:
        raise ValueError("injection rule cannot set headers and headerNames")
    if "headerNames" in data:
        return NetworkPolicyTransform(
            header_names=_string_iterable(data["headerNames"], "headerNames")
        )
    headers = _mapping(data.get("headers", {}), "injection rule headers")
    return NetworkPolicyTransform(
        headers={
            _string(key, "header name"): _string(value, "header value")
            for key, value in headers.items()
        }
    )


def _parse_network_policy_rule(value: object) -> NetworkPolicyRule:
    data = _mapping(value, "network policy rule")
    transforms: list[NetworkPolicyTransform] = []
    for raw_transform in _iterable(data.get("transform", ()), "network policy transforms"):
        transform_data = _mapping(raw_transform, "network policy transform")
        if "headers" in transform_data and "headerNames" in transform_data:
            raise ValueError("network policy transform cannot set headers and headerNames")
        if "headerNames" in transform_data:
            transforms.append(
                NetworkPolicyTransform(
                    header_names=_string_iterable(
                        transform_data["headerNames"], "transform headerNames"
                    )
                )
            )
        else:
            headers = _mapping(transform_data.get("headers", {}), "transform headers")
            transforms.append(
                NetworkPolicyTransform(
                    headers={
                        _string(key, "header name"): _string(value, "header value")
                        for key, value in headers.items()
                    }
                )
            )
    forward_url = data.get("forwardURL", data.get("forwardUrl"))
    return NetworkPolicyRule(
        transform=transforms,
        match=_parse_optional_request_matcher(data.get("match")),
        forward_url=None if forward_url is None else _string(forward_url, "forwardURL"),
    )


def _parse_optional_request_matcher(value: object) -> NetworkPolicyRequestMatcher | None:
    if value is None:
        return None
    data = _mapping(value, "request matcher")
    return NetworkPolicyRequestMatcher(
        path=None if "path" not in data else _parse_matcher(data["path"]),
        method=_optional_string_iterable(data.get("method"), "request matcher method"),
        query=_parse_optional_key_value_matchers(
            data.get("queryString", data.get("query")), "request matcher queryString"
        ),
        headers=_parse_optional_key_value_matchers(data.get("headers"), "request matcher headers"),
    )


def _parse_optional_key_value_matchers(
    value: object, name: str
) -> tuple[NetworkPolicyKeyValueMatcher, ...] | None:
    if value is None:
        return None
    return tuple(
        NetworkPolicyKeyValueMatcher(
            key=None if "key" not in data else _parse_matcher(data["key"]),
            value=None if "value" not in data else _parse_matcher(data["value"]),
        )
        for item in _iterable(value, name)
        for data in (_mapping(item, name),)
    )


def _parse_matcher(value: object) -> NetworkPolicyMatcher:
    data = _mapping(value, "network policy matcher")
    variants = [
        (kind, data[key])
        for kind, key in (
            ("exact", "exact"),
            ("starts_with", "startsWith"),
            ("regex", "regex"),
        )
        if key in data
    ]
    if len(variants) != 1:
        raise ValueError(
            "network policy matcher requires exactly one of exact, startsWith, or regex"
        )
    kind, matcher_value = variants[0]
    return NetworkPolicyMatcher(
        cast(Literal["exact", "starts_with", "regex"], kind),
        _string(matcher_value, "network policy matcher value"),
    )


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _iterable(value: object, name: str) -> Iterable[object]:
    if isinstance(value, (str, bytes, Mapping)) or not isinstance(value, Iterable):
        raise TypeError(f"{name} must be an iterable")
    return cast(Iterable[object], value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _string_iterable(value: object, name: str) -> tuple[str, ...]:
    return tuple(_string(item, name) for item in _iterable(value, name))


def _optional_string_iterable(value: object, name: str) -> tuple[str, ...] | None:
    return None if value is None else _string_iterable(value, name)


@dataclass(frozen=True, slots=True)
class SnapshotExpiration:
    """Represent a platform-side snapshot lifetime.

    Args:
        value: Lifetime in seconds or as a ``timedelta``. Zero disables
            expiration; nonzero values must be between one day and ten years.

    Raises:
        ValueError: If the lifetime is outside the supported range.
    """

    value: timedelta

    def __init__(self, value: int | float | timedelta) -> None:
        normalized = coerce_duration(value, SECOND)
        if normalized != _ZERO_DELTA and not (
            _MIN_SNAPSHOT_EXPIRATION <= normalized <= _MAX_SNAPSHOT_EXPIRATION
        ):
            raise ValueError(
                "Snapshot expiration must be 0 or between one day and ten years inclusive"
            )
        object.__setattr__(self, "value", normalized)


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


@dataclass(frozen=True, slots=True)
class _WriteFile:
    path: str
    content: bytes
    mode: int | None = None


@dataclass(frozen=True, slots=True)
class DirectoryEntry:
    """Describe one entry returned by a sandbox directory listing.

    Attributes:
        path: Entry name relative to the listed directory, not a full path.
        kind: Filesystem entry type.
    """

    path: str
    kind: Literal["file", "directory", "symlink", "other"]


class SandboxStatus(StrEnum):
    """Lifecycle status reported for a sandbox or runtime session."""

    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


class ProcessStatus(StrEnum):
    """Lifecycle status derived for a sandbox process."""

    RUNNING = "running"
    EXITED = "exited"


@dataclass(frozen=True, slots=True)
class CompletedProcess:
    """The captured result of one completed remote process."""

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
    delete_evicted: bool = Field(default=True, serialization_alias="deleteEvicted")

    @field_validator("expiration", mode="before")
    @classmethod
    def _coerce_expiration(cls, value: object) -> SnapshotExpiration | None:
        return _parse_snapshot_expiration(value)

    def to_api_dict(self, *, exclude: set[str] | None = None) -> JSONObject:
        """Serialize the retention policy for the Sandbox API."""
        data = super().to_api_dict(exclude=exclude)
        expiration = _parse_snapshot_expiration(self.expiration)
        if expiration is not None:
            data["expiration"] = to_ms_int(expiration.value)
        return data


SnapshotRetentionUpdate: TypeAlias = SnapshotRetention | None | _Omitted


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


class ProcessLog(BaseModel):
    """One streamed process output event."""

    data: str
    stream: ProcessLogStream
