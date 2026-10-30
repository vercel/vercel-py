"""Parse the SDK/CLI network policy JSON format into authored Python values."""

import json
import re
from collections.abc import Iterable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from vercel._internal.core.polyfills import Self
from vercel.sandbox._internal.models import (
    NetworkPolicy,
    NetworkPolicyKeyValueMatcher,
    NetworkPolicyMatcher,
    NetworkPolicyRequestMatcher,
    NetworkPolicyResponse,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkPolicyTransform,
)


class NetworkPolicyParseError(ValueError):
    """Invalid policy input, with paths and explanations but no input values."""

    issues: tuple[str, ...]

    def __init__(self, issues: Iterable[str]) -> None:
        self.issues = tuple(issues)
        super().__init__("Invalid network policy:\n" + "\n".join(f"  {i}" for i in self.issues))


class _Input(BaseModel):
    # Wire input is deliberately separate from model_dump()'s Python representation.
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    @field_validator("*", mode="before")
    @classmethod
    def _reject_null(cls, value: object) -> object:
        # Optional JSON fields may be omitted, but the SDK does not accept null.
        if value is None:
            raise ValueError("must not be null")
        return value

    @model_validator(mode="after")
    def _validate_value(self) -> Self:
        # Reuse authored-model invariants; Pydantic preserves the nested wire path.
        self.to_value()
        return self

    def to_value(self) -> object:
        raise NotImplementedError


class _Matcher(_Input):
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


class _Pair(_Input):
    key: _Matcher | None = None
    value: _Matcher | None = None

    def to_value(self) -> NetworkPolicyKeyValueMatcher:
        return NetworkPolicyKeyValueMatcher(
            key=None if self.key is None else self.key.to_value(),
            value=None if self.value is None else self.value.to_value(),
        )


class _Match(_Input):
    path: _Matcher | None = None
    method: list[str] | None = None
    query: list[_Pair] | None = Field(default=None, alias="queryString")
    headers: list[_Pair] | None = None

    def to_value(self) -> NetworkPolicyRequestMatcher:
        return NetworkPolicyRequestMatcher(
            path=None if self.path is None else self.path.to_value(),
            method=None if self.method is None else tuple(self.method),
            query=None if self.query is None else tuple(pair.to_value() for pair in self.query),
            headers=(
                None if self.headers is None else tuple(pair.to_value() for pair in self.headers)
            ),
        )


class _Transform(_Input):
    headers: dict[str, str] | None = None

    def to_value(self) -> NetworkPolicyTransform:
        return NetworkPolicyTransform(headers=self.headers)


class _Response(_Input):
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


class _Rule(_Input):
    match: _Match | None = None
    transform: list[_Transform] | None = None
    forward_url: str | None = Field(default=None, alias="forwardURL")
    response: _Response | None = None

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


class _Subnets(_Input):
    allow: list[str] | None = None
    deny: list[str] | None = None

    def to_value(self) -> NetworkPolicySubnets:
        return NetworkPolicySubnets(
            allow=None if self.allow is None else tuple(self.allow),
            deny=None if self.deny is None else tuple(self.deny),
        )


class _Policy(_Input):
    allow: dict[str, list[_Rule]] | None = None
    subnets: _Subnets | None = None

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


def parse_network_policy(input: object) -> NetworkPolicy:
    """Parse decoded network policy JSON into an immutable authored policy.

    Accepts ``"allow-all"``, ``"deny-all"``, or the JS SDK/CLI object format,
    with an optional domain list or rule map and optional subnets. Use
    ``json.load(file)`` or ``json.loads(text)`` to decode JSON first.

    Unknown keys are rejected throughout the policy, so a typo cannot silently
    widen a credential-bearing rule. Python's authored-model structural
    invariants also apply. Raises :class:`NetworkPolicyParseError` with paths
    and explanations that never include input values.
    """
    if isinstance(input, str):
        if input == "allow-all":
            return NetworkPolicy.allow_all()
        if input == "deny-all":
            return NetworkPolicy.deny_all()
    try:
        return _Policy.model_validate(input).to_value()
    except ValidationError as error:
        issues = []
        for issue in error.errors(include_url=False, include_input=False, include_context=False):
            message = "unknown key" if issue["type"] == "extra_forbidden" else issue["msg"]
            issues.append(f"{_format_path(issue['loc'])}: {message}")
    # Raise outside the handler so the exception does not retain Pydantic's
    # credential-bearing input through __context__ or __cause__.
    raise NetworkPolicyParseError(issues)


def _format_path(path: tuple[str | int, ...]) -> str:
    result = ""
    for key in path:
        if isinstance(key, int):
            result += f"[{key}]"
        else:
            if re.fullmatch(r"[A-Za-z_$][\w$]*", key):
                result += ("." if result else "") + key
            else:
                result += f"[{json.dumps(key)}]"
    return result or "policy"
