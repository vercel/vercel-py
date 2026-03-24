from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias, cast

__all__ = [
    "NetworkTransformer",
    "NetworkPolicyRule",
    "NetworkPolicySubnets",
    "NetworkPolicyCustom",
    "NetworkPolicy",
    "ApiNetworkInjectionRule",
    "ApiNetworkPolicy",
]

_REDACTED_HEADER_VALUE = "<redacted>"


@dataclass(frozen=True, slots=True)
class NetworkTransformer:
    """Header transforms applied to a network policy rule."""

    headers: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class NetworkPolicyRule:
    """Rule configuration for a network policy domain."""

    transform: list[NetworkTransformer] | None = None


@dataclass(frozen=True, slots=True)
class NetworkPolicySubnets:
    """CIDR allow/deny configuration for a network policy."""

    allow: list[str] | None = None
    deny: list[str] | None = None


NetworkPolicyAllow: TypeAlias = list[str] | dict[str, list[NetworkPolicyRule]]


@dataclass(frozen=True, slots=True)
class NetworkPolicyCustom:
    """Custom network policy with domain allow lists and subnet rules."""

    allow: NetworkPolicyAllow
    subnets: NetworkPolicySubnets | None = None


NetworkPolicy: TypeAlias = Literal["allow-all", "deny-all"] | NetworkPolicyCustom


@dataclass(frozen=True, slots=True)
class ApiNetworkInjectionRule:
    """Wire-format injection rule for a single domain."""

    domain: str
    headers: dict[str, str] | None = None
    header_names: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"domain": self.domain}
        if self.headers is not None:
            payload["headers"] = self.headers
        if self.header_names is not None:
            payload["headerNames"] = self.header_names
        return payload


@dataclass(frozen=True, slots=True)
class ApiNetworkPolicy:
    """Wire-format network policy returned by the Sandbox API."""

    mode: Literal["allow-all", "deny-all", "custom"]
    allowed_domains: list[str] | None = None
    injection_rules: list[ApiNetworkInjectionRule] | None = None
    allowed_cidrs: list[str] | None = None
    denied_cidrs: list[str] | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ApiNetworkPolicy:
        injection_rules_payload = payload.get("injectionRules")
        injection_rules: list[ApiNetworkInjectionRule] | None = None
        if isinstance(injection_rules_payload, Sequence) and not isinstance(
            injection_rules_payload, (str, bytes)
        ):
            injection_rules = []
            for item in injection_rules_payload:
                if not isinstance(item, Mapping):
                    continue
                domain = item.get("domain")
                if not isinstance(domain, str):
                    continue
                headers = item.get("headers")
                header_names = item.get("headerNames")
                injection_rules.append(
                    ApiNetworkInjectionRule(
                        domain=domain,
                        headers=dict(headers) if isinstance(headers, Mapping) else None,
                        header_names=(
                            list(header_names) if isinstance(header_names, Sequence) else None
                        ),
                    )
                )

        return cls(
            mode=cast(
                Literal["allow-all", "deny-all", "custom"],
                payload.get("mode", "custom"),
            ),
            allowed_domains=(
                list(payload["allowedDomains"]) if "allowedDomains" in payload else None
            ),
            injection_rules=injection_rules,
            allowed_cidrs=list(payload["allowedCIDRs"]) if "allowedCIDRs" in payload else None,
            denied_cidrs=list(payload["deniedCIDRs"]) if "deniedCIDRs" in payload else None,
        )

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

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"mode": self.mode}
        if self.allowed_domains is not None:
            payload["allowedDomains"] = list(self.allowed_domains)
        if self.injection_rules is not None:
            payload["injectionRules"] = [rule.to_dict() for rule in self.injection_rules]
        if self.allowed_cidrs is not None:
            payload["allowedCIDRs"] = list(self.allowed_cidrs)
        if self.denied_cidrs is not None:
            payload["deniedCIDRs"] = list(self.denied_cidrs)
        return payload

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
            headers = _redacted_headers(rule)
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


def _redacted_headers_from_names(header_names: Sequence[str]) -> dict[str, str]:
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


def _redacted_headers(rule: ApiNetworkInjectionRule) -> dict[str, str]:
    if rule.header_names is not None:
        return _redacted_headers_from_names(rule.header_names)
    return dict.fromkeys(rule.headers or {}, _REDACTED_HEADER_VALUE)


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
