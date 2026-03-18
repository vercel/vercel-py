from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from .types import NetworkPolicy, NetworkPolicyRule

__all__ = ["to_api_network_policy", "from_api_network_policy"]

_REDACTED_HEADER_VALUE = "<redacted>"


def _merge_rule_headers(rules: list[NetworkPolicyRule]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for rule in rules:
        for transform in rule.get("transform", []):
            headers.update(transform.get("headers", {}))
    return headers


def _subnets_to_api(network_policy: Mapping[str, Any]) -> dict[str, list[str]]:
    subnets = network_policy.get("subnets")
    if not isinstance(subnets, Mapping):
        return {}

    subnet_payload: dict[str, list[str]] = {}
    if "allow" in subnets:
        subnet_payload["allowedCIDRs"] = list(subnets["allow"])
    if "deny" in subnets:
        subnet_payload["deniedCIDRs"] = list(subnets["deny"])
    return subnet_payload


def to_api_network_policy(network_policy: NetworkPolicy) -> dict[str, Any]:
    if isinstance(network_policy, str):
        return {"mode": network_policy}

    allow = network_policy.get("allow")
    if isinstance(allow, list):
        payload: dict[str, Any] = {
            "mode": "custom",
            "allowedDomains": list(allow),
        }
        payload.update(_subnets_to_api(network_policy))
        return payload

    policy_payload: dict[str, Any] = {
        "mode": "custom",
        "allowedDomains": list(allow.keys()) if isinstance(allow, Mapping) else [],
    }

    injection_rules: list[dict[str, Any]] = []
    if isinstance(allow, Mapping):
        for domain, rules in allow.items():
            headers = _merge_rule_headers(list(rules))
            if not headers:
                continue
            injection_rules.append({"domain": domain, "headers": headers})

    if injection_rules:
        policy_payload["injectionRules"] = injection_rules

    policy_payload.update(_subnets_to_api(network_policy))
    return policy_payload


def _subnets_from_api(network_policy: Mapping[str, Any]) -> dict[str, list[str]]:
    allowed = network_policy.get("allowedCIDRs") if "allowedCIDRs" in network_policy else None
    denied = network_policy.get("deniedCIDRs") if "deniedCIDRs" in network_policy else None

    subnet_payload: dict[str, list[str]] = {}
    if allowed is not None:
        subnet_payload["allow"] = list(allowed)
    if denied is not None:
        subnet_payload["deny"] = list(denied)
    return subnet_payload


def from_api_network_policy(network_policy: Mapping[str, Any]) -> NetworkPolicy:
    mode = network_policy.get("mode")
    if mode in ("allow-all", "deny-all"):
        return cast(NetworkPolicy, mode)

    allowed_domains = list(network_policy.get("allowedDomains") or [])
    injection_rules = list(network_policy.get("injectionRules") or [])
    subnets = _subnets_from_api(network_policy)

    if not injection_rules:
        list_policy_result: dict[str, Any] = {"allow": allowed_domains}
        if subnets:
            list_policy_result["subnets"] = subnets
        return cast(NetworkPolicy, list_policy_result)

    allow: dict[str, list[NetworkPolicyRule]] = {domain: [] for domain in allowed_domains}
    for rule in injection_rules:
        domain = rule.get("domain")
        if not isinstance(domain, str):
            continue

        allow.setdefault(domain, [])
        header_names = rule.get("headerNames")
        if header_names is None and isinstance(rule.get("headers"), Mapping):
            header_names = list(rule["headers"].keys())
        header_names = header_names or []
        headers = {name: _REDACTED_HEADER_VALUE for name in header_names if isinstance(name, str)}
        if not headers:
            continue
        allow[domain].append({"transform": [{"headers": headers}]})

    record_policy_result: dict[str, Any] = {"allow": allow}
    if subnets:
        record_policy_result["subnets"] = subnets
    return cast(NetworkPolicy, record_policy_result)
