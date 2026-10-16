"""The API's network-policy readback.

The API stores a redacted policy: header values, response bodies, and matcher
strings never come back. A session reports the redacted rules; the named
sandbox reports only domains and CIDRs. This mirrors `parseV2NetworkPolicy`
and `sanitizeNetworkPolicyForStorage`.
"""

from __future__ import annotations

from typing import Any

JSON = dict[str, Any]

_MATCH_DIMENSIONS = ("path", "method", "queryString", "headers")


def _match_dimensions(rule: JSON) -> JSON:
    match = rule.get("match")
    if not match:
        return {}
    return {"matchDimensions": [name for name in _MATCH_DIMENSIONS if name in match]}


def _merged_headers(rules: list[JSON]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for rule in rules:
        for transform in rule.get("transform", ()):
            headers.update(transform.get("headers", {}))
    return headers


def session_readback(body: JSON) -> JSON:
    """Redact a submitted policy the way a session reports it."""
    if "mode" in body:
        return {
            key: value
            for key, value in body.items()
            if key in {"mode", "allowedDomains", "allowedCIDRs", "deniedCIDRs"}
        }

    allow = body.get("allow") or {}
    subnets = body.get("subnets") or {}
    policy: JSON = {"mode": "custom"}
    if isinstance(allow, list):
        if allow:
            policy["allowedDomains"] = list(allow)
        rule_map: dict[str, list[JSON]] = {}
    else:
        rule_map = allow
        if rule_map:
            policy["allowedDomains"] = list(rule_map)
    if "allow" in subnets:
        policy["allowedCIDRs"] = list(subnets["allow"])
    if "deny" in subnets:
        policy["deniedCIDRs"] = list(subnets["deny"])

    injection: list[JSON] = []
    forward: list[JSON] = []
    response: list[JSON] = []
    for domain, rules in rule_map.items():
        per_rule = any(
            rule.get(key) is not None
            for rule in rules
            for key in ("match", "forwardURL", "response")
        )
        if per_rule:
            for rule in rules:
                headers = _merged_headers([rule])
                if headers:
                    injection.append(
                        {"domain": domain, "headerNames": list(headers), **_match_dimensions(rule)}
                    )
        else:
            headers = _merged_headers(rules)
            if headers:
                injection.append({"domain": domain, "headerNames": list(headers)})
        for rule in rules:
            if rule.get("forwardURL") is not None:
                forward.append(
                    {"domain": domain, "forwardURL": rule["forwardURL"], **_match_dimensions(rule)}
                )
            if rule.get("response") is not None:
                response.append(
                    {
                        "domain": domain,
                        "statusCode": rule["response"]["statusCode"],
                        **_match_dimensions(rule),
                    }
                )
    if injection:
        policy["injectionRules"] = injection
    if forward:
        policy["forwardRules"] = forward
    if response:
        policy["responseRules"] = response
    return policy


def sandbox_readback(session_policy: JSON) -> JSON:
    """Reduce a session readback to what the named sandbox reports."""
    return {
        key: value
        for key, value in session_policy.items()
        if key in {"mode", "allowedDomains", "allowedCIDRs", "deniedCIDRs"}
    }


__all__ = ["sandbox_readback", "session_readback"]
