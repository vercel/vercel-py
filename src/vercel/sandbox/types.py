from __future__ import annotations

from typing import Literal, TypeAlias, TypedDict

__all__ = [
    "NetworkTransformer",
    "NetworkPolicyRule",
    "NetworkPolicySubnets",
    "NetworkPolicyCustom",
    "NetworkPolicy",
]


class NetworkTransformer(TypedDict, total=False):
    """Header transforms applied to a network policy rule."""

    headers: dict[str, str]


class NetworkPolicyRule(TypedDict, total=False):
    """Rule configuration for a network policy domain."""

    transform: list[NetworkTransformer]


class NetworkPolicySubnets(TypedDict, total=False):
    """CIDR allow/deny configuration for a network policy."""

    allow: list[str]
    deny: list[str]


_NetworkPolicyAllow: TypeAlias = list[str] | dict[str, list[NetworkPolicyRule]]


class NetworkPolicyCustom(TypedDict, total=False):
    """Custom network policy with domain allow lists and subnet rules."""

    allow: _NetworkPolicyAllow
    subnets: NetworkPolicySubnets


NetworkPolicy: TypeAlias = Literal["allow-all", "deny-all"] | NetworkPolicyCustom
