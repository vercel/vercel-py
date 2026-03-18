"""Conversion contract tests for sandbox network policies."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from vercel.sandbox.network_policy import from_api_network_policy, to_api_network_policy
from vercel.sandbox.types import NetworkPolicy, NetworkPolicyRule


def _headers(*names: str) -> dict[str, str]:
    return {name: f"value-for-{name.lower()}" for name in names}


def _record_policy_domains(policy: NetworkPolicy) -> dict[str, set[str]]:
    if isinstance(policy, str):
        return {}

    allow = policy["allow"]
    if isinstance(allow, list):
        return {domain: set() for domain in allow}

    domains: dict[str, set[str]] = {}
    for domain, rules in allow.items():
        header_names: set[str] = set()
        for rule in rules:
            for transform in rule.get("transform", []):
                header_names.update(transform.get("headers", {}).keys())
        domains[domain] = header_names
    return domains


def _rule_header_names(rules: Iterable[NetworkPolicyRule]) -> set[str]:
    header_names: set[str] = set()
    for rule in rules:
        for transform in rule.get("transform", []):
            header_names.update(transform.get("headers", {}).keys())
    return header_names


def _round_trip_domains(policy: NetworkPolicy) -> dict[str, set[str]]:
    result = from_api_network_policy(to_api_network_policy(policy))
    if isinstance(result, str):
        return {}
    allow = result["allow"]
    if isinstance(allow, list):
        return {domain: set() for domain in allow}

    domains: dict[str, set[str]] = {}
    for domain, rules in allow.items():
        domains[domain] = _rule_header_names(rules)
    return domains


def _domain_strategy() -> st.SearchStrategy[str]:
    label = st.from_regex(r"[a-z][a-z0-9-]{0,5}", fullmatch=True)
    wildcard = st.just("*")
    subdomain = st.builds(lambda left, right: f"{left}.{right}", label, label)
    return st.one_of(label, subdomain, wildcard)


def _header_name_strategy() -> st.SearchStrategy[str]:
    return st.from_regex(r"X-[A-Z][A-Za-z0-9-]{0,10}", fullmatch=True)


def _header_value_strategy() -> st.SearchStrategy[str]:
    return st.text(
        alphabet=st.characters(blacklist_categories=["Cs"]),
        min_size=1,
        max_size=12,
    )


def _subnet_strategy() -> st.SearchStrategy[str]:
    return st.sampled_from(["10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"])


def _build_list_policy(data: dict[str, Any]) -> NetworkPolicy:
    policy: dict[str, Any] = {"allow": data["allow"]}
    subnets = data.get("subnets")
    if subnets is not None:
        policy["subnets"] = subnets
    return cast(NetworkPolicy, policy)


def _list_policy_strategy() -> st.SearchStrategy[NetworkPolicy]:
    subnet_list = st.lists(_subnet_strategy(), unique=True, max_size=3)
    subnets = st.one_of(
        st.none(),
        st.fixed_dictionaries(
            {
                "allow": subnet_list,
                "deny": subnet_list,
            },
        ),
        st.fixed_dictionaries({"allow": subnet_list}),
        st.fixed_dictionaries({"deny": subnet_list}),
    )
    return st.fixed_dictionaries(
        {
            "allow": st.lists(_domain_strategy(), unique=True, max_size=4),
            "subnets": subnets,
        },
    ).map(_build_list_policy)


def _build_record_policy(data: dict[str, Any]) -> NetworkPolicy:
    policy: dict[str, Any] = {"allow": data["allow"]}
    subnets = data.get("subnets")
    if subnets is not None:
        policy["subnets"] = subnets
    return cast(NetworkPolicy, policy)


def _record_policy_strategy() -> st.SearchStrategy[NetworkPolicy]:
    headers = st.dictionaries(_header_name_strategy(), _header_value_strategy(), max_size=4)
    transform = st.fixed_dictionaries({"headers": headers})
    rule = st.fixed_dictionaries(
        {
            "transform": st.lists(transform, max_size=3),
        },
    )
    rules = st.lists(rule, max_size=3)
    subnets = st.one_of(
        st.none(),
        st.fixed_dictionaries(
            {
                "allow": st.lists(_subnet_strategy(), unique=True, max_size=3),
                "deny": st.lists(_subnet_strategy(), unique=True, max_size=3),
            },
        ),
        st.fixed_dictionaries({"allow": st.lists(_subnet_strategy(), unique=True, max_size=3)}),
        st.fixed_dictionaries({"deny": st.lists(_subnet_strategy(), unique=True, max_size=3)}),
    )
    return st.fixed_dictionaries(
        {
            "allow": st.dictionaries(_domain_strategy(), rules, max_size=4),
            "subnets": subnets,
        },
    ).map(_build_record_policy)


def _policy_semantics(policy: NetworkPolicy) -> Any:
    if isinstance(policy, str):
        return ("mode", policy)

    allow = policy["allow"]
    subnets = policy.get("subnets")
    if isinstance(allow, list):
        subnet_semantics = None
        if subnets is not None:
            subnet_semantics = (
                tuple(sorted(subnets.get("allow", []))),
                tuple(sorted(subnets.get("deny", []))),
            )
        return ("list", tuple(sorted(allow)), subnet_semantics)

    domain_semantics = tuple(
        sorted((domain, frozenset(_rule_header_names(rules))) for domain, rules in allow.items())
    )
    subnet_semantics = None
    if subnets is not None:
        subnet_semantics = (
            tuple(sorted(subnets.get("allow", []))),
            tuple(sorted(subnets.get("deny", []))),
        )
    return ("record", domain_semantics, subnet_semantics)


class TestNetworkPolicyModes:
    @pytest.mark.parametrize(
        ("policy", "api_payload"),
        [
            ("allow-all", {"mode": "allow-all"}),
            ("deny-all", {"mode": "deny-all"}),
        ],
        ids=["allow_all", "deny_all"],
    )
    def test_mode_strings_round_trip_exactly(
        self, policy: NetworkPolicy, api_payload: dict[str, Any]
    ) -> None:
        assert to_api_network_policy(policy) == api_payload
        assert from_api_network_policy(api_payload) == policy

    @given(st.sampled_from(["allow-all", "deny-all"]))
    @settings(max_examples=2, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_mode_strings_round_trip_property_based(self, policy: NetworkPolicy) -> None:
        assert from_api_network_policy(to_api_network_policy(policy)) == policy


class TestNetworkPolicyExamples:
    def test_custom_list_form_converts_with_subnets(self) -> None:
        policy: NetworkPolicy = {
            "allow": ["example.com", "*.example.net"],
            "subnets": {"allow": ["10.0.0.0/8"], "deny": ["192.168.0.0/16"]},
        }
        api_payload = {
            "mode": "custom",
            "allowedDomains": ["example.com", "*.example.net"],
            "allowedCIDRs": ["10.0.0.0/8"],
            "deniedCIDRs": ["192.168.0.0/16"],
        }

        assert to_api_network_policy(policy) == api_payload
        assert from_api_network_policy(api_payload) == policy

    def test_custom_record_form_converts_with_injection_rules(self) -> None:
        policy: NetworkPolicy = {
            "allow": {
                "example.com": [{"transform": [{"headers": _headers("X-API-Key")}]}],
            },
        }
        api_payload = {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [
                {
                    "domain": "example.com",
                    "headers": {"X-API-Key": "value-for-x-api-key"},
                }
            ],
        }
        api_response = {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [{"domain": "example.com", "headerNames": ["X-API-Key"]}],
        }

        assert to_api_network_policy(policy) == api_payload
        assert from_api_network_policy(api_response) == {
            "allow": {
                "example.com": [
                    {"transform": [{"headers": {"X-API-Key": "<redacted>"}}]},
                ]
            }
        }

    def test_mixed_record_form_merges_headers_per_domain(self) -> None:
        policy: NetworkPolicy = {
            "allow": {
                "*": [{"transform": [{"headers": _headers("X-Wild")}]}],
                "api.example.com": [{"transform": [{"headers": _headers("X-Api", "X-Dupe")}]}],
                "docs.example.com": [{"transform": [{"headers": _headers("X-Dupe", "X-Docs")}]}],
            },
        }
        api_payload = {
            "mode": "custom",
            "allowedDomains": ["*", "api.example.com", "docs.example.com"],
            "injectionRules": [
                {
                    "domain": "*",
                    "headers": {"X-Wild": "value-for-x-wild"},
                },
                {
                    "domain": "api.example.com",
                    "headers": {
                        "X-Api": "value-for-x-api",
                        "X-Dupe": "value-for-x-dupe",
                    },
                },
                {
                    "domain": "docs.example.com",
                    "headers": {
                        "X-Dupe": "value-for-x-dupe",
                        "X-Docs": "value-for-x-docs",
                    },
                },
            ],
        }

        assert to_api_network_policy(policy) == api_payload
        assert from_api_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["*", "api.example.com", "docs.example.com"],
                "injectionRules": [
                    {"domain": "*", "headerNames": ["X-Wild"]},
                    {"domain": "api.example.com", "headerNames": ["X-Api", "X-Dupe"]},
                    {"domain": "docs.example.com", "headerNames": ["X-Dupe", "X-Docs"]},
                ],
            }
        ) == {
            "allow": {
                "*": [{"transform": [{"headers": {"X-Wild": "<redacted>"}}]}],
                "api.example.com": [
                    {"transform": [{"headers": {"X-Api": "<redacted>", "X-Dupe": "<redacted>"}}]}
                ],
                "docs.example.com": [
                    {"transform": [{"headers": {"X-Dupe": "<redacted>", "X-Docs": "<redacted>"}}]}
                ],
            }
        }


class TestNetworkPolicyEdgeCases:
    def test_empty_rule_arrays_remain_valid_allowed_domains(self) -> None:
        policy: NetworkPolicy = {"allow": {"example.com": []}}

        assert to_api_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com"],
        }
        assert from_api_network_policy({"mode": "custom", "allowedDomains": ["example.com"]}) == {
            "allow": ["example.com"]
        }

    def test_domains_with_empty_transforms_do_not_emit_injection_rules(self) -> None:
        policy: NetworkPolicy = {
            "allow": {
                "example.com": [{"transform": [{"headers": {}}]}],
                "api.example.com": [{"transform": [{"headers": _headers("X-Api")}]}],
            },
        }

        assert to_api_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com", "api.example.com"],
            "injectionRules": [
                {
                    "domain": "api.example.com",
                    "headers": {"X-Api": "value-for-x-api"},
                }
            ],
        }

    def test_multiple_transforms_for_same_domain_merge_headers(self) -> None:
        policy: NetworkPolicy = {
            "allow": {
                "example.com": [
                    {
                        "transform": [
                            {
                                "headers": {
                                    "X-First": "one",
                                    "X-Dupe": "first",
                                }
                            }
                        ]
                    },
                    {
                        "transform": [
                            {
                                "headers": {
                                    "X-Dupe": "second",
                                    "X-Second": "two",
                                }
                            }
                        ]
                    },
                ],
            },
        }

        assert to_api_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [
                {
                    "domain": "example.com",
                    "headers": {
                        "X-First": "one",
                        "X-Dupe": "second",
                        "X-Second": "two",
                    },
                }
            ],
        }

    def test_injection_rules_for_unknown_domains_surface_in_public_result(self) -> None:
        assert from_api_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["example.com"],
                "injectionRules": [{"domain": "api.example.com", "headerNames": ["X-Trace"]}],
            }
        ) == {
            "allow": {
                "example.com": [],
                "api.example.com": [{"transform": [{"headers": {"X-Trace": "<redacted>"}}]}],
            }
        }

    def test_missing_subnet_fields_do_not_create_empty_subnets(self) -> None:
        assert from_api_network_policy({"mode": "custom", "allowedDomains": ["example.com"]}) == {
            "allow": ["example.com"]
        }

    def test_missing_optional_fields_do_not_raise_conversion_errors(self) -> None:
        assert from_api_network_policy({"mode": "custom"}) == {"allow": []}


class TestNetworkPolicyGeneratedCases:
    @given(_list_policy_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_list_form_policies_round_trip_exactly(self, policy: NetworkPolicy) -> None:
        assert _policy_semantics(from_api_network_policy(to_api_network_policy(policy))) == (
            _policy_semantics(policy)
        )

    @given(_record_policy_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_record_form_policies_preserve_domains_and_header_names(
        self, policy: NetworkPolicy
    ) -> None:
        assert _record_policy_domains(from_api_network_policy(to_api_network_policy(policy))) == (
            _record_policy_domains(policy)
        )
