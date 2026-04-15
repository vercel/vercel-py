"""Conversion contract tests for sandbox network policies."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from vercel._internal.sandbox.models import parse_network_policy, serialize_network_policy
from vercel.sandbox import (
    NetworkPolicy,
    NetworkPolicyCustom,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkTransformer,
)


def _headers(*names: str) -> dict[str, str]:
    return {name: f"value-for-{name.lower()}" for name in names}


def _record_policy_domains(policy: NetworkPolicy) -> dict[str, set[str]]:
    if isinstance(policy, str):
        return {}

    if isinstance(policy.allow, list):
        return {domain: set() for domain in policy.allow}

    domains: dict[str, set[str]] = {}
    for domain, rules in policy.allow.items():
        domains[domain] = _rule_header_names(rules)
    return domains


def _rule_header_names(rules: Iterable[NetworkPolicyRule]) -> set[str]:
    header_names: set[str] = set()
    for rule in rules:
        for transform in rule.transform or []:
            header_names.update((transform.headers or {}).keys())
    return header_names


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


def _subnets_from_data(data: dict[str, Any] | None) -> NetworkPolicySubnets | None:
    if data is None:
        return None
    return NetworkPolicySubnets(allow=data.get("allow"), deny=data.get("deny"))


def _build_list_policy(data: dict[str, Any]) -> NetworkPolicyCustom:
    return NetworkPolicyCustom(
        allow=data["allow"],
        subnets=_subnets_from_data(data.get("subnets")),
    )


def _list_policy_strategy() -> st.SearchStrategy[NetworkPolicyCustom]:
    subnet_list = st.lists(_subnet_strategy(), unique=True, max_size=3)
    subnets = st.one_of(
        st.none(),
        st.fixed_dictionaries({"allow": subnet_list, "deny": subnet_list}),
        st.fixed_dictionaries({"allow": subnet_list}),
        st.fixed_dictionaries({"deny": subnet_list}),
    )
    return st.fixed_dictionaries(
        {
            "allow": st.lists(_domain_strategy(), unique=True, max_size=4),
            "subnets": subnets,
        }
    ).map(_build_list_policy)


def _build_record_policy(data: dict[str, Any]) -> NetworkPolicyCustom:
    return NetworkPolicyCustom(
        allow=data["allow"],
        subnets=_subnets_from_data(data.get("subnets")),
    )


def _record_policy_strategy() -> st.SearchStrategy[NetworkPolicyCustom]:
    headers = st.dictionaries(_header_name_strategy(), _header_value_strategy(), max_size=4)
    transform = st.builds(NetworkTransformer, headers=headers)
    rule = st.builds(NetworkPolicyRule, transform=st.lists(transform, max_size=3))
    rules = st.lists(rule, max_size=3)
    subnets = st.one_of(
        st.none(),
        st.fixed_dictionaries(
            {
                "allow": st.lists(_subnet_strategy(), unique=True, max_size=3),
                "deny": st.lists(_subnet_strategy(), unique=True, max_size=3),
            }
        ),
        st.fixed_dictionaries({"allow": st.lists(_subnet_strategy(), unique=True, max_size=3)}),
        st.fixed_dictionaries({"deny": st.lists(_subnet_strategy(), unique=True, max_size=3)}),
    )
    return st.fixed_dictionaries(
        {
            "allow": st.dictionaries(_domain_strategy(), rules, max_size=4),
            "subnets": subnets,
        }
    ).map(_build_record_policy)


def _policy_semantics(policy: NetworkPolicy) -> Any:
    if isinstance(policy, str):
        return ("mode", policy)

    subnet_semantics = None
    if policy.subnets is not None:
        subnet_semantics = (
            tuple(sorted(policy.subnets.allow or [])),
            tuple(sorted(policy.subnets.deny or [])),
        )

    if isinstance(policy.allow, list):
        return ("list", tuple(sorted(policy.allow)), subnet_semantics)

    domain_semantics = tuple(
        sorted(
            (domain, frozenset(_rule_header_names(rules))) for domain, rules in policy.allow.items()
        )
    )
    return ("record", domain_semantics, subnet_semantics)


def _subnet_semantics(policy: NetworkPolicy) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    if isinstance(policy, str) or policy.subnets is None:
        return None

    return (
        tuple(sorted(policy.subnets.allow or [])),
        tuple(sorted(policy.subnets.deny or [])),
    )


def _normalized_custom_policy_semantics(
    policy: NetworkPolicy,
) -> tuple[tuple[tuple[str, frozenset[str]], ...], tuple[tuple[str, ...], tuple[str, ...]] | None]:
    if isinstance(policy, str):
        raise TypeError("expected custom policy")

    domain_semantics = tuple(
        sorted(
            (domain, frozenset(name.lower() for name in names))
            for domain, names in _record_policy_domains(policy).items()
        )
    )
    return (domain_semantics, _subnet_semantics(policy))


def _header_values(policy: NetworkPolicyCustom) -> set[str]:
    if isinstance(policy.allow, list):
        return set()

    values: set[str] = set()
    for rules in policy.allow.values():
        for rule in rules:
            for transform in rule.transform or []:
                values.update((transform.headers or {}).values())
    return values


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
        self, policy: NetworkPolicy, api_payload: dict[str, str]
    ) -> None:
        assert serialize_network_policy(policy) == api_payload
        assert parse_network_policy(api_payload) == policy

    @given(st.sampled_from(["allow-all", "deny-all"]))
    @settings(max_examples=2, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_mode_strings_round_trip_property_based(self, policy: NetworkPolicy) -> None:
        assert parse_network_policy(serialize_network_policy(policy)) == policy

    @pytest.mark.parametrize("policy", ["allow-all", "deny-all"])
    def test_internal_codec_round_trip_modes(self, policy: NetworkPolicy) -> None:
        assert parse_network_policy(serialize_network_policy(policy)) == policy


class TestNetworkPolicyExamples:
    def test_custom_list_form_converts_with_subnets(self) -> None:
        policy = NetworkPolicyCustom(
            allow=["example.com", "*.example.net"],
            subnets=NetworkPolicySubnets(
                allow=["10.0.0.0/8"],
                deny=["192.168.0.0/16"],
            ),
        )
        api_payload = {
            "mode": "custom",
            "allowedDomains": ["example.com", "*.example.net"],
            "allowedCIDRs": ["10.0.0.0/8"],
            "deniedCIDRs": ["192.168.0.0/16"],
        }

        assert serialize_network_policy(policy) == api_payload
        assert parse_network_policy(api_payload) == policy

    def test_custom_record_form_converts_with_injection_rules(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers=_headers("X-API-Key"))])
                ]
            }
        )
        api_payload = {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [
                {"domain": "example.com", "headers": {"X-API-Key": "value-for-x-api-key"}}
            ],
        }
        api_response = {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [{"domain": "example.com", "headerNames": ["X-API-Key"]}],
        }

        assert serialize_network_policy(policy) == api_payload
        assert parse_network_policy(api_response) == NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"X-API-Key": "<redacted>"})]
                    )
                ]
            }
        )

    def test_mixed_record_form_merges_headers_per_domain(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "*": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers=_headers("X-Wild"))])
                ],
                "api.example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers=_headers("X-Api", "X-Dupe"))]
                    )
                ],
                "docs.example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers=_headers("X-Dupe", "X-Docs"))]
                    )
                ],
            }
        )
        api_payload = {
            "mode": "custom",
            "allowedDomains": ["*", "api.example.com", "docs.example.com"],
            "injectionRules": [
                {"domain": "*", "headers": {"X-Wild": "value-for-x-wild"}},
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

        assert serialize_network_policy(policy) == api_payload
        assert parse_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["*", "api.example.com", "docs.example.com"],
                "injectionRules": [
                    {"domain": "*", "headerNames": ["X-Wild"]},
                    {"domain": "api.example.com", "headerNames": ["X-Api", "X-Dupe"]},
                    {"domain": "docs.example.com", "headerNames": ["X-Dupe", "X-Docs"]},
                ],
            }
        ) == NetworkPolicyCustom(
            allow={
                "*": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"X-Wild": "<redacted>"})]
                    )
                ],
                "api.example.com": [
                    NetworkPolicyRule(
                        transform=[
                            NetworkTransformer(
                                headers={"X-Api": "<redacted>", "X-Dupe": "<redacted>"}
                            )
                        ]
                    )
                ],
                "docs.example.com": [
                    NetworkPolicyRule(
                        transform=[
                            NetworkTransformer(
                                headers={"X-Dupe": "<redacted>", "X-Docs": "<redacted>"}
                            )
                        ]
                    )
                ],
            }
        )


class TestNetworkPolicyEdgeCases:
    def test_empty_rule_arrays_remain_valid_allowed_domains(self) -> None:
        policy = NetworkPolicyCustom(allow={"example.com": []})

        assert serialize_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com"],
        }
        assert parse_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["example.com"],
            }
        ) == NetworkPolicyCustom(allow=["example.com"])

    def test_domains_with_empty_transforms_do_not_emit_injection_rules(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [NetworkPolicyRule(transform=[NetworkTransformer(headers={})])],
                "api.example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers=_headers("X-Api"))])
                ],
            }
        )

        assert serialize_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com", "api.example.com"],
            "injectionRules": [
                {"domain": "api.example.com", "headers": {"X-Api": "value-for-x-api"}}
            ],
        }

    def test_multiple_transforms_for_same_domain_merge_headers(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[
                            NetworkTransformer(headers={"X-First": "one", "X-Dupe": "first"})
                        ]
                    ),
                    NetworkPolicyRule(
                        transform=[
                            NetworkTransformer(headers={"X-Dupe": "second", "X-Second": "two"})
                        ]
                    ),
                ]
            }
        )

        assert serialize_network_policy(policy) == {
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

    def test_multiple_transforms_merge_case_insensitive_header_names(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers={"X-Trace": "first"})]),
                    NetworkPolicyRule(
                        transform=[
                            NetworkTransformer(
                                headers={"x-trace": "second", "X-Other": "other-value"}
                            )
                        ]
                    ),
                ]
            }
        )

        assert serialize_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [
                {
                    "domain": "example.com",
                    "headers": {"x-trace": "second", "X-Other": "other-value"},
                }
            ],
        }

    def test_single_rule_preserves_distinct_case_variants_in_api_headers(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[
                            NetworkTransformer(headers={"X-Trace": "first", "x-trace": "second"})
                        ]
                    )
                ]
            }
        )

        assert serialize_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [
                {
                    "domain": "example.com",
                    "headers": {"X-Trace": "first", "x-trace": "second"},
                }
            ],
        }

    def test_later_rules_replace_earlier_case_variants_for_same_header(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers={"X-Trace": "first"})]),
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"x-trace": "second"})]
                    ),
                    NetworkPolicyRule(transform=[NetworkTransformer(headers={"X-Other": "other"})]),
                ]
            }
        )

        assert serialize_network_policy(policy) == {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [
                {
                    "domain": "example.com",
                    "headers": {"x-trace": "second", "X-Other": "other"},
                }
            ],
        }

    def test_api_response_header_names_merge_case_insensitively(self) -> None:
        assert parse_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["example.com"],
                "injectionRules": [
                    {"domain": "example.com", "headerNames": ["X-Trace", "x-trace", "X-Other"]}
                ],
            }
        ) == NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[
                            NetworkTransformer(
                                headers={"x-trace": "<redacted>", "X-Other": "<redacted>"}
                            )
                        ]
                    )
                ]
            }
        )

    def test_api_response_header_names_keep_last_case_variant(self) -> None:
        assert parse_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["example.com"],
                "injectionRules": [
                    {"domain": "example.com", "headerNames": ["X-Trace", "x-trace", "X-TRACE"]}
                ],
            }
        ) == NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"X-TRACE": "<redacted>"})]
                    )
                ]
            }
        )

    def test_empty_api_header_names_fall_back_to_headers(self) -> None:
        assert parse_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["example.com"],
                "injectionRules": [
                    {
                        "domain": "example.com",
                        "headers": {"X-Trace": "trace-value"},
                        "headerNames": [],
                    }
                ],
            }
        ) == NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"X-Trace": "<redacted>"})]
                    )
                ]
            }
        )

    def test_injection_rules_for_unknown_domains_surface_in_public_result(self) -> None:
        assert parse_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["example.com"],
                "injectionRules": [{"domain": "api.example.com", "headerNames": ["X-Trace"]}],
            }
        ) == NetworkPolicyCustom(
            allow={
                "example.com": [],
                "api.example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"X-Trace": "<redacted>"})]
                    )
                ],
            }
        )

    def test_missing_subnet_fields_do_not_create_empty_subnets(self) -> None:
        assert parse_network_policy(
            {
                "mode": "custom",
                "allowedDomains": ["example.com"],
            }
        ) == NetworkPolicyCustom(allow=["example.com"])

    def test_missing_optional_fields_do_not_raise_conversion_errors(self) -> None:
        assert parse_network_policy({"mode": "custom"}) == NetworkPolicyCustom(allow=[])


class TestNetworkPolicyGeneratedCases:
    @given(_list_policy_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_list_form_policies_round_trip_exactly(
        self, policy: NetworkPolicyCustom
    ) -> None:
        assert _policy_semantics(parse_network_policy(serialize_network_policy(policy))) == (
            _policy_semantics(policy)
        )

    @given(_record_policy_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_record_form_policies_preserve_normalized_semantics(
        self, policy: NetworkPolicyCustom
    ) -> None:
        round_tripped = parse_network_policy(serialize_network_policy(policy))

        assert isinstance(round_tripped, NetworkPolicyCustom)
        assert _normalized_custom_policy_semantics(
            round_tripped
        ) == _normalized_custom_policy_semantics(policy)

    @given(_record_policy_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_record_form_policies_decode_to_redacted_headers(
        self, policy: NetworkPolicyCustom
    ) -> None:
        round_tripped = parse_network_policy(serialize_network_policy(policy))

        assert isinstance(round_tripped, NetworkPolicyCustom)
        assert _header_values(round_tripped) <= {"<redacted>"}
