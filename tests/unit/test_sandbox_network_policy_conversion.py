"""Conversion contract tests for sandbox network policies."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from vercel._internal.sandbox.network_policy import (
    ApiNetworkInjectionRule,
    ApiNetworkPolicy,
)
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


def _case_insensitive_headers(items: Iterable[tuple[str, str]]) -> dict[str, str]:
    merged: dict[str, tuple[str, str]] = {}
    for name, value in items:
        merged[name.lower()] = (name, value)
    return dict(merged.values())


def _domain_strategy() -> st.SearchStrategy[str]:
    label = st.from_regex(r"[a-z][a-z0-9-]{0,5}", fullmatch=True)
    wildcard = st.just("*")
    subdomain = st.builds(lambda left, right: f"{left}.{right}", label, label)
    return st.one_of(label, subdomain, wildcard)


def _header_name_strategy() -> st.SearchStrategy[str]:
    return st.from_regex(r"X-[A-Z][A-Za-z0-9-]{0,10}", fullmatch=True)


@st.composite
def _header_name_case_variant_strategy(draw: st.DrawFn) -> str:
    canonical = draw(st.from_regex(r"x-[a-z][a-z0-9-]{0,10}", fullmatch=True))
    chars: list[str] = []
    for char in canonical:
        if char.isalpha():
            chars.append(char.upper() if draw(st.booleans()) else char.lower())
        else:
            chars.append(char)
    return "".join(chars)


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


@st.composite
def _duplicate_header_assignments_strategy(
    draw: st.DrawFn,
) -> list[tuple[str, str]]:
    canonical_names = draw(
        st.lists(
            st.from_regex(r"x-[a-z][a-z0-9-]{0,10}", fullmatch=True),
            min_size=1,
            max_size=4,
            unique=True,
        )
    )
    assignments: list[tuple[str, str]] = []
    for canonical_name in canonical_names:
        variant_count = draw(st.integers(min_value=1, max_value=3))
        for _ in range(variant_count):
            variant_chars: list[str] = []
            for char in canonical_name:
                if char.isalpha():
                    variant_chars.append(char.upper() if draw(st.booleans()) else char.lower())
                else:
                    variant_chars.append(char)
            assignments.append(
                (
                    "".join(variant_chars),
                    draw(_header_value_strategy()),
                )
            )
    return draw(st.permutations(assignments).map(list))


class TestNetworkPolicyModes:
    @pytest.mark.parametrize(
        ("policy", "api_payload"),
        [
            ("allow-all", ApiNetworkPolicy(mode="allow-all")),
            ("deny-all", ApiNetworkPolicy(mode="deny-all")),
        ],
        ids=["allow_all", "deny_all"],
    )
    def test_mode_strings_round_trip_exactly(
        self, policy: NetworkPolicy, api_payload: ApiNetworkPolicy
    ) -> None:
        assert ApiNetworkPolicy.from_network_policy(policy) == api_payload
        assert api_payload.to_network_policy() == policy

    @given(st.sampled_from(["allow-all", "deny-all"]))
    @settings(max_examples=2, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_mode_strings_round_trip_property_based(self, policy: NetworkPolicy) -> None:
        assert ApiNetworkPolicy.from_network_policy(policy).to_network_policy() == policy

    @pytest.mark.parametrize("policy", ["allow-all", "deny-all"])
    def test_api_network_policy_codec_methods_round_trip_modes(self, policy: NetworkPolicy) -> None:
        assert ApiNetworkPolicy.from_network_policy(policy).to_network_policy() == policy


class TestNetworkPolicyExamples:
    def test_custom_list_form_converts_with_subnets(self) -> None:
        policy = NetworkPolicyCustom(
            allow=["example.com", "*.example.net"],
            subnets=NetworkPolicySubnets(
                allow=["10.0.0.0/8"],
                deny=["192.168.0.0/16"],
            ),
        )
        api_payload = ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com", "*.example.net"],
            allowed_cidrs=["10.0.0.0/8"],
            denied_cidrs=["192.168.0.0/16"],
        )

        assert ApiNetworkPolicy.from_network_policy(policy) == api_payload
        assert api_payload.to_network_policy() == policy

    def test_custom_record_form_converts_with_injection_rules(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers=_headers("X-API-Key"))])
                ]
            }
        )
        api_payload = ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    headers={"X-API-Key": "value-for-x-api-key"},
                )
            ],
        )
        api_response = ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    header_names=["X-API-Key"],
                )
            ],
        )

        assert ApiNetworkPolicy.from_network_policy(policy) == api_payload
        assert api_response.to_network_policy() == NetworkPolicyCustom(
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
        api_payload = ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["*", "api.example.com", "docs.example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(domain="*", headers={"X-Wild": "value-for-x-wild"}),
                ApiNetworkInjectionRule(
                    domain="api.example.com",
                    headers={
                        "X-Api": "value-for-x-api",
                        "X-Dupe": "value-for-x-dupe",
                    },
                ),
                ApiNetworkInjectionRule(
                    domain="docs.example.com",
                    headers={
                        "X-Dupe": "value-for-x-dupe",
                        "X-Docs": "value-for-x-docs",
                    },
                ),
            ],
        )

        assert ApiNetworkPolicy.from_network_policy(policy) == api_payload
        assert ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["*", "api.example.com", "docs.example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(domain="*", header_names=["X-Wild"]),
                ApiNetworkInjectionRule(
                    domain="api.example.com",
                    header_names=["X-Api", "X-Dupe"],
                ),
                ApiNetworkInjectionRule(
                    domain="docs.example.com",
                    header_names=["X-Dupe", "X-Docs"],
                ),
            ],
        ).to_network_policy() == NetworkPolicyCustom(
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

        assert ApiNetworkPolicy.from_network_policy(policy) == ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
        )
        assert ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
        ).to_network_policy() == NetworkPolicyCustom(allow=["example.com"])

    def test_domains_with_empty_transforms_do_not_emit_injection_rules(self) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [NetworkPolicyRule(transform=[NetworkTransformer(headers={})])],
                "api.example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers=_headers("X-Api"))])
                ],
            }
        )

        assert ApiNetworkPolicy.from_network_policy(policy) == ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com", "api.example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="api.example.com",
                    headers={"X-Api": "value-for-x-api"},
                )
            ],
        )

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

        assert ApiNetworkPolicy.from_network_policy(policy) == ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    headers={
                        "X-First": "one",
                        "X-Dupe": "second",
                        "X-Second": "two",
                    },
                )
            ],
        )

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

        assert ApiNetworkPolicy.from_network_policy(policy) == ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    headers={"x-trace": "second", "X-Other": "other-value"},
                )
            ],
        )

    def test_api_response_header_names_merge_case_insensitively(self) -> None:
        assert ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    header_names=["X-Trace", "x-trace", "X-Other"],
                )
            ],
        ).to_network_policy() == NetworkPolicyCustom(
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

    def test_api_network_policy_to_dict_uses_wire_keys(self) -> None:
        policy = ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    headers={"X-Trace": "trace-value"},
                )
            ],
            allowed_cidrs=["10.0.0.0/8"],
            denied_cidrs=["192.168.0.0/16"],
        )

        assert policy.to_dict() == {
            "mode": "custom",
            "allowedDomains": ["example.com"],
            "injectionRules": [
                {
                    "domain": "example.com",
                    "headers": {"X-Trace": "trace-value"},
                }
            ],
            "allowedCIDRs": ["10.0.0.0/8"],
            "deniedCIDRs": ["192.168.0.0/16"],
        }

    def test_injection_rules_for_unknown_domains_surface_in_public_result(self) -> None:
        assert ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="api.example.com",
                    header_names=["X-Trace"],
                )
            ],
        ).to_network_policy() == NetworkPolicyCustom(
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
        assert ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
        ).to_network_policy() == NetworkPolicyCustom(allow=["example.com"])

    def test_missing_optional_fields_do_not_raise_conversion_errors(self) -> None:
        assert ApiNetworkPolicy(mode="custom").to_network_policy() == NetworkPolicyCustom(allow=[])


class TestNetworkPolicyGeneratedCases:
    @given(_list_policy_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_list_form_policies_round_trip_exactly(
        self, policy: NetworkPolicyCustom
    ) -> None:
        assert _policy_semantics(
            ApiNetworkPolicy.from_network_policy(policy).to_network_policy()
        ) == _policy_semantics(policy)

    @given(_record_policy_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_record_form_policies_preserve_domains_and_header_names(
        self, policy: NetworkPolicyCustom
    ) -> None:
        assert _record_policy_domains(
            ApiNetworkPolicy.from_network_policy(policy).to_network_policy()
        ) == (_record_policy_domains(policy))

    @given(_duplicate_header_assignments_strategy())
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_record_form_merges_duplicate_headers_case_insensitively(
        self, assignments: list[tuple[str, str]]
    ) -> None:
        policy = NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers={name: value})])
                    for name, value in assignments
                ]
            }
        )

        api_policy = ApiNetworkPolicy.from_network_policy(policy)

        assert api_policy == ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    headers=_case_insensitive_headers(assignments),
                )
            ],
        )

    @given(st.lists(_header_name_case_variant_strategy(), min_size=1, max_size=8))
    @settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_generated_api_header_names_collapse_case_insensitive_duplicates(
        self, header_names: list[str]
    ) -> None:
        expected_headers = dict.fromkeys(
            _case_insensitive_headers((name, "<redacted>") for name in header_names),
            "<redacted>",
        )

        assert ApiNetworkPolicy(
            mode="custom",
            allowed_domains=["example.com"],
            injection_rules=[
                ApiNetworkInjectionRule(
                    domain="example.com",
                    header_names=header_names,
                )
            ],
        ).to_network_policy() == NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(transform=[NetworkTransformer(headers=expected_headers)])
                ]
            }
        )
