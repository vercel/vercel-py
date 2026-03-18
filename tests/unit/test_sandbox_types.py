"""Unit tests for public sandbox network policy types."""

from vercel.sandbox.types import (
    NetworkPolicy,
    NetworkPolicyCustom,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkTransformer,
)


class TestSandboxNetworkPolicyTypes:
    def test_typed_dict_fields_are_public(self) -> None:
        assert NetworkTransformer.__optional_keys__ == frozenset({"headers"})
        assert NetworkPolicyRule.__optional_keys__ == frozenset({"transform"})
        assert NetworkPolicySubnets.__optional_keys__ == frozenset({"allow", "deny"})
        assert NetworkPolicyCustom.__optional_keys__ == frozenset({"allow", "subnets"})

    def test_network_policy_alias_is_importable(self) -> None:
        assert NetworkPolicy is not None
