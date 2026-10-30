"""Behavior of the network policy format shared with the JS SDK and CLI."""

import traceback
from copy import deepcopy
from typing import Any

import pytest
from hypothesis import given, strategies as st

from vercel.sandbox import (
    NetworkPolicy,
    NetworkPolicyParseError,
    NetworkPolicySubnets,
    parse_network_policy,
    sync,
)
from vercel.sandbox._internal.models import _serialize_network_policy


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ("allow-all", NetworkPolicy.allow_all()),
        ("deny-all", NetworkPolicy.deny_all()),
        ({}, NetworkPolicy.custom()),
        ({"allow": []}, NetworkPolicy.custom()),
        (
            {"allow": ["*.npmjs.org", "github.com"]},
            NetworkPolicy.custom(
                allow={"*.npmjs.org": (), "github.com": ()},
            ),
        ),
        (
            {"subnets": {"deny": ["10.0.0.0/8"]}},
            NetworkPolicy.custom(
                subnets=NetworkPolicySubnets(deny=["10.0.0.0/8"]),
            ),
        ),
    ],
)
def test_parse_network_policy_accepts_modes_and_custom_shapes(
    data: object, expected: NetworkPolicy
) -> None:
    assert parse_network_policy(data) == expected
    assert sync.parse_network_policy is parse_network_policy
    assert sync.NetworkPolicyParseError is NetworkPolicyParseError
    assert "parse_network_policy" in sync.__all__
    assert "NetworkPolicyParseError" in sync.__all__


def _policy() -> dict[str, Any]:
    return {
        "allow": {
            "registry.npmjs.org": [
                {
                    "match": {
                        "method": ["GET", "HEAD"],
                        "path": {"startsWith": "/v1/"},
                        "headers": [{"key": {"exact": "x-token"}}],
                        "queryString": [{"value": {"regex": "^read$"}}],
                    },
                    "transform": [{"headers": {"authorization": "Bearer ZZSECRETZZ"}}],
                },
                {"forwardURL": "https://proxy.example.com"},
                {"response": {"statusCode": 403, "body": "no", "contentType": "text/plain"}},
            ],
            "*": [],
        },
        "subnets": {"allow": ["10.0.0.0/8"], "deny": ["10.1.0.0/16"]},
    }


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("subnets",),
        ("allow", "registry.npmjs.org", 0),
        ("allow", "registry.npmjs.org", 0, "match"),
        ("allow", "registry.npmjs.org", 0, "match", "path"),
        ("allow", "registry.npmjs.org", 0, "match", "headers", 0),
        ("allow", "registry.npmjs.org", 0, "match", "headers", 0, "key"),
        ("allow", "registry.npmjs.org", 0, "match", "queryString", 0),
        ("allow", "registry.npmjs.org", 0, "transform", 0),
        ("allow", "registry.npmjs.org", 2, "response"),
    ],
)
def test_parse_network_policy_rejects_unknown_keys_at_every_level(
    path: tuple[str | int, ...],
) -> None:
    data = _policy()
    target: Any = data
    for key in path:
        target = target[key]
    target["typo"] = "ZZSECRETZZ"

    with pytest.raises(NetworkPolicyParseError) as caught:
        parse_network_policy(data)

    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert len(caught.value.issues) == 1
    assert caught.value.issues[0].endswith("typo: unknown key")
    assert "ZZSECRETZZ" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("allow",), None),
        (("allow",), "registry.npmjs.org"),
        (("allow",), [1]),
        (("subnets", "deny"), "10.0.0.0/8"),
        (("subnets", "allow"), [None]),
        (("allow", "registry.npmjs.org"), {}),
        (("allow", "registry.npmjs.org", 0, "match", "method"), "GET"),
        (("allow", "registry.npmjs.org", 0, "match", "method"), [1]),
        (("allow", "registry.npmjs.org", 0, "match", "path"), {"kind": "exact", "value": "/"}),
        (("allow", "registry.npmjs.org", 0, "match", "path"), {"startsWith": None}),
        (("allow", "registry.npmjs.org", 0, "match", "path"), {"exact": "/", "regex": ".*"}),
        (("allow", "registry.npmjs.org", 0, "transform"), None),
        (("allow", "registry.npmjs.org", 0, "transform", 0, "headers"), {"a": 1}),
        (("allow", "registry.npmjs.org", 1, "forwardURL"), 1),
        (("allow", "registry.npmjs.org", 2, "response", "statusCode"), 403.5),
        (("allow", "registry.npmjs.org", 2, "response", "statusCode"), True),
        (("allow", "registry.npmjs.org", 2, "response", "statusCode"), "403"),
        (("allow", "registry.npmjs.org", 2, "response", "statusCode"), 600),
        (("allow", "registry.npmjs.org", 2, "response", "statusCode"), 204),
        (("allow", "registry.npmjs.org", 2, "response", "contentType"), None),
        (("allow", "registry.npmjs.org", 2, "response", "headers"), {"Content-Length": "0"}),
        (("allow", "registry.npmjs.org", 2, "response", "headers"), {"X-A": "1", "x-a": "2"}),
        (("allow", "registry.npmjs.org", 0), {}),
        (("allow", "registry.npmjs.org", 0), {"transform": [], "response": {"statusCode": 403}}),
    ],
)
def test_parse_network_policy_rejects_invalid_rules_without_values(
    path: tuple[str | int, ...],
    value: object,
) -> None:
    data = _policy()
    target: Any = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(NetworkPolicyParseError) as caught:
        parse_network_policy(data)

    assert caught.value.issues
    assert all(issue.startswith(("allow", "subnets")) for issue in caught.value.issues)
    assert "ZZSECRETZZ" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize(
    "data",
    [
        None,
        True,
        42,
        [],
        "ZZSECRETZZ",
        '{"allow": []}',
        {"mode": "allow-all"},
        {"mode": "custom", "allowedDomains": []},
        NetworkPolicy.deny_all(),
    ],
)
def test_parse_network_policy_requires_decoded_sdk_format(data: object) -> None:
    with pytest.raises(NetworkPolicyParseError) as caught:
        parse_network_policy(data)
    assert "ZZSECRETZZ" not in str(caught.value)


def test_parse_network_policy_collects_precise_wire_paths() -> None:
    data = _policy()
    data["allow"]["registry.npmjs.org"][0]["match"]["methods"] = ["GET"]
    data["subnet"] = {"deny": []}
    with pytest.raises(NetworkPolicyParseError) as caught:
        parse_network_policy(data)
    assert caught.value.issues == (
        'allow["registry.npmjs.org"][0].match.methods: unknown key',
        "subnet: unknown key",
    )


@pytest.mark.parametrize(
    "rule",
    [
        {"response": {"statusCode": 403.0}},
        {"transform": []},
        {"match": {}, "transform": [{}]},
        {"match": {"method": [], "headers": [], "queryString": []}, "transform": []},
        {"match": {"queryString": [{}], "headers": [{}]}, "response": {"statusCode": 403}},
    ],
)
def test_parse_network_policy_preserves_explicit_empty_fields(rule: dict[str, Any]) -> None:
    data = {"allow": {"example.com": [rule]}}
    policy = parse_network_policy(data)
    assert _serialize_network_policy(policy) == data
    assert NetworkPolicy.model_validate_json(policy.model_dump_json()) == policy


@given(st.dictionaries(st.text(min_size=1), st.text()))
def test_parse_network_policy_copies_headers_and_preserves_wire_values(
    headers: dict[str, str],
) -> None:
    data = _policy()
    data["allow"]["registry.npmjs.org"][0]["transform"][0]["headers"] = headers
    expected = deepcopy(data)
    policy = parse_network_policy(data)
    headers.clear()
    data.clear()
    assert _serialize_network_policy(policy) == expected
    assert NetworkPolicy.model_validate_json(policy.model_dump_json()) == policy
