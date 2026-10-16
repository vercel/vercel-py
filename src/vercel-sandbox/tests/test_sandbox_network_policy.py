"""Authoring network policies and reading back what the API keeps of them."""

from typing import Literal

import httpx2 as httpx
import pytest
from fake_sandbox import FakeSandboxApi, wire

from vercel import sandbox
from vercel.sandbox import (
    NetworkPolicy,
    NetworkPolicyForwardRuleSummary,
    NetworkPolicyInjectionRuleSummary,
    NetworkPolicyKeyValueMatcher,
    NetworkPolicyMatcher,
    NetworkPolicyReadback,
    NetworkPolicyRequestMatcher,
    NetworkPolicyResponse,
    NetworkPolicyResponseRuleSummary,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkPolicyTransform,
    SandboxResponseError,
    sync as sandbox_sync,
)

_ALL_DIMENSIONS: tuple[Literal["path", "method", "query", "headers"], ...] = (
    "path",
    "method",
    "query",
    "headers",
)
_MATCH_WIRE = {
    "path": {"startsWith": "/v1/"},
    "method": ["POST"],
    "queryString": [{"key": {"exact": "stream"}, "value": {"regex": "^(true|false)$"}}],
    "headers": [{"key": {"exact": "authorization"}, "value": {"startsWith": "Bearer "}}],
}
_AUTHORED_WIRE = {
    "allow": {
        "example.com": [],
        "api.example.com": [
            {
                "match": _MATCH_WIRE,
                "transform": [
                    {"headers": {"Authorization": "Bearer secret", "X-Trace": "one"}},
                    {"headers": {"X-Trace": "two"}},
                ],
            },
            {"match": _MATCH_WIRE, "forwardURL": "https://forward-proxy.internal/ingress/"},
            {"transform": [{"headers": {"X-Fallback": "fallback"}}]},
            {
                "match": _MATCH_WIRE,
                "response": {
                    "statusCode": 403,
                    "headers": {"X-Policy": "blocked"},
                    "body": '{"error":"forbidden"}',
                    "contentType": "application/json",
                },
            },
        ],
    },
    "subnets": {"allow": ["10.0.0.0/8"], "deny": ["10.1.0.0/16"]},
}


def _authored_policy() -> NetworkPolicy:
    matcher = NetworkPolicyRequestMatcher(
        path=NetworkPolicyMatcher.starts_with("/v1/"),
        method=["POST"],
        query=[
            NetworkPolicyKeyValueMatcher(
                key=NetworkPolicyMatcher.exact("stream"),
                value=NetworkPolicyMatcher.regex("^(true|false)$"),
            )
        ],
        headers=[
            NetworkPolicyKeyValueMatcher(
                key=NetworkPolicyMatcher.exact("authorization"),
                value=NetworkPolicyMatcher.starts_with("Bearer "),
            )
        ],
    )
    return NetworkPolicy.custom(
        allow={
            "example.com": (),
            "api.example.com": [
                NetworkPolicyRule(
                    match=matcher,
                    transform=[
                        NetworkPolicyTransform(
                            headers={"Authorization": "Bearer secret", "X-Trace": "one"}
                        ),
                        NetworkPolicyTransform(headers={"X-Trace": "two"}),
                    ],
                ),
                NetworkPolicyRule(
                    match=matcher, forward_url="https://forward-proxy.internal/ingress/"
                ),
                NetworkPolicyRule(
                    transform=[NetworkPolicyTransform(headers={"X-Fallback": "fallback"})]
                ),
                NetworkPolicyRule(
                    match=matcher,
                    response=NetworkPolicyResponse(
                        status_code=403,
                        headers={"X-Policy": "blocked"},
                        body='{"error":"forbidden"}',
                        content_type="application/json",
                    ),
                ),
            ],
        },
        subnets=NetworkPolicySubnets(allow=["10.0.0.0/8"], deny=["10.1.0.0/16"]),
    )


_SANDBOX_READBACK = NetworkPolicyReadback(
    mode="custom",
    allowed_domains=("example.com", "api.example.com"),
    subnets=NetworkPolicySubnets(allow=["10.0.0.0/8"], deny=["10.1.0.0/16"]),
)
_SESSION_READBACK = NetworkPolicyReadback(
    mode="custom",
    allowed_domains=("example.com", "api.example.com"),
    subnets=NetworkPolicySubnets(allow=["10.0.0.0/8"], deny=["10.1.0.0/16"]),
    injection_rules=(
        NetworkPolicyInjectionRuleSummary(
            domain="api.example.com",
            header_names=("Authorization", "X-Trace"),
            match_dimensions=_ALL_DIMENSIONS,
        ),
        NetworkPolicyInjectionRuleSummary(domain="api.example.com", header_names=("X-Fallback",)),
    ),
    forward_rules=(
        NetworkPolicyForwardRuleSummary(
            domain="api.example.com",
            forward_url="https://forward-proxy.internal/ingress/",
            match_dimensions=_ALL_DIMENSIONS,
        ),
    ),
    response_rules=(
        NetworkPolicyResponseRuleSummary(
            domain="api.example.com", status_code=403, match_dimensions=_ALL_DIMENSIONS
        ),
    ),
)


async def test_policies_are_authored_whole_and_read_back_redacted(
    fake_api: FakeSandboxApi,
) -> None:
    async with fake_api.session():
        handle = await sandbox.create_sandbox(
            name="preview", network_policy=NetworkPolicy.allow_all()
        )
        assert handle.network_policy == NetworkPolicyReadback(mode="allow-all")
        assert handle.current_session is not None
        assert handle.current_session.network_policy == NetworkPolicyReadback(mode="allow-all")

        updated = await handle.update_network_policy(_authored_policy())
        assert updated.network_policy == _SESSION_READBACK
        inspected = await sandbox.get_sandbox(name="preview")
        assert inspected.network_policy == _SANDBOX_READBACK
        assert inspected.current_session is not None
        assert inspected.current_session.network_policy == _SESSION_READBACK

        requests_before = len(fake_api.requests)
        with pytest.raises(TypeError, match="must be a NetworkPolicy"):
            await inspected.update_network_policy(inspected.network_policy)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="must be a NetworkPolicy"):
            await inspected.update_network_policy(
                inspected.current_session.network_policy  # type: ignore[arg-type]
            )
        with pytest.raises(TypeError, match="must be a NetworkPolicy"):
            await inspected.update(network_policy=inspected.network_policy)  # type: ignore[arg-type]
        assert len(fake_api.requests) == requests_before

        await handle.update(network_policy=_authored_policy())
        assert handle.network_policy == _SANDBOX_READBACK

        updated_session = await handle.update_network_policy(NetworkPolicy.deny_all())
        assert updated_session.network_policy == NetworkPolicyReadback(mode="deny-all")
        assert handle.current_session is updated_session

    assert fake_api.requests_to("sandboxes.create")[-1].json()["networkPolicy"] == {
        "mode": "allow-all"
    }
    first, last = fake_api.requests_to("sessions.network_policy")
    assert first.json() == _AUTHORED_WIRE
    assert last.json() == {"mode": "deny-all"}
    assert fake_api.requests_to("sandboxes.update")[-1].json()["networkPolicy"] == _AUTHORED_WIRE


def test_sync_readback_is_not_writable(fake_api: FakeSandboxApi) -> None:
    with fake_api.session(sync=True):
        handle = sandbox_sync.create_sandbox(name="preview", network_policy=_authored_policy())
        assert handle.network_policy == _SANDBOX_READBACK
        assert handle.current_session is not None
        assert handle.current_session.network_policy == _SESSION_READBACK
        with pytest.raises(TypeError, match="must be a NetworkPolicy"):
            handle.update_network_policy(handle.network_policy)  # type: ignore[arg-type]

    assert sandbox_sync.NetworkPolicy is NetworkPolicy
    assert sandbox_sync.NetworkPolicyReadback is NetworkPolicyReadback
    assert sandbox_sync.NetworkPolicyInjectionRuleSummary is NetworkPolicyInjectionRuleSummary
    assert sandbox_sync.NetworkPolicyForwardRuleSummary is NetworkPolicyForwardRuleSummary
    assert sandbox_sync.NetworkPolicyResponseRuleSummary is NetworkPolicyResponseRuleSummary
    assert not fake_api.requests_to("sessions.network_policy")
    assert fake_api.requests_to("sandboxes.create")[-1].json()["networkPolicy"] == _AUTHORED_WIRE


def test_policy_values_validate_their_structure() -> None:
    with pytest.raises(ValueError, match="requires a key or value"):
        NetworkPolicyKeyValueMatcher()
    with pytest.raises(ValueError, match="at least one matching dimension"):
        NetworkPolicyRequestMatcher()
    with pytest.raises(ValueError, match="exactly one"):
        NetworkPolicyRule()
    with pytest.raises(ValueError, match="exactly one"):
        NetworkPolicyRule(
            transform=[NetworkPolicyTransform(headers={"X-Test": "value"})],
            response=NetworkPolicyResponse(status_code=403),
        )
    with pytest.raises(ValueError, match="between 200 and 599"):
        NetworkPolicyResponse(status_code=199)
    with pytest.raises(ValueError, match="content_type is required"):
        NetworkPolicyResponse(status_code=403, body="blocked")
    with pytest.raises(ValueError, match="body is not allowed"):
        NetworkPolicyResponse(status_code=204, body="blocked", content_type="text/plain")
    with pytest.raises(ValueError, match="proxy-managed headers"):
        NetworkPolicyResponse(status_code=403, headers={"Content-Length": "0"})
    with pytest.raises(TypeError, match="headers must map strings to strings"):
        NetworkPolicyResponse(status_code=403, headers={"X-Count": 1})  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="body must be a string"):
        NetworkPolicyResponse(status_code=403, body=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="content_type must be a string"):
        NetworkPolicyResponse(status_code=403, content_type=1)  # type: ignore[arg-type]

    headers = {"X-Secret": "value"}
    rules = [NetworkPolicyRule(transform=[NetworkPolicyTransform(headers=headers)])]
    allow = {"example.com": rules}
    copied = NetworkPolicy.custom(allow=allow)
    headers["X-Secret"] = "changed"
    rules.clear()
    allow.clear()
    assert copied.allow["example.com"][0].transform[0].headers == {"X-Secret": "value"}
    with pytest.raises(TypeError):
        hash(copied)
    with pytest.raises(TypeError):
        copied.allow["other.example.com"] = ()  # type: ignore[index]


@pytest.mark.parametrize(
    "session_policy",
    [
        {
            "mode": "custom",
            "injectionRules": [{"domain": "example.com", "headerNames": "X-Secret"}],
        },
        {"mode": "custom", "responseRules": [{"domain": "example.com", "statusCode": "403"}]},
    ],
)
async def test_malformed_policy_readback_is_rejected(
    fake_api: FakeSandboxApi, session_policy: dict[str, object]
) -> None:
    fake_api.respond(
        "sandboxes.get",
        httpx.Response(
            200,
            json=wire.sandbox_response(session=wire.session_payload(network_policy=session_policy)),
        ),
    )

    async with fake_api.session():
        with pytest.raises(SandboxResponseError, match="malformed network policy"):
            await sandbox.get_sandbox(name="preview")


async def test_policies_must_be_authored_values(fake_api: FakeSandboxApi) -> None:
    fake_api.seed()
    readback = NetworkPolicyReadback(
        mode="custom",
        allowed_domains=("example.com",),
        response_rules=(
            NetworkPolicyResponseRuleSummary(
                domain="example.com", status_code=403, match_dimensions=("path",)
            ),
        ),
    )

    async with fake_api.session():
        with pytest.raises(TypeError, match="must be a NetworkPolicy"):
            await sandbox.create_sandbox(
                name="other",
                network_policy={"mode": "allow-all"},  # type: ignore[arg-type]
            )
        handle = await sandbox.get_sandbox(name="preview")
        with pytest.raises(TypeError, match="must be a NetworkPolicy"):
            await handle.update_network_policy(readback)  # type: ignore[arg-type]

    assert fake_api.routes() == ["sandboxes.get"]
