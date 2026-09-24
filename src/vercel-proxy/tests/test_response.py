"""Tests for vercel.proxy.Response."""

import json
import types
from typing import Any, cast

import pytest

from vercel.proxy import Kind, Response

# ---------------------------------------------------------------------------
# Response.next()
# ---------------------------------------------------------------------------


def test_next_kind() -> None:
    assert Response.next().kind == Kind.CONTINUING


def test_next_destination_none() -> None:
    assert Response.next().destination is None


def test_next_status_200() -> None:
    assert Response.next().status == 200


def test_next_body_empty() -> None:
    assert Response.next().body == b""


def test_next_headers_empty_by_default() -> None:
    assert dict(Response.next().headers) == {}


def test_next_headers_with_value() -> None:
    res = Response.next(headers={"x-tenant": "acme"})
    assert dict(res.headers) == {"x-tenant": "acme"}


def test_next_headers_none_value_signals_drop() -> None:
    res = Response.next(headers={"authorization": None})
    assert dict(res.headers) == {"authorization": None}


def test_next_headers_returns_mapping_proxy() -> None:
    res = Response.next(headers={"k": "v"})
    assert isinstance(res.headers, types.MappingProxyType)


def test_next_headers_mapping_proxy_is_immutable() -> None:
    res = Response.next(headers={"k": "v"})
    with pytest.raises(TypeError):
        cast(Any, res.headers)["k"] = "new"


# ---------------------------------------------------------------------------
# Response.rewrite()
# ---------------------------------------------------------------------------


def test_rewrite_kind() -> None:
    assert Response.rewrite("/api").kind == Kind.CONTINUING


def test_rewrite_destination() -> None:
    assert Response.rewrite("/api/v2").destination == "/api/v2"


def test_rewrite_absolute_destination() -> None:
    dest = "https://internal.example.com/api"
    assert Response.rewrite(dest).destination == dest


def test_rewrite_status_200() -> None:
    assert Response.rewrite("/x").status == 200


def test_rewrite_body_empty() -> None:
    assert Response.rewrite("/x").body == b""


def test_rewrite_headers_empty_by_default() -> None:
    assert dict(Response.rewrite("/x").headers) == {}


def test_rewrite_headers_set() -> None:
    res = Response.rewrite("/x", headers={"x-tenant": "acme"})
    assert dict(res.headers) == {"x-tenant": "acme"}


def test_rewrite_headers_none_value_signals_drop() -> None:
    res = Response.rewrite("/x", headers={"authorization": None})
    assert dict(res.headers) == {"authorization": None}


# ---------------------------------------------------------------------------
# Response.redirect()
# ---------------------------------------------------------------------------


def test_redirect_kind() -> None:
    assert Response.redirect("/login").kind == Kind.TERMINATING


def test_redirect_destination() -> None:
    assert Response.redirect("/login").destination == "/login"


def test_redirect_default_status_307() -> None:
    assert Response.redirect("/login").status == 307


def test_redirect_custom_status_301() -> None:
    assert Response.redirect("/login", status=301).status == 301


def test_redirect_boundary_300() -> None:
    assert Response.redirect("/x", status=300).status == 300


def test_redirect_boundary_399() -> None:
    assert Response.redirect("/x", status=399).status == 399


def test_redirect_status_200_raises() -> None:
    with pytest.raises(ValueError, match="3xx"):
        Response.redirect("/login", status=200)


def test_redirect_status_400_raises() -> None:
    with pytest.raises(ValueError, match="3xx"):
        Response.redirect("/login", status=400)


def test_redirect_body_empty() -> None:
    assert Response.redirect("/login").body == b""


def test_redirect_headers_empty_by_default() -> None:
    assert dict(Response.redirect("/login").headers) == {}


def test_redirect_headers_set() -> None:
    res = Response.redirect("/login", headers={"set-cookie": "s=; Max-Age=0"})
    assert dict(res.headers) == {"set-cookie": "s=; Max-Age=0"}


# ---------------------------------------------------------------------------
# Response.json()
# ---------------------------------------------------------------------------


def test_json_kind() -> None:
    assert Response.json({}).kind == Kind.TERMINATING


def test_json_destination_none() -> None:
    assert Response.json({}).destination is None


def test_json_default_status_200() -> None:
    assert Response.json({}).status == 200


def test_json_custom_status() -> None:
    assert Response.json({}, status=201).status == 201


def test_json_body_encoded() -> None:
    assert json.loads(Response.json({"k": "v"}).body) == {"k": "v"}


def test_json_body_nested() -> None:
    data = {"users": [{"id": 1}]}
    assert json.loads(Response.json(data).body) == data


def test_json_content_type_set() -> None:
    assert Response.json({}).headers["content-type"] == "application/json"


def test_json_content_type_overrides_caller_value() -> None:
    res = Response.json({}, headers={"content-type": "text/plain"})
    assert res.headers["content-type"] == "application/json"


def test_json_extra_headers_preserved() -> None:
    res = Response.json({}, headers={"x-trace": "abc"})
    assert dict(res.headers) == {"x-trace": "abc", "content-type": "application/json"}


# ---------------------------------------------------------------------------
# Response.respond()
# ---------------------------------------------------------------------------


def test_respond_kind() -> None:
    assert Response.respond(status=200).kind == Kind.TERMINATING


def test_respond_destination_none() -> None:
    assert Response.respond(status=200).destination is None


def test_respond_status() -> None:
    assert Response.respond(status=204).status == 204


def test_respond_default_body_empty() -> None:
    assert Response.respond(status=204).body == b""


def test_respond_body_set() -> None:
    assert Response.respond(status=200, body=b"hello").body == b"hello"


def test_respond_headers_empty_by_default() -> None:
    assert dict(Response.respond(status=204).headers) == {}


def test_respond_headers_set() -> None:
    res = Response.respond(status=200, headers={"x-foo": "bar"})
    assert dict(res.headers) == {"x-foo": "bar"}


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_immutability_setattr_raises() -> None:
    res = Response.next()
    with pytest.raises(AttributeError, match="immutable"):
        cast(Any, res)._kind = Kind.TERMINATING


def test_immutability_delattr_raises() -> None:
    res = Response.next()
    with pytest.raises(AttributeError, match="immutable"):
        del cast(Any, res)._kind


def test_immutability_public_property_raises() -> None:
    res = Response.next()
    with pytest.raises(AttributeError, match="immutable"):
        cast(Any, res).status = 404


# ---------------------------------------------------------------------------
# Kind
# ---------------------------------------------------------------------------


def test_kind_continuing_value() -> None:
    assert Kind.CONTINUING.value == "continuing"


def test_kind_terminating_value() -> None:
    assert Kind.TERMINATING.value == "terminating"


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


def test_repr_next() -> None:
    assert repr(Response.next()) == "Response(kind=<Kind.CONTINUING: 'continuing'>)"


def test_repr_next_with_headers() -> None:
    assert repr(Response.next(headers={"x-a": None})) == (
        "Response(kind=<Kind.CONTINUING: 'continuing'>, headers={'x-a': None})"
    )


def test_repr_rewrite() -> None:
    assert repr(Response.rewrite("/api")) == (
        "Response(kind=<Kind.CONTINUING: 'continuing'>, destination='/api')"
    )


def test_repr_redirect() -> None:
    assert repr(Response.redirect("/x", status=301)) == (
        "Response(kind=<Kind.TERMINATING: 'terminating'>, destination='/x', status=301)"
    )


def test_repr_json() -> None:
    assert repr(Response.json({"k": "v"})) == (
        "Response(kind=<Kind.TERMINATING: 'terminating'>, body=b'{\"k\": \"v\"}', "
        "headers={'content-type': 'application/json'})"
    )


def test_repr_respond() -> None:
    assert repr(Response.respond(status=204, body=b"hi")) == (
        "Response(kind=<Kind.TERMINATING: 'terminating'>, status=204, body=b'hi')"
    )
