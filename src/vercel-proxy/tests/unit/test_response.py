import json

import pytest

from vercel.proxy import Response

# ---------------------------------------------------------------------------
# next()
# ---------------------------------------------------------------------------


def test_next_kind() -> None:
    assert Response.next().kind == "continuing"


def test_next_status_200() -> None:
    assert Response.next().status == 200


def test_next_empty_body() -> None:
    assert Response.next().body == b""


def test_next_no_headers_by_default() -> None:
    assert Response.next().headers is None


def test_next_headers_stored_as_given() -> None:
    r = Response.next(headers={"x-foo": "bar"})
    assert r.headers == {"x-foo": "bar"}


def test_next_multiple_headers() -> None:
    r = Response.next(headers={"x-a": "1", "x-b": "2"})
    assert r.headers == {"x-a": "1", "x-b": "2"}


def test_next_no_destination() -> None:
    assert Response.next().destination is None


# ---------------------------------------------------------------------------
# rewrite()
# ---------------------------------------------------------------------------


def test_rewrite_kind() -> None:
    assert Response.rewrite("/hello").kind == "continuing"


def test_rewrite_destination() -> None:
    assert Response.rewrite("/hello").destination == "/hello"


def test_rewrite_status_200() -> None:
    assert Response.rewrite("/hello").status == 200


def test_rewrite_empty_body() -> None:
    assert Response.rewrite("/hello").body == b""


def test_rewrite_no_headers_by_default() -> None:
    assert Response.rewrite("/hello").headers is None


def test_rewrite_headers_stored_as_given() -> None:
    r = Response.rewrite("/hello", headers={"x-tenant": "acme"})
    assert r.headers == {"x-tenant": "acme"}


# ---------------------------------------------------------------------------
# redirect()
# ---------------------------------------------------------------------------


def test_redirect_kind() -> None:
    assert Response.redirect("/login").kind == "terminating"


def test_redirect_destination() -> None:
    assert Response.redirect("/login").destination == "/login"


def test_redirect_default_status_307() -> None:
    assert Response.redirect("/login").status == 307


def test_redirect_custom_status() -> None:
    assert Response.redirect("/login", status=308).status == 308


def test_redirect_empty_body() -> None:
    assert Response.redirect("/login").body == b""


def test_redirect_no_headers_normalised_to_empty() -> None:
    assert Response.redirect("/login").headers == {}


def test_redirect_headers_returned_to_client() -> None:
    r = Response.redirect("/login", headers={"set-cookie": "s=1"})
    assert r.headers is not None
    assert r.headers.get("set-cookie") == "s=1"


# ---------------------------------------------------------------------------
# json()
# ---------------------------------------------------------------------------


def test_json_kind() -> None:
    assert Response.json({}).kind == "terminating"


def test_json_default_status_200() -> None:
    assert Response.json({}).status == 200


def test_json_custom_status() -> None:
    assert Response.json({}, status=403).status == 403


def test_json_serialises_body() -> None:
    r = Response.json({"key": "value"})
    assert json.loads(r.body) == {"key": "value"}


def test_json_list() -> None:
    assert json.loads(Response.json([1, 2, 3]).body) == [1, 2, 3]


def test_json_sets_content_type() -> None:
    h = Response.json({}).headers
    assert h is not None
    assert h.get("content-type") == "application/json"


def test_json_extra_headers_merged() -> None:
    r = Response.json({}, headers={"x-custom": "1"})
    assert r.headers is not None
    assert r.headers.get("content-type") == "application/json"
    assert r.headers.get("x-custom") == "1"


def test_json_content_type_not_overridable() -> None:
    r = Response.json({}, headers={"content-type": "text/plain"})
    assert r.headers is not None
    assert r.headers.get("content-type") == "application/json"


# ---------------------------------------------------------------------------
# respond()
# ---------------------------------------------------------------------------


def test_respond_kind() -> None:
    assert Response.respond(status=200).kind == "terminating"


def test_respond_status() -> None:
    assert Response.respond(status=418).status == 418


def test_respond_body() -> None:
    assert Response.respond(status=200, body=b"ok").body == b"ok"


def test_respond_headers() -> None:
    r = Response.respond(status=200, headers={"x-custom": "1"})
    assert r.headers is not None
    assert r.headers.get("x-custom") == "1"


def test_respond_empty_defaults() -> None:
    r = Response.respond(status=200)
    assert r.body == b""
    assert r.headers == {}


def test_respond_no_destination() -> None:
    assert Response.respond(status=200).destination is None


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_setattr_raises() -> None:
    with pytest.raises(AttributeError):
        Response.next()._status = 500  # type: ignore[misc]


def test_delattr_raises() -> None:
    with pytest.raises(AttributeError):
        del Response.next()._status  # type: ignore[misc]


def test_headers_view_is_immutable() -> None:
    r = Response.next(headers={"x-foo": "bar"})
    assert r.headers is not None
    with pytest.raises(TypeError):
        r.headers["x-foo"] = "baz"  # type: ignore[index]
