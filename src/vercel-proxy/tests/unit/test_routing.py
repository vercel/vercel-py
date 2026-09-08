import pytest

from vercel.proxy._routing import compile_host, compile_path

# ---------------------------------------------------------------------------
# Exact match
# ---------------------------------------------------------------------------


def test_exact_match() -> None:
    p = compile_path("/health")
    assert p.match("/health") == {}


def test_exact_no_match() -> None:
    assert compile_path("/health").match("/other") is None


def test_exact_prefix_not_matched() -> None:
    assert compile_path("/health").match("/health/check") is None


# ---------------------------------------------------------------------------
# Single-segment capture {param}
# ---------------------------------------------------------------------------


def test_single_segment_capture() -> None:
    p = compile_path("/users/{id}")
    assert p.match("/users/42") == {"id": "42"}


def test_single_segment_no_slashes() -> None:
    assert compile_path("/users/{id}").match("/users/a/b") is None


def test_multiple_captures() -> None:
    p = compile_path("/users/{user_id}/posts/{post_id}")
    assert p.match("/users/1/posts/99") == {"user_id": "1", "post_id": "99"}


def test_capture_no_match_empty_segment() -> None:
    assert compile_path("/users/{id}").match("/users/") is None


# ---------------------------------------------------------------------------
# Multi-segment capture {param:path}
# ---------------------------------------------------------------------------


def test_path_capture_single_segment() -> None:
    p = compile_path("/files/{path:path}")
    assert p.match("/files/readme.txt") == {"path": "readme.txt"}


def test_path_capture_multiple_segments() -> None:
    p = compile_path("/files/{path:path}")
    assert p.match("/files/a/b/c") == {"path": "a/b/c"}


def test_path_capture_is_greedy() -> None:
    p = compile_path("/files/{path:path}")
    assert p.match("/files/deep/nested/file.txt") == {"path": "deep/nested/file.txt"}


def test_path_capture_can_match_empty() -> None:
    p = compile_path("/files/{path:path}")
    assert p.match("/files/") == {"path": ""}


def test_path_capture_mid_pattern() -> None:
    p = compile_path("/a/{x:path}/b")
    assert p.match("/a/foo/b") == {"x": "foo"}


def test_path_capture_mid_pattern_multiple_segments() -> None:
    p = compile_path("/a/{x:path}/b")
    assert p.match("/a/foo/bar/baz/b") == {"x": "foo/bar/baz"}


def test_path_capture_mid_pattern_empty() -> None:
    p = compile_path("/a/{x:path}/b")
    assert p.match("/a//b") == {"x": ""}


def test_path_capture_mid_pattern_no_match() -> None:
    assert compile_path("/a/{x:path}/b").match("/a/foo") is None


def test_path_capture_mid_pattern_no_match_wrong_suffix() -> None:
    assert compile_path("/a/{x:path}/b").match("/a/foo/c") is None


def test_path_capture_mid_pattern_with_other_param() -> None:
    p = compile_path("/api/{version}/{path:path}/meta")
    assert p.match("/api/v1/users/42/meta") == {"version": "v1", "path": "users/42"}


def test_two_path_converters_with_literal_separator() -> None:
    p = compile_path("/a/{x:path}/b/{y:path}/c")
    assert p.match("/a/foo/b/bar/c") == {"x": "foo", "y": "bar"}
    assert p.match("/a/foo/baz/b/bar/qux/c") == {"x": "foo/baz", "y": "bar/qux"}


def test_two_adjacent_path_converters_allowed() -> None:
    p = compile_path("/a/{x:path}/{y:path}/b")
    assert p.match("/a/foo/bar/b") == {"x": "foo", "y": "bar"}
    assert p.match("/a/foo/b/bar/b") == {"x": "foo/b", "y": "bar"}


def test_path_capture_mid_pattern_trailing_slash() -> None:
    p = compile_path("/a/{x:path}/b")
    assert p.match("/a/foo/b/") == {"x": "foo"}


# ---------------------------------------------------------------------------
# Trailing slash (default: transparent)
# ---------------------------------------------------------------------------


def test_trailing_slash_matches_without() -> None:
    assert compile_path("/health").match("/health") == {}


def test_trailing_slash_matches_with() -> None:
    assert compile_path("/health").match("/health/") == {}


def test_trailing_slash_param_without() -> None:
    assert compile_path("/users/{id}").match("/users/42") == {"id": "42"}


def test_trailing_slash_param_with() -> None:
    assert compile_path("/users/{id}").match("/users/42/") == {"id": "42"}


# ---------------------------------------------------------------------------
# strict=True
# ---------------------------------------------------------------------------


def test_strict_no_trailing_slash() -> None:
    assert compile_path("/health", strict=True).match("/health") == {}


def test_strict_rejects_trailing_slash() -> None:
    assert compile_path("/health", strict=True).match("/health/") is None


def test_strict_param_rejects_trailing_slash() -> None:
    assert compile_path("/users/{id}", strict=True).match("/users/42/") is None


# ---------------------------------------------------------------------------
# Special characters in literals
# ---------------------------------------------------------------------------


def test_dot_in_literal_escaped() -> None:
    p = compile_path("/files/readme.txt")
    assert p.match("/files/readme.txt") == {}
    assert p.match("/files/readmeXtxt") is None


def test_plus_in_literal_escaped() -> None:
    p = compile_path("/v1+beta/health")
    assert p.match("/v1+beta/health") == {}
    assert p.match("/v1beta/health") is None


# ---------------------------------------------------------------------------
# Explicit str converter
# ---------------------------------------------------------------------------


def test_explicit_str_converter() -> None:
    assert compile_path("/users/{id:str}").match("/users/42") == {"id": "42"}


def test_explicit_str_no_slashes() -> None:
    assert compile_path("/users/{id:str}").match("/users/a/b") is None


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_unclosed_brace_raises() -> None:
    with pytest.raises(ValueError, match="unclosed"):
        compile_path("/users/{id")


def test_unknown_converter_raises() -> None:
    with pytest.raises(ValueError, match="unknown converter"):
        compile_path("/users/{id:int}")


def test_duplicate_param_raises() -> None:
    with pytest.raises(ValueError, match="duplicate param"):
        compile_path("/{x}/{x}")


def test_invalid_param_name_raises() -> None:
    with pytest.raises(ValueError, match="invalid param name"):
        compile_path("/{123}")


# ---------------------------------------------------------------------------
# compile_host
# ---------------------------------------------------------------------------


def test_host_exact_match() -> None:
    p = compile_host("myapp.com")
    assert p.match("myapp.com") == {}


def test_host_exact_no_match() -> None:
    p = compile_host("myapp.com")
    assert p.match("other.com") is None


def test_host_param_capture() -> None:
    p = compile_host("{tenant}.myapp.com")
    assert p.match("acme.myapp.com") == {"tenant": "acme"}


def test_host_param_no_match_wrong_base() -> None:
    p = compile_host("{tenant}.myapp.com")
    assert p.match("acme.other.com") is None


def test_host_param_no_dots_in_label() -> None:
    p = compile_host("{tenant}.myapp.com")
    assert p.match("a.b.myapp.com") is None


def test_host_multiple_params() -> None:
    p = compile_host("{env}.{tenant}.myapp.com")
    assert p.match("prod.acme.myapp.com") == {"env": "prod", "tenant": "acme"}


def test_host_path_converter_unsupported() -> None:
    with pytest.raises(ValueError, match="unknown converter"):
        compile_host("{path:path}.myapp.com")


def test_host_duplicate_param_raises() -> None:
    with pytest.raises(ValueError, match="duplicate param name"):
        compile_host("{x}.{x}.myapp.com")
