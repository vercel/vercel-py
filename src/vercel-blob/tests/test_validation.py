"""Pathname identity and validation properties at the HTTP boundary."""

from __future__ import annotations

from urllib.parse import quote, unquote, urlsplit

import httpx2 as httpx
import pytest
from hypothesis import example, given, strategies as st

from vercel.blob._internal.api_client import _PutRequest
from vercel.blob._internal.validation import (
    construct_delivery_url,
    parse_and_validate_delivery_url,
    validate_pathname,
)
from vercel.blob.errors import BlobError

_HOST = "teststore.public.blob.vercel-storage.com"
_CHARACTERS = st.characters(
    exclude_categories=["Cs"],
    exclude_characters="/" + "".join(chr(i) for i in range(32)) + "\x7f",
)
_SEGMENT = st.text(_CHARACTERS, max_size=24).map(lambda value: "name-" + value)
_PATHNAMES = st.lists(_SEGMENT, min_size=1, max_size=5).map("/".join)


@given(pathname=st.text(max_size=100))
@example(pathname="//file.txt")
@example(pathname="folder//file.txt")
@example(pathname="file\ud800.txt")
def test_accepted_pathnames_are_serializable_and_idempotent(pathname: str) -> None:
    try:
        normalized = validate_pathname(pathname)
    except BlobError:
        return

    assert normalized == pathname.removeprefix("/")
    assert validate_pathname(normalized) == normalized
    assert normalized and not normalized.startswith("/")
    assert "//" not in normalized
    assert all(segment not in (".", "..") for segment in normalized.split("/"))
    normalized.encode("utf-8")

    request = httpx.Request("GET", construct_delivery_url("teststore", normalized, "public"))
    parsed = urlsplit(str(request.url))
    assert parsed.scheme == "https"
    assert parsed.netloc == _HOST
    assert parsed.query == parsed.fragment == ""
    assert unquote(request.url.raw_path.decode("ascii"), errors="strict") == "/" + normalized


@given(pathname=_PATHNAMES, leading_slash=st.booleans())
@example(pathname="folder/report?#.txt", leading_slash=True)
@example(pathname="folder/%2e%2e/%2f.txt", leading_slash=False)
@example(pathname="folder/space and \\ quote'\";$.txt", leading_slash=False)
@example(pathname="folder/é😀.txt", leading_slash=False)
def test_pathname_identity_survives_put_and_delivery_encoding(
    pathname: str, leading_slash: bool
) -> None:
    original = "/" + pathname if leading_slash else pathname
    put = _PutRequest(pathname=original, body=b"", access="public")
    assert put.pathname == pathname
    assert put.add_random_suffix is False

    upload = httpx.Request("PUT", "https://vercel.com/api/blob", params={"pathname": put.pathname})
    assert list(upload.url.params.multi_items()) == [("pathname", pathname)]

    delivery = construct_delivery_url("teststore", pathname, "public")
    request = httpx.Request("GET", delivery)
    assert request.url.host == _HOST
    assert request.url.query == b""
    assert request.url.fragment == ""
    assert unquote(request.url.raw_path.decode("ascii"), errors="strict") == "/" + pathname
    clean_url, store, access, encoded_path = parse_and_validate_delivery_url(delivery)
    assert clean_url == delivery
    assert (store, access) == ("teststore", "public")
    assert unquote(encoded_path, errors="strict") == pathname


@given(left=_PATHNAMES, right=_PATHNAMES)
def test_distinct_pathnames_remain_distinct_delivery_urls(left: str, right: str) -> None:
    left_url = httpx.Request("GET", construct_delivery_url("teststore", left, "public")).url
    right_url = httpx.Request("GET", construct_delivery_url("teststore", right, "public")).url
    assert (left_url.raw_path == right_url.raw_path) == (left == right)


@given(left=_SEGMENT, right=_SEGMENT, dot=st.sampled_from([".", ".."]))
def test_dot_segments_rejected_in_pathnames_and_encoded_delivery_urls(
    left: str, right: str, dot: str
) -> None:
    pathname = f"{left}/{dot}/{right}"
    with pytest.raises(BlobError, match="dot segments"):
        validate_pathname(pathname)

    encoded_dot = "%2e" * len(dot)
    url = f"https://{_HOST}/{quote(left, safe='')}/{encoded_dot}/{quote(right, safe='')}"
    with pytest.raises(BlobError, match="dot segments"):
        parse_and_validate_delivery_url(url)


@given(left=_SEGMENT, right=_SEGMENT)
def test_doubled_separators_rejected_before_normalization(left: str, right: str) -> None:
    for pathname in (f"{left}//{right}", f"//{left}"):
        with pytest.raises(BlobError, match="cannot contain.*//"):
            validate_pathname(pathname)


@given(segment=_SEGMENT, codepoint=st.integers(min_value=0xD800, max_value=0xDFFF))
def test_surrogates_rejected_before_encoding(segment: str, codepoint: int) -> None:
    with pytest.raises(BlobError, match="Unicode"):
        validate_pathname(segment + chr(codepoint))


@given(segment=_SEGMENT, control=st.sampled_from([*map(chr, range(32)), "\x7f"]))
def test_controls_rejected_in_pathnames_and_encoded_delivery_urls(
    segment: str, control: str
) -> None:
    pathname = segment + control
    with pytest.raises(BlobError, match="control characters"):
        validate_pathname(pathname)
    with pytest.raises(BlobError, match="control characters"):
        parse_and_validate_delivery_url(f"https://{_HOST}/{quote(pathname, safe='')}")


@pytest.mark.parametrize(
    "pathname",
    ["x" * 950, "😀" * 475, "/" + "x" * 949, "x" * 948 + "😀"],
    ids=["ascii", "astral", "leading-slash", "mixed"],
)
def test_put_accepts_950_utf16_units(pathname: str) -> None:
    request = _PutRequest(pathname=pathname, body=b"", access="public")
    assert request.pathname == pathname.removeprefix("/")


@pytest.mark.parametrize(
    "pathname",
    ["x" * 951, "😀" * 476, "/" + "x" * 950, "x" * 949 + "😀"],
    ids=["ascii", "astral", "leading-slash", "mixed"],
)
def test_put_rejects_more_than_950_utf16_units(pathname: str) -> None:
    with pytest.raises(BlobError, match="maximum length is 950"):
        _PutRequest(pathname=pathname, body=b"", access="public")


@given(bmp=st.integers(0, 960), astral=st.integers(0, 480), leading_slash=st.booleans())
def test_put_limit_matches_javascript_string_length(
    bmp: int, astral: int, leading_slash: bool
) -> None:
    pathname = ("/" if leading_slash else "") + "a" * bmp + "😀" * astral
    units = len(pathname.encode("utf-16-le")) // 2
    if not pathname or pathname == "/" or units > 950:
        with pytest.raises(BlobError):
            _PutRequest(pathname=pathname, body=b"", access="public")
    else:
        assert _PutRequest(
            pathname=pathname, body=b"", access="public"
        ).pathname == pathname.removeprefix("/")


@pytest.mark.parametrize("pathname", ["x" * 981, "😀" * 476])
def test_read_pathnames_do_not_inherit_upload_limit(pathname: str) -> None:
    assert validate_pathname(pathname) == pathname
    url = construct_delivery_url("teststore", pathname, "public")
    assert parse_and_validate_delivery_url(url)[0] == url


@pytest.mark.parametrize("encoded", ["%ff", "%ed%a0%80"])
def test_delivery_url_rejects_invalid_utf8(encoded: str) -> None:
    with pytest.raises(BlobError, match="UTF-8"):
        parse_and_validate_delivery_url(f"https://{_HOST}/file{encoded}.txt")
