"""The sync surface mirrors the async one, documentation included."""

import inspect

import pytest

import vercel.passport as passport_async
import vercel.passport.sync as passport_sync


@pytest.mark.parametrize("name", ["get_identity", "require_identity", "verify_identity"])
def test_signatures_match(name: str) -> None:
    async_signature = inspect.signature(getattr(passport_async, name))
    sync_signature = inspect.signature(getattr(passport_sync, name))

    assert sync_signature == async_signature


@pytest.mark.parametrize("name", ["get_identity", "require_identity", "verify_identity"])
def test_docstrings_match_apart_from_the_blocking_note(name: str) -> None:
    """The docstrings are duplicated so neither surface sends readers elsewhere."""
    async_doc = inspect.getdoc(getattr(passport_async, name))
    sync_doc = inspect.getdoc(getattr(passport_sync, name))
    assert async_doc is not None
    assert sync_doc is not None

    summary, _, body = async_doc.partition("\n\n")
    sync_summary, _, sync_rest = sync_doc.partition("\n\n")
    note, _, sync_body = sync_rest.partition("\n\n")

    assert sync_summary == summary
    assert f"`vercel.passport.{name}`" in note
    assert sync_body == body


def test_exports_match() -> None:
    assert set(passport_sync.__all__) == set(passport_async.__all__) - {"__version__", "sync"}
