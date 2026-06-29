"""Tests for wait-continuation idempotency-key/delay selection.

Mirrors @workflow/core's wait-continuation.ts: the delayed message that wakes a
run when a pending wait elapses is keyed on the wait's correlation id so repeated
suspension passes over the same wait collapse to a single timer, with hop/second
suffixes for the chained-long-wait and near-elapsed cases.
"""

from __future__ import annotations

from datetime import datetime

from vercel._internal.polyfills import UTC
from vercel._internal.workflow.runtime import (
    NEAR_ELAPSED_WAIT_THRESHOLD_SECONDS,
    WAIT_CONTINUATION_MAX_DELAY_SECONDS,
    _wait_continuation_dispatch,
)

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
CID = "wait_abc"


def test_mid_range_wait_uses_bare_correlation_id() -> None:
    delay, key = _wait_continuation_dispatch(100, CID, NOW)
    assert delay == 100
    assert key == CID


def test_repeated_passes_produce_identical_key() -> None:
    # The whole point: two suspension passes over the same pending wait dedupe.
    _, k1 = _wait_continuation_dispatch(100, CID, NOW)
    _, k2 = _wait_continuation_dispatch(100, CID, NOW)
    assert k1 == k2 == CID


def test_long_wait_clamps_delay_and_suffixes_hop() -> None:
    timeout = WAIT_CONTINUATION_MAX_DELAY_SECONDS * 2 + 5  # spans 3 hops
    delay, key = _wait_continuation_dispatch(timeout, CID, NOW)
    assert delay == WAIT_CONTINUATION_MAX_DELAY_SECONDS
    assert key == f"{CID}:hop-3"


def test_single_hop_wait_has_no_suffix() -> None:
    _, key = _wait_continuation_dispatch(WAIT_CONTINUATION_MAX_DELAY_SECONDS, CID, NOW)
    assert key == CID


def test_near_elapsed_wait_is_second_bucketed() -> None:
    delay, key = _wait_continuation_dispatch(NEAR_ELAPSED_WAIT_THRESHOLD_SECONDS, CID, NOW)
    assert delay == NEAR_ELAPSED_WAIT_THRESHOLD_SECONDS
    assert key == f"{CID}:{int(NOW.timestamp())}"
