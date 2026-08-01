"""The payload wire format shared with `@workflow/core`.

Every run input, step input, step result, workflow output and hook payload
goes through :mod:`vercel._internal.workflow.serialization`, which writes a
4-byte format tag followed by `devalue.stringify` output. The devalue codec
itself is covered by ``test_devalue.py`` (and checked against the real JS
library in ``tests/integration/test_devalue.py``); what is pinned here is the
envelope around it and the errors it produces, because a payload that cannot
be read is otherwise noticed several frames away from its cause.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from vercel._internal import devalue
from vercel._internal.workflow import serialization as ser, world as w


def test_payload_is_devl_plus_devalue() -> None:
    # The exact bytes `@workflow/core`'s client codec produces:
    # `encodeWithFormatPrefix(DEVALUE_V1, encoder.encode(stringify(value)))`.
    assert ser.dehydrate("charged 42") == b'devl["charged 42"]'
    value = [[21], {}]
    assert ser.dehydrate(value) == b"devl" + devalue.stringify(value).encode()


@pytest.mark.parametrize(
    "value",
    [
        None,
        42,
        "unicode 你好",
        [[21], {"tier": "pro"}],
        {"nested": {"list": [1, 2.5, True, None]}},
        datetime(2026, 7, 30, 17, 6, 33, tzinfo=timezone.utc),
        b"\x00\xff\x80",
    ],
)
def test_round_trip(value) -> None:
    assert ser.hydrate(ser.dehydrate(value), what="a payload") == value


def test_shared_references_survive() -> None:
    # devalue's flattened form is what makes a repeated value one slot; the
    # JSON encoding this replaced silently duplicated it.
    shared = {"id": 1}
    restored = ser.hydrate(ser.dehydrate([shared, shared]), what="a payload")
    assert restored[0] is restored[1]


def test_envelope_formats_are_reported_by_name() -> None:
    # Written by the TS SDK when a run has encryption or compression enabled.
    # Unsupported here, but the payload is not corrupt and should not read as if
    # it were.
    for prefix in (ser.ENCRYPTED, ser.SEALED, ser.GZIP, ser.ZSTD):
        with pytest.raises(ser.SerializationError, match=f"{prefix.decode()}.*cannot read"):
            ser.hydrate(prefix + b"...", what="a payload")


def test_unknown_format_is_rejected() -> None:
    # `json` is what this SDK wrote before the devalue migration.
    with pytest.raises(ser.SerializationError, match="unknown serialization format"):
        ser.hydrate(b'json"charged 42"', what="a payload")


def test_non_binary_payload_is_rejected_at_the_boundary() -> None:
    # A `run_created` response carries the string '[Circular]' in place of the
    # input it echoes back. Naming the payload is the point: the alternative is
    # an AttributeError wherever the value is eventually used.
    with pytest.raises(ser.SerializationError, match="the input of run wrun_1 is not"):
        ser.hydrate("[Circular]", what="the input of run wrun_1")


def test_truncated_payload_is_rejected() -> None:
    with pytest.raises(ser.SerializationError, match="too short"):
        ser.hydrate(b"dev", what="a payload")


def test_undecodable_payload_names_the_field() -> None:
    with pytest.raises(ser.SerializationError, match="Cannot deserialize the output of run"):
        ser.hydrate(b"devlnot json", what="the output of run wrun_1")


def _run_row(**overrides) -> dict:
    return {
        "runId": "wrun_1",
        "status": "pending",
        "deploymentId": "dpl_1",
        "workflowName": "wf",
        "specVersion": 2,
        "createdAt": "2026-07-30T00:00:00.000Z",
        "updatedAt": "2026-07-30T00:00:00.000Z",
        **overrides,
    }


def test_a_stored_payload_loads_as_binary_not_text() -> None:
    """A run's ``input`` must reach the model as ``bytes``.

    It is typed ``bytes | str`` because of the ``'[Circular]'`` case below, and
    pydantic's lax coercion would happily put a payload in the ``str`` arm --
    where it fails much later, at the point of use, rather than here.
    """
    payload = ser.dehydrate([[21], {}])

    run = w.WorkflowRunAdaptor.validate_python(_run_row(input=payload))

    assert run.input == payload
    assert ser.hydrate(run.input, what="the input of run wrun_1") == [[21], {}]


def test_the_circular_marker_still_loads_as_text() -> None:
    # What a `run_created` response echoes back in place of the input.
    run = w.WorkflowRunAdaptor.validate_python(_run_row(input="[Circular]"))

    assert run.input == "[Circular]"
