"""On-disk format of the LocalWorld data directory.

The TypeScript `@workflow/world-local` package reads and writes the same
`.workflow-data` tree, so the files have to be interchangeable. Its writer is
``JSON.stringify(value, jsonReplacer, 2)``: two-space indent, ``Uint8Array``
smuggled through a ``{__type, data}`` wrapper, ``Date`` as
``toISOString()``, and unset fields simply absent -- ``undefined`` is not a
JSON value. Its reader is correspondingly strict: the run schema types
``output`` / ``error`` / ``completedAt`` as ``z.undefined()``, which rejects an
explicit null outright, and ``z.coerce.date()`` would quietly turn a null
``startedAt`` into the epoch. So the omissions below are load-bearing, not
cosmetic.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone

import pydantic
import pytest

from vercel._internal.workflow import serialization as ser, world as w
from vercel._internal.workflow.worlds import local as local_mod

RUN_ID = "wrun_test"


def _world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


def _iter_json_files(data_dir):
    return sorted(p for p in data_dir.rglob("*.json") if p.is_file())


async def _populate(world) -> str:
    """Drive a run through every entity the local world persists."""
    result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_1",
            workflowName="workflow//./src/wf//main",
            input=ser.dehydrate([]),
            executionContext={"workflowCoreVersion": "5.0.0"},
        ).into_event(),
    )
    assert result.run is not None
    run_id = result.run.run_id
    await world.events_create(run_id, w.RunStartedEvent())
    await world.events_create(
        run_id,
        w.StepCreatedEventData(
            stepName="step//./src/wf//pay", input=ser.dehydrate(ser.step_arguments((), {}))
        ).into_event("step_0"),
    )
    await world.events_create(run_id, w.StepStartedEventData().into_event("step_0"))
    await world.events_create(
        run_id, w.StepCompletedEventData(result=ser.dehydrate(42)).into_event("step_0")
    )
    await world.events_create(run_id, w.HookCreatedEventData(token="tok-abc").into_event("hook_0"))
    return run_id


def test_dumps_js_matches_json_stringify_conventions() -> None:
    # Mirrors `JSON.stringify(value, jsonReplacer, 2)`: two-space indent, no
    # space before the comma, no \uXXXX escaping of non-ASCII, and no trailing
    # newline. JS has one number type, so an integral float loses its ".0".
    payload = {
        "note": "你好",
        "blob": b"\x00\xff\x80",
        "at": datetime(2026, 7, 30, 17, 6, 33, 759000, tzinfo=timezone.utc),
        "count": 1.0,
        "ratio": 0.25,
        "empty": {},
        "items": [],
    }
    assert local_mod.dumps_js(payload).decode() == (
        "{\n"
        '  "note": "你好",\n'
        '  "blob": {\n'
        '    "__type": "Uint8Array",\n'
        f'    "data": "{base64.b64encode(bytes([0, 255, 128])).decode()}"\n'
        "  },\n"
        '  "at": "2026-07-30T17:06:33.759Z",\n'
        '  "count": 1,\n'
        '  "ratio": 0.25,\n'
        '  "empty": {},\n'
        '  "items": []\n'
        "}"
    )


def test_to_js_iso_matches_date_to_iso_string() -> None:
    # `Date.prototype.toISOString` is always UTC with exactly three fractional
    # digits. Sub-millisecond precision is truncated (a JS Date never had it),
    # a naive datetime is read as UTC, and an offset-aware one is converted.
    assert (
        local_mod.to_js_iso(datetime(2026, 7, 30, 17, 6, 33, 759876, tzinfo=timezone.utc))
        == "2026-07-30T17:06:33.759Z"
    )
    assert local_mod.to_js_iso(datetime(2026, 1, 2, 3, 4, 5)) == "2026-01-02T03:04:05.000Z"
    assert (
        local_mod.to_js_iso(datetime(2026, 7, 30, 13, 6, 33, tzinfo=timezone(timedelta(hours=-4))))
        == "2026-07-30T17:06:33.000Z"
    )


def test_binary_payloads_round_trip_through_the_uint8array_wrapper() -> None:
    blob = bytes(range(256))
    encoded = local_mod.dumps_js({"blob": blob})
    # On disk it is the wrapper TS's `jsonReplacer` produces, not a bare string.
    assert json.loads(encoded)["blob"] == {
        "__type": "Uint8Array",
        "data": base64.b64encode(blob).decode(),
    }
    assert local_mod._decode_js(json.loads(encoded)) == {"blob": blob}


async def test_written_files_are_json_stringify_output(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    await _populate(world)

    files = _iter_json_files(world.data_dir)
    assert files, "expected the run to persist some entities"
    for path in files:
        text = path.read_text(encoding="utf-8")
        # Parses as JSON (not CBOR or any other container)...
        data = json.loads(text)
        # ...and is formatted the way JSON.stringify(value, replacer, 2) is.
        # The token-claim sidecar is the one file TS writes compactly.
        indent = None if path.parent.name == "tokens" else 2
        assert text == json.dumps(
            data, indent=indent, separators=(",", ":") if indent is None else (",", ": ")
        ), f"{path.relative_to(world.data_dir)} is not JSON.stringify output"


async def test_unset_fields_are_omitted_not_null(tmp_path, monkeypatch) -> None:
    # The TS run schema types output/error/completedAt as `z.undefined()`, which
    # rejects null, and its date fields would coerce a null to the epoch. A null
    # anywhere a field is merely unset therefore breaks the TS reader.
    world = _world(tmp_path, monkeypatch)
    await _populate(world)

    def assert_no_null(value, path: str) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                assert item is not None, f"{path}.{key} is null; TS omits unset fields"
                assert_no_null(item, f"{path}.{key}")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                assert_no_null(item, f"{path}[{index}]")

    for path in _iter_json_files(world.data_dir):
        assert_no_null(
            json.loads(path.read_text(encoding="utf-8")), str(path.relative_to(world.data_dir))
        )


async def test_run_row_always_carries_attributes(tmp_path, monkeypatch) -> None:
    # TS writes `attributes: runData.attributes ?? {}` on creation and copies it
    # forward on every lifecycle transition, so the field is never absent.
    world = _world(tmp_path, monkeypatch)
    run_id = await _populate(world)
    run_path = world.data_dir / "runs" / f"{run_id}.json"

    assert json.loads(run_path.read_text(encoding="utf-8"))["attributes"] == {}

    # A transition must preserve attributes another writer put there rather than
    # clobbering them with its own stale snapshot.
    stored = json.loads(run_path.read_text(encoding="utf-8"))
    stored["attributes"] = {"tier": "pro"}
    run_path.write_text(json.dumps(stored), encoding="utf-8")
    await world.events_create(
        run_id, w.RunCompletedEventData(output=ser.dehydrate(42)).into_event()
    )
    assert json.loads(run_path.read_text(encoding="utf-8"))["attributes"] == {"tier": "pro"}


async def test_timestamps_survive_the_round_trip(tmp_path, monkeypatch) -> None:
    # Timestamps land on disk at millisecond resolution, so the world stamps
    # them at that resolution too -- otherwise the value handed to the caller
    # would differ from the one a later read parses back out.
    world = _world(tmp_path, monkeypatch)
    result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_1",
            workflowName="workflow//./src/wf//main",
            input=ser.dehydrate([]),
        ).into_event(),
    )
    assert result.run is not None
    reread = await world.runs_get(result.run.run_id)
    assert reread.created_at == result.run.created_at
    assert reread.updated_at == result.run.updated_at


async def test_hook_token_claim_matches_the_ts_sidecar(tmp_path, monkeypatch) -> None:
    # TS writes `JSON.stringify({token, hookId, runId, eventId})` -- compact,
    # and carrying the eventId its claim-recovery path reads back.
    world = _world(tmp_path, monkeypatch)
    result = await world.events_create(
        RUN_ID, w.HookCreatedEventData(token="tok-abc").into_event("hook_0")
    )

    assert result.event is not None
    assert result.event.server_props is not None

    claims = list((world.data_dir / "hooks" / "tokens").glob("*.json"))
    assert len(claims) == 1
    text = claims[0].read_text(encoding="utf-8")
    assert json.loads(text) == {
        "token": "tok-abc",
        "hookId": "hook_0",
        "runId": RUN_ID,
        "eventId": result.event.server_props.event_id,
    }
    assert " " not in text, "the claim sidecar is written compactly, as in TS"


async def test_reads_a_log_written_at_a_newer_spec_version(tmp_path, monkeypatch) -> None:
    # The shape of a `vercel/workflow` e2e run against a Python app: the log is
    # opened by the TypeScript driver, whose `start()` writes `run_created` at
    # the version its World declares -- 6 on the Vercel adapter -- and the
    # Python app under test replays it. Nothing here decodes by version (the
    # payload prefix says the format), and the row inherits the version of the
    # event.
    world = _world(tmp_path, monkeypatch)
    result = await world.events_create(
        None,
        w.RunCreatedEvent(
            eventData=w.RunCreatedEventData(
                deploymentId="dpl_1",
                workflowName="workflow//./src/wf//main",
                input=ser.dehydrate([]),
            ),
            specVersion=6,
        ),
    )
    assert result.run is not None
    run_id = result.run.run_id
    assert result.run.spec_version == 6
    assert json.loads((world.data_dir / "runs" / f"{run_id}.json").read_text())["specVersion"] == 6
    assert [event.spec_version for event in (await world.events_list(run_id)).data] == [6]

    # Our own writes into the run leave its version alone.
    await world.events_create(run_id, w.RunStartedEvent())
    assert (await world.runs_get(run_id)).spec_version == 6


def test_rejects_a_spec_version_above_the_supported_ceiling() -> None:
    with pytest.raises(pydantic.ValidationError, match="less than or equal to 6"):
        w.RunStartedEvent(specVersion=w.SPEC_VERSION_MAX_SUPPORTED + 1)
