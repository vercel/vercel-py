from __future__ import annotations

from scripts import turbo_summary


def _task(task_id: str, *, status: str = "MISS", exit_code: int | None = 0) -> dict[str, object]:
    task: dict[str, object] = {"taskId": task_id, "cache": {"status": status}}
    if exit_code is not None:
        task["execution"] = {"exitCode": exit_code}
    return task


def test_collect_reports_status_and_tasks_missing_from_the_run() -> None:
    summary = {
        "tasks": [
            _task("a#poe:test"),
            _task("b#poe:test", status="HIT", exit_code=None),
            _task("c#poe:test", exit_code=1),
            _task("a#sources"),
        ]
    }

    statuses, missing = turbo_summary.collect(summary, ["a", "b", "c", "d"], ["poe:test"])

    assert statuses == {"a#poe:test": "passed", "b#poe:test": "cached", "c#poe:test": "failed"}
    assert missing == ["d#poe:test"]
