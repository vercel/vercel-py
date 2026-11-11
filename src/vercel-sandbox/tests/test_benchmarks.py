import anyio.to_thread
from benchmarks.sdk_overhead import Result, Workload, run_async, run_sync

_TINY = Workload(output_lines=40, file_bytes=128 * 1024, request_iterations=1, bulk_iterations=1)


def _scenario_names(results: list[Result], mode: str) -> list[str]:
    assert all(result.fastest_seconds > 0 and result.requests > 0 for result in results)
    return [result.name.removeprefix(f"{mode} ") for result in results]


async def test_overhead_benchmark_times_the_same_scenarios_in_both_modes() -> None:
    timed_async = _scenario_names(await run_async(_TINY), "async")
    timed_sync = _scenario_names(await anyio.to_thread.run_sync(run_sync, _TINY), "sync")

    assert "communicate, many lines" in timed_async
    assert timed_sync == timed_async
