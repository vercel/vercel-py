"""Measure the CPU time the SDK adds to Sandbox operations.

Every request is answered in-process, so no network or Sandbox API time is in
the numbers: what remains is the SDK and HTTPX building requests and decoding
responses. Each scenario reports the median and fastest of its iterations, and
bulk scenarios also report throughput.

Each scenario also reports how many requests one call makes. Against the real
API every request is a network round trip, which costs far more than the SDK
time measured here, so that count is the first thing to keep low.

Run it from this package's directory::

    uv run python -m benchmarks.sdk_overhead
    uv run python -m benchmarks.sdk_overhead --mode async --only run_command
    uv run python -m benchmarks.sdk_overhead --profile "communicate, many lines"
"""

import argparse
import cProfile
import json
import pstats
import statistics
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any

import anyio
import httpx2 as httpx

from vercel import sandbox
from vercel.api import session
from vercel.sandbox import SandboxCredentials, SandboxServiceOptions, sync as sandbox_sync

_BASE_URL = "https://sandbox.test"
_SESSION_ID = "sbx_0123456789abcdef"
_CREDENTIALS = SandboxCredentials(token="token", team_id="team_123", project_id="prj_123")
_NETWORK_CHUNK = 64 * 1024
_MANY_LINES_ENV = {"BENCHMARK_OUTPUT": "many-lines"}
_LONG_COMMAND_ID = "cmd_many_lines"


@dataclass(frozen=True)
class Workload:
    """Sizes of the bulk scenarios and how often each scenario is timed."""

    output_lines: int = 20_000
    file_bytes: int = 16 * 1024 * 1024
    request_iterations: int = 1_000
    bulk_iterations: int = 10


@dataclass(frozen=True)
class Result:
    name: str
    requests: int
    median_seconds: float
    fastest_seconds: float
    megabytes_per_second: float | None


@dataclass(frozen=True)
class _Scenario:
    name: str
    run_async: Callable[[Any], Awaitable[object]]
    run_sync: Callable[[Any], object]
    bulk_bytes: int | None = None


def _command(exit_code: int | None, command_id: str = "cmd_0123456789abcdef") -> dict[str, object]:
    return {
        "command": {
            "id": command_id,
            "name": "sh",
            "args": ["-c", "true"],
            "cwd": "/vercel/sandbox",
            "sessionId": _SESSION_ID,
            "exitCode": exit_code,
            "startedAt": 1,
        }
    }


def _ndjson(records: list[dict[str, object]]) -> bytes:
    return "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records).encode()


def _network_chunks(body: bytes) -> list[bytes]:
    return [body[start : start + _NETWORK_CHUNK] for start in range(0, len(body), _NETWORK_CHUNK)]


class _SyncChunks(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def __iter__(self) -> Iterator[bytes]:
        yield from self._chunks


class _AsyncChunks(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


class _FakeSandboxApi:
    """Answer the requests the scenarios make with realistic response bodies."""

    def __init__(self, workload: Workload) -> None:
        output: list[dict[str, object]] = [
            {"stream": "stdout", "data": "x" * 79 + "\n"} for _ in range(workload.output_lines)
        ]
        self._sandbox = json.dumps(
            {
                "sandbox": {
                    "name": "benchmark",
                    "currentSessionId": _SESSION_ID,
                    "image": "vercel/sandbox/universal:latest",
                    "status": "running",
                    "persistent": True,
                    "region": "iad1",
                    "timeout": 300000,
                    "networkPolicy": {"mode": "allow-all"},
                    "createdAt": 1,
                    "updatedAt": 2,
                },
                "session": {
                    "id": _SESSION_ID,
                    "sourceSandboxName": "benchmark",
                    "projectId": _CREDENTIALS.project_id,
                    "status": "running",
                    "cwd": "/vercel/sandbox",
                    "region": "iad1",
                    "memory": 2048,
                    "vcpus": 1,
                    "timeout": 300000,
                    "networkPolicy": {"mode": "allow-all"},
                },
                "routes": [
                    {"url": "https://benchmark.test", "subdomain": "benchmark", "port": 3000}
                ],
            }
        ).encode()
        self._command = json.dumps(_command(0)).encode()
        self._long_command = json.dumps(_command(0, _LONG_COMMAND_ID)).encode()
        self._short_run = _ndjson([_command(None), *output[:2], _command(0)])
        self._long_run = _network_chunks(_ndjson([_command(None), *output, _command(0)]))
        self._short_logs = _ndjson(output[:2])
        self._long_logs = _network_chunks(_ndjson(output))
        self._file = _network_chunks(bytes(range(256)) * (workload.file_bytes // 256))
        self.output_bytes = sum(len(chunk) for chunk in self._long_logs)
        self.requests = 0

    def _respond(self, request: httpx.Request) -> bytes | list[bytes]:
        self.requests += 1
        path = request.url.path
        if path.endswith("/fs/read"):
            return self._file
        if path.endswith("/logs"):
            return self._long_logs if _LONG_COMMAND_ID in path else self._short_logs
        if path.endswith("/cmd"):
            long = json.loads(request.content).get("env") == _MANY_LINES_ENV
            if request.url.params.get("wait") == "true":
                return self._long_run if long else self._short_run
            return self._long_command if long else self._command
        if "/cmd/" in path:
            return self._long_command if _LONG_COMMAND_ID in path else self._command
        if "/fs/" in path:
            return b"{}"
        return self._sandbox

    def handle_sync(self, request: httpx.Request) -> httpx.Response:
        for _ in request.stream:  # type: ignore[union-attr]
            pass
        body = self._respond(request)
        if isinstance(body, bytes):
            return httpx.Response(200, content=body)
        return httpx.Response(200, stream=_SyncChunks(body))

    async def handle_async(self, request: httpx.Request) -> httpx.Response:
        async for _ in request.stream:  # type: ignore[union-attr]
            pass
        body = self._respond(request)
        if isinstance(body, bytes):
            return httpx.Response(200, content=body)
        return httpx.Response(200, stream=_AsyncChunks(body))


def _scenarios(workload: Workload, api: _FakeSandboxApi) -> list[_Scenario]:
    content = bytes(range(256)) * (workload.file_bytes // 256)

    async def communicate(box: Any) -> object:
        return await (await box.create_command("sh", env=_MANY_LINES_ENV)).communicate()

    def communicate_sync(box: Any) -> object:
        return box.create_command("sh", env=_MANY_LINES_ENV).communicate()

    async def read_handle(box: Any) -> object:
        async with box.fs.open("/tmp/file", "rb") as file:
            return await file.read()

    def read_handle_sync(box: Any) -> object:
        with box.fs.open("/tmp/file", "rb") as file:
            return file.read()

    return [
        _Scenario(
            "get_sandbox",
            lambda box: sandbox.get_sandbox(name="benchmark"),
            lambda box: sandbox_sync.get_sandbox(name="benchmark"),
        ),
        _Scenario(
            "run_command",
            lambda box: box.run_command("sh", ["-c", "true"], capture_output=True),
            lambda box: box.run_command("sh", ["-c", "true"], capture_output=True),
        ),
        _Scenario(
            "fs.exists",
            lambda box: box.fs.exists("/tmp/file"),
            lambda box: box.fs.exists("/tmp/file"),
        ),
        _Scenario(
            "fs.write_bytes, 1 KiB",
            lambda box: box.fs.write_bytes("/tmp/file", content[:1024]),
            lambda box: box.fs.write_bytes("/tmp/file", content[:1024]),
        ),
        _Scenario(
            "run_command, many lines",
            lambda box: box.run_command("sh", env=_MANY_LINES_ENV, capture_output=True),
            lambda box: box.run_command("sh", env=_MANY_LINES_ENV, capture_output=True),
            api.output_bytes,
        ),
        _Scenario("communicate, many lines", communicate, communicate_sync, api.output_bytes),
        _Scenario(
            "fs.read_bytes",
            lambda box: box.fs.read_bytes("/tmp/file"),
            lambda box: box.fs.read_bytes("/tmp/file"),
            len(content),
        ),
        _Scenario("fs.open(rb).read", read_handle, read_handle_sync, len(content)),
        _Scenario(
            "fs.write_bytes",
            lambda box: box.fs.write_bytes("/tmp/file", content),
            lambda box: box.fs.write_bytes("/tmp/file", content),
            len(content),
        ),
    ]


def _selected(scenarios: list[_Scenario], only: str | None) -> list[_Scenario]:
    return [scenario for scenario in scenarios if only is None or only in scenario.name]


def _result(scenario: _Scenario, mode: str, samples: list[float], requests: int) -> Result:
    median = statistics.median(samples)
    return Result(
        name=f"{mode} {scenario.name}",
        requests=requests // len(samples),
        median_seconds=median,
        fastest_seconds=min(samples),
        megabytes_per_second=(
            None if scenario.bulk_bytes is None else scenario.bulk_bytes / median / 1e6
        ),
    )


def _iterations(scenario: _Scenario, workload: Workload) -> int:
    return workload.request_iterations if scenario.bulk_bytes is None else workload.bulk_iterations


async def run_async(
    workload: Workload | None = None, *, only: str | None = None, profile: str | None = None
) -> list[Result]:
    """Time each scenario through the async API, profiling ``profile`` if it is named."""
    workload = workload or Workload()
    api = _FakeSandboxApi(workload)

    async def credentials() -> SandboxCredentials:
        return _CREDENTIALS

    options = SandboxServiceOptions(base_url=_BASE_URL, credentials_factory=credentials)
    results = []
    async with session(
        service_options=[options],
        httpx_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(api.handle_async)
        ),
    ):
        box = await sandbox.get_sandbox(name="benchmark")
        for scenario in _selected(_scenarios(workload, api), only):
            await scenario.run_async(box)
            samples = []
            requests_before = api.requests
            profiler = cProfile.Profile() if scenario.name == profile else None
            for _ in range(_iterations(scenario, workload)):
                if profiler is not None:
                    profiler.enable()
                started = time.perf_counter()
                await scenario.run_async(box)
                samples.append(time.perf_counter() - started)
                if profiler is not None:
                    profiler.disable()
            if profiler is not None:
                pstats.Stats(profiler).sort_stats("tottime").print_stats(30)
            results.append(_result(scenario, "async", samples, api.requests - requests_before))
    return results


def run_sync(workload: Workload | None = None, *, only: str | None = None) -> list[Result]:
    """Time each scenario through the sync API."""
    workload = workload or Workload()
    api = _FakeSandboxApi(workload)
    options = sandbox_sync.SandboxServiceOptions(
        base_url=_BASE_URL, credentials_factory=lambda: _CREDENTIALS
    )
    results = []
    with session(
        service_options=[options],
        httpx_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(api.handle_sync)),
    ):
        box = sandbox_sync.get_sandbox(name="benchmark")
        for scenario in _selected(_scenarios(workload, api), only):
            scenario.run_sync(box)
            samples = []
            requests_before = api.requests
            for _ in range(_iterations(scenario, workload)):
                started = time.perf_counter()
                scenario.run_sync(box)
                samples.append(time.perf_counter() - started)
            results.append(_result(scenario, "sync", samples, api.requests - requests_before))
    return results


def format_results(results: list[Result]) -> str:
    lines = [f"{'scenario':<34}{'requests':>9}{'median':>12}{'fastest':>12}{'throughput':>14}"]
    for result in results:
        throughput = (
            ""
            if result.megabytes_per_second is None
            else f"{result.megabytes_per_second:,.1f} MB/s"
        )
        lines.append(
            f"{result.name:<34}"
            f"{result.requests:>9}"
            f"{result.median_seconds * 1e3:>9.3f} ms"
            f"{result.fastest_seconds * 1e3:>9.3f} ms"
            f"{throughput:>14}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--mode", choices=["async", "sync", "both"], default="both")
    parser.add_argument("--only", help="run the scenarios whose name contains this text")
    parser.add_argument("--profile", help="print a cProfile report for this async scenario")
    parser.add_argument("--json", action="store_true", help="print results as JSON")
    args = parser.parse_args(argv)

    results: list[Result] = []
    if args.mode != "sync":
        results += anyio.run(lambda: run_async(only=args.only, profile=args.profile))
    if args.mode != "async":
        results += run_sync(only=args.only)
    if args.json:
        print(json.dumps([result.__dict__ for result in results], indent=2))
    else:
        print(f"Python {sys.version.split()[0]}")
        print(format_results(results))


if __name__ == "__main__":
    main()
