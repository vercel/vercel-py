from __future__ import annotations

from typing import Any, Literal, cast
from typing_extensions import Self

import argparse
import cProfile
import json
import os
import pstats
import subprocess  # noqa: S404
import sys
import threading
import time
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType

import anyio
import httpx
from anyio import to_thread
from anyio.abc import Process
from anyio.lowlevel import checkpoint

from vercel.queue import (
    ALL_DEPLOYMENTS,
    ByteStreamTransport,
    Delivery,
    QueueClient,
    Topic,
    subscribe,
)
from vercel.queue._internal import lease as lease_internal  # noqa: PLC2701
from vercel.queue._internal.embedded import EmbeddedQueueDispatcher  # noqa: PLC2701
from vercel.queue._internal.lease import reset_lease_renewal_worker_for_tests  # noqa: PLC2701
from vercel.queue.devserver import embedded_queue_dev_server, queue_client_asgi_dev_server
from vercel.queue.embedded import embedded_queue_service
from vercel.queue.testing import clear_subscriptions

ScenarioName = Literal["small", "large"]
DeliveryMode = Literal["pull", "push"]
Architecture = Literal["in-process", "split"]
ClientName = Literal["python", "js"]
DEFAULT_SMALL_MESSAGES = 10_000
DEFAULT_LARGE_MESSAGES = 100
DEFAULT_CONCURRENCY = 100
DEFAULT_POLL_LIMIT = 10
DEFAULT_SMALL_PAYLOAD_BYTES = 64
DEFAULT_LARGE_PAYLOAD_BYTES = 1024 * 1024
DEFAULT_CHUNK_BYTES = 64 * 1024
TOPIC = "bench-topic"
CONSUMER_GROUP = "bench-consumer"
LOCAL_TOKEN = "local-token"  # noqa: S105
MODULE_PATH = str(Path(__file__).resolve())
PROJECT_ROOT = Path(__file__).resolve().parents[3]
BENCHMARK_ROOT = PROJECT_ROOT / ".benchmarks"
JS_CLIENT_ROOT = BENCHMARK_ROOT / "js-client"
JS_CLIENT_SCRIPT = JS_CLIENT_ROOT / "client.mjs"
ROLE_NEWLINE = b"\n"
ENV_QUEUE_SERVER_PROFILE = "VERCEL_QUEUE_BENCH_QUEUE_SERVER_PROFILE"
ENV_CLIENT_SERVER_PROFILE = "VERCEL_QUEUE_BENCH_CLIENT_SERVER_PROFILE"
ENV_LEASE_WORKER_PROFILE = "VERCEL_QUEUE_BENCH_LEASE_WORKER_PROFILE"
ENV_SPLIT_ROLE = "VERCEL_QUEUE_BENCH_ROLE"
ENV_QUEUE_BASE_URL = "VERCEL_QUEUE_BENCH_QUEUE_BASE_URL"
ENV_BENCH_CONFIG = "VERCEL_QUEUE_BENCH_CONFIG"


@dataclass(frozen=True)
class BenchmarkConfig:
    scenario: ScenarioName
    messages: int
    delivery: DeliveryMode = "pull"
    concurrency: int = DEFAULT_CONCURRENCY
    poll_limit: int = DEFAULT_POLL_LIMIT
    small_payload_bytes: int = DEFAULT_SMALL_PAYLOAD_BYTES
    large_payload_bytes: int = DEFAULT_LARGE_PAYLOAD_BYTES
    chunk_size: int = DEFAULT_CHUNK_BYTES
    architecture: Architecture = "in-process"
    client: ClientName = "python"

    def __post_init__(self) -> None:
        if self.client == "js" and (self.architecture != "split" or self.delivery != "push"):
            raise ValueError("--client js requires --architecture split --delivery push")


@dataclass(frozen=True)
class LatencySummary:
    minimum_seconds: float
    median_seconds: float
    p95_seconds: float
    p99_seconds: float
    maximum_seconds: float

    @classmethod
    def from_samples(cls, samples: Sequence[float]) -> LatencySummary:
        if not samples:
            return cls(0.0, 0.0, 0.0, 0.0, 0.0)
        sorted_samples = sorted(samples)
        return cls(
            minimum_seconds=sorted_samples[0],
            median_seconds=_percentile(sorted_samples, 0.50),
            p95_seconds=_percentile(sorted_samples, 0.95),
            p99_seconds=_percentile(sorted_samples, 0.99),
            maximum_seconds=sorted_samples[-1],
        )

    def to_json(self) -> dict[str, float]:
        return {
            "min_seconds": self.minimum_seconds,
            "median_seconds": self.median_seconds,
            "p95_seconds": self.p95_seconds,
            "p99_seconds": self.p99_seconds,
            "max_seconds": self.maximum_seconds,
        }


@dataclass(frozen=True)
class PhaseResult:
    seconds: float
    messages: int
    bytes: int
    latency: LatencySummary

    @property
    def messages_per_second(self) -> float:
        return _rate(self.messages, self.seconds)

    @property
    def mib_per_second(self) -> float:
        return _rate(self.bytes / (1024 * 1024), self.seconds)

    def to_json(self) -> dict[str, Any]:
        return {
            "seconds": self.seconds,
            "messages": self.messages,
            "bytes": self.bytes,
            "messages_per_second": self.messages_per_second,
            "mib_per_second": self.mib_per_second,
            "latency": self.latency.to_json(),
        }


@dataclass(frozen=True)
class BenchmarkResult:
    scenario: ScenarioName
    config: BenchmarkConfig
    total_seconds: float
    send: PhaseResult
    receive_ack: PhaseResult
    errors: int
    peak_in_flight: int

    @property
    def messages_per_second(self) -> float:
        return _rate(self.config.messages, self.total_seconds)

    @property
    def mib_per_second(self) -> float:
        return _rate(self.send.bytes / (1024 * 1024), self.total_seconds)

    def to_json(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "config": {
                "delivery": self.config.delivery,
                "messages": self.config.messages,
                "concurrency": self.config.concurrency,
                "poll_limit": self.config.poll_limit,
                "small_payload_bytes": self.config.small_payload_bytes,
                "large_payload_bytes": self.config.large_payload_bytes,
                "chunk_size": self.config.chunk_size,
                "architecture": self.config.architecture,
                "client": self.config.client,
            },
            "total_seconds": self.total_seconds,
            "messages_per_second": self.messages_per_second,
            "mib_per_second": self.mib_per_second,
            "send": self.send.to_json(),
            "receive_ack": self.receive_ack.to_json(),
            "errors": self.errors,
            "peak_in_flight": self.peak_in_flight,
        }


@dataclass
class _PhaseCounters:
    latencies: list[float] = field(default_factory=list)
    bytes: int = 0
    messages: int = 0
    errors: int = 0


@dataclass
class _SendState:
    next_message: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0
    counters: _PhaseCounters = field(default_factory=_PhaseCounters)


@dataclass
class _ReceiveState:
    received: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0
    counters: _PhaseCounters = field(default_factory=_PhaseCounters)


@dataclass
class _PushState:
    handled: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0
    counters: _PhaseCounters = field(default_factory=_PhaseCounters)
    done: anyio.Event = field(default_factory=anyio.Event)
    handlers: list[Any] = field(default_factory=list)


@dataclass
class _SplitCallbackMetrics:
    lock: threading.Lock = field(default_factory=threading.Lock)
    messages: int = 0
    errors: int = 0
    latencies: list[float] = field(default_factory=list)

    def record_success(self, latency: float) -> None:
        with self.lock:
            self.messages += 1
            self.latencies.append(latency)

    def record_error(self, latency: float) -> None:
        with self.lock:
            self.errors += 1
            self.latencies.append(latency)


class _BenchmarkPushClient(QueueClient):
    def __init__(self, *, metrics: _SplitCallbackMetrics, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._metrics = metrics

    async def accept_and_handle(
        self,
        raw_body: Any,
        headers: Any = None,
        *,
        lease_duration: Any | None = None,
    ) -> None:
        started = time.perf_counter()
        try:
            await super().accept_and_handle(
                raw_body,
                headers,
                lease_duration=lease_duration,
            )
        except Exception:
            self._metrics.record_error(time.perf_counter() - started)
            raise
        self._metrics.record_success(time.perf_counter() - started)


@dataclass
class _ReceiveResult:
    phase: PhaseResult
    errors: int
    peak_in_flight: int
    state: _PushState | None = None
    started: float | None = None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run async Vercel Queue SDK load benchmarks.")
    parser.add_argument(
        "--scenario",
        choices=["small", "large", "all"],
        default="all",
        help="benchmark scenario to run",
    )
    parser.add_argument(
        "--delivery",
        choices=["pull", "push", "all"],
        default="pull",
        help="delivery path to benchmark",
    )
    parser.add_argument(
        "--architecture",
        choices=["in-process", "split"],
        default="in-process",
        help="run push delivery in-process or split across localhost uvicorn servers",
    )
    parser.add_argument(
        "--client",
        choices=["python", "js"],
        default="python",
        help="callback client implementation for split push benchmarks",
    )
    parser.add_argument(
        "--messages",
        type=_positive_int,
        default=None,
        help="message count for the selected scenario",
    )
    parser.add_argument(
        "--concurrency",
        type=_positive_int,
        default=DEFAULT_CONCURRENCY,
        help="number of concurrent send and receive workers",
    )
    parser.add_argument(
        "--poll-limit",
        type=_poll_limit,
        default=DEFAULT_POLL_LIMIT,
        help="messages requested per poll call, from 1 through 10",
    )
    parser.add_argument(
        "--small-size",
        type=_positive_int,
        default=DEFAULT_SMALL_PAYLOAD_BYTES,
        help="approximate small JSON payload body bytes",
    )
    parser.add_argument(
        "--large-size",
        type=_positive_int,
        default=DEFAULT_LARGE_PAYLOAD_BYTES,
        help="large byte-stream payload bytes per message",
    )
    parser.add_argument(
        "--chunk-size",
        type=_positive_int,
        default=DEFAULT_CHUNK_BYTES,
        help="chunk size for streamed large messages",
    )
    parser.add_argument(
        "--backend",
        choices=["asyncio", "trio"],
        default="asyncio",
        help="AnyIO backend to use",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit machine-readable JSON instead of text",
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=None,
        help="write cProfile data to this path and print top cumulative functions",
    )
    parser.add_argument(
        "--split-profile-dir",
        type=Path,
        default=None,
        help="write separate cProfile data for split queue and client processes",
    )
    return parser.parse_args(argv)


async def run_benchmarks(
    configs: Sequence[BenchmarkConfig],
    profile_dir: Path | None = None,
) -> list[BenchmarkResult]:
    return [await run_benchmark(config, profile_dir=profile_dir) for config in configs]


async def run_benchmark(
    config: BenchmarkConfig,
    *,
    profile_dir: Path | None = None,
) -> BenchmarkResult:
    _validate_config(config)
    if config.architecture == "split":
        return await _run_split_benchmark(config, profile_dir=profile_dir)
    total_started = time.perf_counter()
    clear_subscriptions()
    metrics = _SplitCallbackMetrics()
    async with embedded_queue_service() as service:
        if config.delivery == "push":

            def get_metrics_client() -> _BenchmarkPushClient:
                return _BenchmarkPushClient(
                    metrics=metrics,
                    token=LOCAL_TOKEN,
                    region=service.region,
                    base_url=service.base_url,
                    deployment=service.deployment,
                    http_client_factory=service.async_http_client_factory,
                )

            object.__setattr__(service, "get_async_client", get_metrics_client)  # noqa: PLC2801
        client = service.get_async_client()
        await _warm_up(client)
        if config.delivery == "push":
            receive_ack = _register_push_handler(config)
            send = await _send_messages(client, config)
            await _wait_for_push_messages(service, receive_ack, config, metrics)
        else:
            send = await _send_messages(client, config)
            receive_ack = await _receive_and_ack_messages(client, config)
    total_seconds = time.perf_counter() - total_started
    try:
        return BenchmarkResult(
            scenario=config.scenario,
            config=config,
            total_seconds=total_seconds,
            send=send.phase,
            receive_ack=receive_ack.phase,
            errors=send.errors
            + receive_ack.errors
            + max(0, config.messages - receive_ack.phase.messages),
            peak_in_flight=max(send.peak_in_flight, receive_ack.peak_in_flight),
        )
    finally:
        clear_subscriptions()


def configs_from_args(args: argparse.Namespace) -> list[BenchmarkConfig]:
    scenarios: list[ScenarioName]
    scenarios = ["small", "large"] if args.scenario == "all" else [args.scenario]
    delivery_modes: list[DeliveryMode]
    delivery_modes = ["pull", "push"] if args.delivery == "all" else [args.delivery]

    configs: list[BenchmarkConfig] = []
    for scenario in scenarios:
        for delivery in delivery_modes:
            default_messages = (
                DEFAULT_SMALL_MESSAGES if scenario == "small" else DEFAULT_LARGE_MESSAGES
            )
            configs.append(
                BenchmarkConfig(
                    scenario=scenario,
                    delivery=delivery,
                    messages=args.messages or default_messages,
                    concurrency=args.concurrency,
                    poll_limit=args.poll_limit,
                    small_payload_bytes=args.small_size,
                    large_payload_bytes=args.large_size,
                    chunk_size=args.chunk_size,
                    architecture=args.architecture,
                    client=args.client,
                )
            )
    return configs


def format_text(results: Sequence[BenchmarkResult]) -> str:
    return "\n\n".join(
        "\n".join([
            f"scenario: {result.scenario}",
            f"delivery: {result.config.delivery}",
            f"architecture: {result.config.architecture}",
            f"client: {result.config.client}",
            f"messages: {result.config.messages}",
            (
                f"total: {_seconds(result.total_seconds)} "
                f"({_rate_text(result.messages_per_second, 'msg/s')}, "
                f"{_rate_text(result.mib_per_second, 'MiB/s')})"
            ),
            _format_phase("send", result.send),
            _format_phase(_receive_phase_name(result.config.delivery), result.receive_ack),
            f"peak in-flight: {result.peak_in_flight}",
            f"errors: {result.errors}",
        ])
        for result in results
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    role = os.environ.get(ENV_SPLIT_ROLE, "driver")
    if role == "split-queue":
        return _profiled_role(args.profile, lambda: _run_split_queue_role(args))
    if role == "split-client":
        return _profiled_role(args.profile, lambda: _run_split_client_role(args))
    if role != "driver":
        raise ValueError(f"unknown benchmark role: {role!r}")
    configs = configs_from_args(args)

    if args.profile is None:
        results = _run_configs_sync(configs, args.backend, profile_dir=args.split_profile_dir)
    else:
        profiler = cProfile.Profile()
        results = profiler.runcall(
            _run_configs_sync,
            configs,
            args.backend,
            profile_dir=args.split_profile_dir,
        )
        profiler.dump_stats(args.profile)
        stats = pstats.Stats(profiler, stream=sys.stderr).sort_stats("cumulative")
        stats.print_stats(25)

    if args.json:
        sys.stdout.write(
            json.dumps([result.to_json() for result in results], indent=2, sort_keys=True) + "\n"
        )
    else:
        sys.stdout.write(f"{format_text(results)}\n")
    return 0


def _profiled_role(profile: Path | None, run: Callable[[], int]) -> int:
    if profile is None:
        return run()
    profiler = cProfile.Profile()
    try:
        return profiler.runcall(run)
    finally:
        profiler.dump_stats(profile)


def _profile_lease_renewal_worker(profile: str | None) -> Callable[[], None]:
    original = lease_internal._run_lease_renewal_worker_thread  # noqa: SLF001
    if profile is None:
        return lambda: None

    def profiled_worker() -> None:
        profiler = cProfile.Profile()
        try:
            profiler.runcall(original)
        finally:
            profiler.dump_stats(profile)

    lease_internal._run_lease_renewal_worker_thread = profiled_worker  # noqa: SLF001  # ty: ignore[invalid-assignment]

    def restore() -> None:
        lease_internal._run_lease_renewal_worker_thread = original  # noqa: SLF001

    return restore


def _run_configs_sync(
    configs: Sequence[BenchmarkConfig],
    backend: str,
    *,
    profile_dir: Path | None = None,
) -> list[BenchmarkResult]:
    return anyio.run(run_benchmarks, configs, profile_dir, backend=backend)


JS_CLIENT_HARNESS = r"""
import http from "node:http";
import { QueueClient } from "@vercel/queue";

const TOPIC = "bench-topic";
const LOCAL_TOKEN = "local-token";
const queueBaseUrl = process.env.VERCEL_QUEUE_BENCH_QUEUE_BASE_URL;
const config = JSON.parse(process.env.VERCEL_QUEUE_BENCH_CONFIG ?? "{}");

if (!queueBaseUrl) {
  throw new Error("VERCEL_QUEUE_BENCH_QUEUE_BASE_URL is required");
}

const metrics = {
  messages: 0,
  bytes: 0,
  errors: 0,
  inFlight: 0,
  peakInFlight: 0,
  latencies: [],
};

const client = new QueueClient({
  token: LOCAL_TOKEN,
  region: "iad1",
  deploymentId: null,
  resolveBaseUrl: () => new URL(queueBaseUrl),
});

function jsonPayloadSize(payload) {
  return Buffer.byteLength(JSON.stringify(payload));
}

async function payloadSize(payload) {
  if (config.scenario !== "large") {
    return jsonPayloadSize(payload);
  }
  if (payload instanceof Uint8Array) {
    return payload.byteLength;
  }
  if (payload instanceof ArrayBuffer) {
    return payload.byteLength;
  }
  if (payload && typeof payload.getReader === "function") {
    let total = 0;
    const reader = payload.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        total += value?.byteLength ?? 0;
      }
    } finally {
      reader.releaseLock();
    }
    return total;
  }
  return jsonPayloadSize(payload);
}

const callback = client.handleCallback(async (payload) => {
  metrics.inFlight += 1;
  metrics.peakInFlight = Math.max(metrics.peakInFlight, metrics.inFlight);
  try {
    metrics.bytes += await payloadSize(payload);
    metrics.messages += 1;
  } catch (error) {
    metrics.errors += 1;
    throw error;
  } finally {
    metrics.inFlight -= 1;
  }
});

const server = http.createServer(async (req, res) => {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const body = Buffer.concat(chunks);
  const request = new Request(`http://localhost${req.url ?? "/"}`, {
    method: req.method,
    headers: req.headers,
    body,
  });
  const started = performance.now();
  try {
    const response = await callback(request);
    metrics.latencies.push((performance.now() - started) / 1000);
    res.writeHead(response.status, Object.fromEntries(response.headers));
    res.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    metrics.errors += 1;
    metrics.latencies.push((performance.now() - started) / 1000);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: String(error) }));
  }
});

function writeJson(payload) {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
}

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  writeJson({ baseUrl: `http://127.0.0.1:${address.port}` });
});

process.stdin.setEncoding("utf8");
let buffer = "";
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line) continue;
    const command = JSON.parse(line);
    if (command.command === "status") {
      writeJson({
        messages: metrics.messages,
        bytes: metrics.bytes,
        errors: metrics.errors,
        peak_in_flight: metrics.peakInFlight,
        latencies: [],
        callback_messages: metrics.messages,
        callback_latencies: metrics.latencies,
      });
    } else if (command.command === "stop") {
      writeJson({ ok: true });
      server.close(() => process.exit(0));
    } else {
      writeJson({ error: `unknown command: ${command.command}` });
    }
  }
});
"""


async def _prepare_js_client_harness() -> Path:
    await to_thread.run_sync(_prepare_js_client_harness_sync)
    return JS_CLIENT_SCRIPT


def _prepare_js_client_harness_sync() -> None:
    JS_CLIENT_ROOT.mkdir(parents=True, exist_ok=True)
    package_json = JS_CLIENT_ROOT / "package.json"
    if not package_json.exists():
        package_json.write_text(
            json.dumps(
                {
                    "private": True,
                    "type": "module",
                    "dependencies": {"@vercel/queue": "latest"},
                },
                indent=2,
            )
            + "\n"
        )
    JS_CLIENT_SCRIPT.write_text(JS_CLIENT_HARNESS.lstrip() + "\n")
    node_modules = JS_CLIENT_ROOT / "node_modules" / "@vercel" / "queue"
    if node_modules.exists():
        return
    npm = os.environ.get("NPM", "npm")
    subprocess.run(  # noqa: S603
        [npm, "install", "--silent", "--no-audit", "--no-fund"],
        cwd=JS_CLIENT_ROOT,
        check=True,
    )


class _HttpPushCallbackDispatcher:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        self._client = httpx.AsyncClient(timeout=30)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.aclose()

    async def accept_and_handle(self, raw_body: bytes, headers: Mapping[str, str]) -> None:
        response = await self._client.post(self._base_url, content=raw_body, headers=headers)
        response.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()


async def _run_split_benchmark(
    config: BenchmarkConfig,
    *,
    profile_dir: Path | None = None,
) -> BenchmarkResult:
    if config.delivery != "push":
        raise ValueError("split architecture currently supports push delivery only")
    total_started = time.perf_counter()
    clear_subscriptions()
    split_processes = await _SplitProcesses.open(config, profile_dir=profile_dir)
    async with split_processes as processes:
        client = QueueClient(
            token=LOCAL_TOKEN,
            region="iad1",
            base_url=processes.queue_base_url,
            deployment=ALL_DEPLOYMENTS,
        )
        send = await _send_messages(client, config)
        receive_ack = await _wait_for_split_push_messages(
            processes,
            config,
            timeout_seconds=300.0 if profile_dir is not None else 30.0,
        )
    total_seconds = time.perf_counter() - total_started
    try:
        return BenchmarkResult(
            scenario=config.scenario,
            config=config,
            total_seconds=total_seconds,
            send=send.phase,
            receive_ack=receive_ack.phase,
            errors=send.errors
            + receive_ack.errors
            + max(0, config.messages - receive_ack.phase.messages),
            peak_in_flight=max(send.peak_in_flight, receive_ack.peak_in_flight),
        )
    finally:
        clear_subscriptions()


@dataclass
class _SplitProcesses:
    queue: Process
    client: Process
    queue_base_url: str

    @classmethod
    async def open(
        cls,
        config: BenchmarkConfig,
        *,
        profile_dir: Path | None,
    ) -> _SplitProcessesContext:
        context = _SplitProcessesContext(config, profile_dir=profile_dir)
        await context.start()
        return context

    async def request_queue(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await _process_request(self.queue, payload)

    async def request_client(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await _process_request(self.client, payload)


class _SplitProcessesContext(_SplitProcesses):
    def __init__(self, config: BenchmarkConfig, *, profile_dir: Path | None) -> None:
        self._config = config
        self._profile_dir = profile_dir
        self.queue_base_url = ""

    async def start(self) -> None:
        profile_dir = self._profile_dir
        if profile_dir is not None:
            profile_dir.mkdir(parents=True, exist_ok=True)
        self.queue = await _start_role_process(
            self._config,
            "split-queue",
            profile=(profile_dir / "queue.prof" if profile_dir is not None else None),
            queue_server_profile=(
                profile_dir / "queue-server.prof" if profile_dir is not None else None
            ),
        )
        queue_ready = await _read_process_json(self.queue)
        if self._config.client == "js":
            self.client = await _start_js_client_process(
                self._config,
                queue_base_url=str(queue_ready["baseUrl"]),
            )
        else:
            self.client = await _start_role_process(
                self._config,
                "split-client",
                queue_base_url=queue_ready["baseUrl"],
                profile=(profile_dir / "client.prof" if profile_dir is not None else None),
                client_server_profile=(
                    profile_dir / "client-server.prof" if profile_dir is not None else None
                ),
                lease_worker_profile=(
                    profile_dir / "client-lease-worker.prof" if profile_dir is not None else None
                ),
            )
        client_ready = await _read_process_json(self.client)
        self.queue_base_url = str(queue_ready["baseUrl"])
        await self.request_queue({"command": "set_callback", "baseUrl": client_ready["baseUrl"]})

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        for proc in (self.queue, self.client):
            try:
                await _process_request(proc, {"command": "stop"})
            except RuntimeError:
                proc.terminate()
        with anyio.move_on_after(5):
            await self.queue.wait()
            await self.client.wait()
        for proc in (self.queue, self.client):
            if proc.returncode is None:
                proc.kill()
                await proc.wait()


async def _start_role_process(
    config: BenchmarkConfig,
    role: str,
    *,
    queue_base_url: str | None = None,
    profile: Path | None = None,
    queue_server_profile: Path | None = None,
    client_server_profile: Path | None = None,
    lease_worker_profile: Path | None = None,
) -> Process:
    env = os.environ.copy()
    command = [
        sys.executable,
        MODULE_PATH,
        "--scenario",
        config.scenario,
        "--delivery",
        config.delivery,
        "--architecture",
        config.architecture,
        "--client",
        config.client,
        "--messages",
        str(config.messages),
        "--concurrency",
        str(config.concurrency),
        "--small-size",
        str(config.small_payload_bytes),
        "--large-size",
        str(config.large_payload_bytes),
        "--chunk-size",
        str(config.chunk_size),
    ]
    env[ENV_SPLIT_ROLE] = role
    if queue_base_url is not None:
        env[ENV_QUEUE_BASE_URL] = queue_base_url
    if profile is not None:
        command.extend(["--profile", str(profile)])
    if queue_server_profile is not None:
        env[ENV_QUEUE_SERVER_PROFILE] = str(queue_server_profile)
    if client_server_profile is not None:
        env[ENV_CLIENT_SERVER_PROFILE] = str(client_server_profile)
    if lease_worker_profile is not None:
        env[ENV_LEASE_WORKER_PROFILE] = str(lease_worker_profile)
    return await anyio.open_process(command, env=env)


async def _start_js_client_process(
    config: BenchmarkConfig,
    *,
    queue_base_url: str,
) -> Process:
    script = await _prepare_js_client_harness()
    env = os.environ.copy()
    env[ENV_QUEUE_BASE_URL] = queue_base_url
    env[ENV_BENCH_CONFIG] = _config_env(config)
    return await anyio.open_process(["node", str(script)], cwd=JS_CLIENT_ROOT, env=env)


async def _read_process_json(process: Process) -> dict[str, Any]:
    stdout = process.stdout
    if stdout is None:
        raise RuntimeError("role process stdout is unavailable")
    line = bytearray()
    while True:
        chunk = await stdout.receive(1)
        if not chunk:
            raise RuntimeError("role process closed stdout before sending JSON")
        if chunk == ROLE_NEWLINE:
            break
        line.extend(chunk)
    return cast("dict[str, Any]", json.loads(line.decode()))


async def _process_request(
    process: Process,
    payload: dict[str, Any],
) -> dict[str, Any]:
    stdin = process.stdin
    if stdin is None:
        raise RuntimeError("role process stdin is unavailable")
    await stdin.send(json.dumps(payload).encode() + ROLE_NEWLINE)
    return await _read_process_json(process)


async def _wait_for_split_push_messages(
    processes: _SplitProcesses,
    config: BenchmarkConfig,
    *,
    timeout_seconds: float = 30.0,
) -> _ReceiveResult:
    started = time.perf_counter()
    deadline = started + timeout_seconds
    client_status: dict[str, Any] = {}
    queue_status: dict[str, Any] = {}
    while time.perf_counter() < deadline:
        client_status = await processes.request_client({"command": "status"})
        queue_status = await processes.request_queue({"command": "status"})
        if (
            client_status["callback_messages"] >= config.messages
            and queue_status["acknowledged"] >= config.messages
        ):
            break
        await anyio.sleep(0.01)
    elapsed = time.perf_counter() - started
    latencies = cast("list[float]", client_status.get("callback_latencies", []))
    if not latencies:
        latencies = cast("list[float]", client_status.get("latencies", []))
    return _ReceiveResult(
        phase=PhaseResult(
            seconds=elapsed,
            messages=int(client_status.get("messages", 0)),
            bytes=int(client_status.get("bytes", 0)),
            latency=LatencySummary.from_samples(latencies),
        ),
        errors=int(client_status.get("errors", 0)) + int(queue_status.get("errors", 0)),
        peak_in_flight=max(
            int(client_status.get("peak_in_flight", 0)),
            int(queue_status.get("peak_in_flight", 0)),
        ),
    )


def _run_split_queue_role(args: argparse.Namespace) -> int:
    anyio.run(_run_split_queue_role_async, args)
    return 0


async def _run_split_queue_role_async(args: argparse.Namespace) -> None:
    config = configs_from_args(args)[0]
    callback_dispatcher: _HttpPushCallbackDispatcher | None = None
    async with anyio.create_task_group() as task_group, AsyncExitStack() as exit_stack:
        with embedded_queue_dev_server(
            manual_clock=False,
            profile=os.environ.get(ENV_QUEUE_SERVER_PROFILE),
        ) as queue_server:

            def callback_dispatcher_factory() -> _HttpPushCallbackDispatcher:
                if callback_dispatcher is None:
                    raise RuntimeError("split callback dispatcher is not registered")
                return callback_dispatcher

            dispatcher = EmbeddedQueueDispatcher(
                queue_server.app.server,
                callback_dispatcher_factory,
            )
            task_group.start_soon(dispatcher.run, name="vercel-split-queue-dispatcher")
            await _write_role_json({"baseUrl": queue_server.base_url})
            try:
                while True:
                    command = await _read_role_command()
                    if command["command"] == "set_callback":
                        callback_dispatcher = await exit_stack.enter_async_context(
                            _HttpPushCallbackDispatcher(str(command["baseUrl"]))
                        )
                        dispatcher.register_subscription(
                            topic=TOPIC,
                            consumer_group=CONSUMER_GROUP,
                            retry_after_seconds=None,
                            initial_delay_seconds=None,
                            max_concurrency=config.concurrency,
                            max_attempts=None,
                        )
                        dispatcher.wake()
                        await _write_role_json({"ok": True})
                    elif command["command"] == "status":
                        await _write_role_json(_split_queue_status(queue_server, dispatcher))
                    elif command["command"] == "stop":
                        await _write_role_json({"ok": True})
                        return
                    else:
                        await _write_role_json({"error": f"unknown command: {command['command']}"})
            finally:
                task_group.cancel_scope.cancel()
                dispatcher.unregister()
                await dispatcher.aclose()


def _run_split_client_role(args: argparse.Namespace) -> int:
    anyio.run(_run_split_client_role_async, args)
    return 0


async def _run_split_client_role_async(args: argparse.Namespace) -> None:
    queue_base_url = os.environ.get(ENV_QUEUE_BASE_URL)
    if queue_base_url is None:
        raise ValueError(f"split client role requires {ENV_QUEUE_BASE_URL}")
    config = configs_from_args(args)[0]
    state = _PushState()
    metrics = _SplitCallbackMetrics()
    _register_split_client_handler(config, state)
    restore_lease_worker = _profile_lease_renewal_worker(os.environ.get(ENV_LEASE_WORKER_PROFILE))
    client = _BenchmarkPushClient(
        metrics=metrics,
        token=LOCAL_TOKEN,
        region="iad1",
        base_url=queue_base_url,
        deployment=ALL_DEPLOYMENTS,
    )
    try:
        with queue_client_asgi_dev_server(
            client=client,
            profile=os.environ.get(ENV_CLIENT_SERVER_PROFILE),
        ) as server:
            await _write_role_json({"baseUrl": server.base_url})
            while True:
                command = await _read_role_command()
                if command["command"] == "status":
                    await _write_role_json(_split_client_status(state, metrics))
                elif command["command"] == "stop":
                    await _write_role_json({"ok": True})
                    return
                else:
                    await _write_role_json({"error": f"unknown command: {command['command']}"})
    finally:
        reset_lease_renewal_worker_for_tests()
        restore_lease_worker()


def _register_split_client_handler(
    config: BenchmarkConfig,
    state: _PushState,
) -> None:
    if config.scenario == "large":

        @subscribe(topic=TOPIC, consumer_group=CONSUMER_GROUP, max_concurrency=config.concurrency)
        async def handle_large(payload: AsyncIterable[bytes]) -> None:
            await _handle_push_payload(state, config, payload)

        state.handlers.append(handle_large)
    else:

        @subscribe(topic=TOPIC, consumer_group=CONSUMER_GROUP, max_concurrency=config.concurrency)
        async def handle_small(payload: dict[str, Any]) -> None:
            await _handle_push_payload(state, config, payload)

        state.handlers.append(handle_small)


def _require_callback_base_url(base_url: str | None) -> str:
    if base_url is None:
        raise RuntimeError("split callback server is not registered")
    return base_url


def _split_queue_status(queue_server: Any, dispatcher: EmbeddedQueueDispatcher) -> dict[str, Any]:
    messages = [
        message
        for message in queue_server.app.server.state.messages
        if message.topic == TOPIC and message.payload != {"warm": True}
    ]
    acknowledged = sum(1 for message in messages if message.acknowledged_for(CONSUMER_GROUP))
    return {
        "messages": len(messages),
        "acknowledged": acknowledged,
        "errors": 0,
        "peak_in_flight": max(dispatcher._inflight_counts.values(), default=0),  # noqa: SLF001
    }


def _split_client_status(
    state: _PushState,
    metrics: _SplitCallbackMetrics,
) -> dict[str, Any]:
    with metrics.lock:
        callback_messages = metrics.messages
        callback_errors = metrics.errors
        callback_latencies = list(metrics.latencies)
    return {
        "messages": state.counters.messages,
        "bytes": state.counters.bytes,
        "errors": state.counters.errors + callback_errors,
        "peak_in_flight": state.peak_in_flight,
        "latencies": state.counters.latencies,
        "callback_messages": callback_messages,
        "callback_latencies": callback_latencies,
    }


async def _read_role_command() -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(await to_thread.run_sync(sys.stdin.readline)))


async def _write_role_json(payload: dict[str, Any]) -> None:
    line = json.dumps(payload, separators=(",", ":")) + "\n"
    await to_thread.run_sync(sys.stdout.write, line)
    await to_thread.run_sync(sys.stdout.flush)


@dataclass(frozen=True)
class _SendResult:
    phase: PhaseResult
    errors: int
    peak_in_flight: int


async def _send_messages(client: QueueClient, config: BenchmarkConfig) -> _SendResult:
    state = _SendState()
    lock = anyio.Lock()
    started = time.perf_counter()

    async def worker() -> None:
        while True:
            async with lock:
                if state.next_message >= config.messages:
                    return
                message_index = state.next_message
                state.next_message += 1
                state.in_flight += 1
                state.peak_in_flight = max(state.peak_in_flight, state.in_flight)

            byte_count = _payload_size(config)
            op_started = time.perf_counter()
            try:
                if config.scenario == "small":
                    await client.send(
                        TOPIC,
                        _small_payload(message_index, config.small_payload_bytes),
                    )
                else:
                    await client.send(
                        Topic[AsyncIterable[bytes]](
                            TOPIC,
                            transport=cast("Any", ByteStreamTransport()),
                        ),
                        _large_stream(config.large_payload_bytes, config.chunk_size),
                    )
            except Exception:
                async with lock:
                    state.counters.errors += 1
                    state.in_flight -= 1
                raise
            latency = time.perf_counter() - op_started

            async with lock:
                state.counters.messages += 1
                state.counters.bytes += byte_count
                state.counters.latencies.append(latency)
                state.in_flight -= 1

    async with anyio.create_task_group() as task_group:
        for _ in range(min(config.concurrency, config.messages)):
            task_group.start_soon(worker)

    elapsed = time.perf_counter() - started
    return _SendResult(
        phase=PhaseResult(
            seconds=elapsed,
            messages=state.counters.messages,
            bytes=state.counters.bytes,
            latency=LatencySummary.from_samples(state.counters.latencies),
        ),
        errors=state.counters.errors,
        peak_in_flight=state.peak_in_flight,
    )


async def _receive_and_ack_messages(
    client: QueueClient,
    config: BenchmarkConfig,
) -> _ReceiveResult:
    state = _ReceiveState()
    lock = anyio.Lock()
    started = time.perf_counter()
    poll_topic: str | Topic[AsyncIterable[bytes]] = TOPIC
    if config.scenario == "large":
        poll_topic = Topic[AsyncIterable[bytes]](TOPIC, transport=ByteStreamTransport())

    async def worker() -> None:
        while True:
            async with lock:
                if state.received >= config.messages:
                    return
                state.in_flight += 1
                state.peak_in_flight = max(state.peak_in_flight, state.in_flight)

            messages_seen = 0
            bytes_seen = 0
            op_started = time.perf_counter()
            try:
                async for delivery in client.poll(
                    poll_topic,
                    CONSUMER_GROUP,
                    limit=config.poll_limit,
                ):
                    async with delivery as message:
                        message_bytes = await _message_bytes(
                            message.payload,
                            scenario=config.scenario,
                        )
                    bytes_seen += message_bytes
                    messages_seen += 1
            except Exception:
                async with lock:
                    state.counters.errors += 1
                    state.in_flight -= 1
                raise
            latency = time.perf_counter() - op_started

            async with lock:
                state.received += messages_seen
                state.counters.messages += messages_seen
                state.counters.bytes += bytes_seen
                if messages_seen:
                    state.counters.latencies.append(latency / messages_seen)
                state.in_flight -= 1

    async with anyio.create_task_group() as task_group:
        for _ in range(min(config.concurrency, config.messages)):
            task_group.start_soon(worker)

    elapsed = time.perf_counter() - started
    return _ReceiveResult(
        phase=PhaseResult(
            seconds=elapsed,
            messages=state.counters.messages,
            bytes=state.counters.bytes,
            latency=LatencySummary.from_samples(state.counters.latencies),
        ),
        errors=state.counters.errors,
        peak_in_flight=state.peak_in_flight,
    )


def _register_push_handler(config: BenchmarkConfig) -> _ReceiveResult:
    state = _PushState()
    started = time.perf_counter()

    if config.scenario == "large":

        @subscribe(topic=TOPIC, consumer_group=CONSUMER_GROUP, max_concurrency=config.concurrency)
        async def handle_large(payload: AsyncIterable[bytes]) -> None:
            await _handle_push_payload(state, config, payload)

        state.handlers.append(handle_large)
    else:

        @subscribe(topic=TOPIC, consumer_group=CONSUMER_GROUP, max_concurrency=config.concurrency)
        async def handle_small(payload: dict[str, Any]) -> None:
            await _handle_push_payload(state, config, payload)

        state.handlers.append(handle_small)

    return _ReceiveResult(
        phase=PhaseResult(
            seconds=0.0,
            messages=0,
            bytes=0,
            latency=LatencySummary.from_samples([]),
        ),
        errors=0,
        peak_in_flight=0,
        state=state,
        started=started,
    )


async def _handle_push_payload(
    state: _PushState,
    config: BenchmarkConfig,
    payload: Any,
) -> None:
    state.in_flight += 1
    state.peak_in_flight = max(state.peak_in_flight, state.in_flight)

    op_started = time.perf_counter()
    try:
        byte_count = await _message_bytes(payload, scenario=config.scenario)
    except Exception:
        state.counters.errors += 1
        state.in_flight -= 1
        if state.handled >= config.messages:
            state.done.set()
        raise
    latency = time.perf_counter() - op_started

    state.handled += 1
    state.counters.messages += 1
    state.counters.bytes += byte_count
    state.counters.latencies.append(latency)
    state.in_flight -= 1
    if state.handled >= config.messages:
        state.done.set()


async def _wait_for_push_messages(
    service: Any,
    receive_ack: _ReceiveResult,
    config: BenchmarkConfig,
    metrics: _SplitCallbackMetrics,
) -> _ReceiveResult:
    if receive_ack.state is None or receive_ack.started is None:
        raise RuntimeError("push receive state was not initialized")
    with anyio.fail_after(30):
        await receive_ack.state.done.wait()
        while _callback_message_count(
            metrics
        ) < config.messages or not _all_push_messages_acknowledged(service, config):
            await checkpoint()
    elapsed = time.perf_counter() - receive_ack.started
    state = receive_ack.state
    callback_messages, callback_errors, callback_latencies = _callback_metrics(metrics)
    receive_ack.phase = PhaseResult(
        seconds=elapsed,
        messages=state.counters.messages,
        bytes=state.counters.bytes,
        latency=LatencySummary.from_samples(callback_latencies),
    )
    receive_ack.errors = state.counters.errors + callback_errors
    receive_ack.peak_in_flight = state.peak_in_flight
    if callback_messages < config.messages:
        receive_ack.errors += config.messages - callback_messages
    return receive_ack


def _callback_metrics(metrics: _SplitCallbackMetrics) -> tuple[int, int, list[float]]:
    with metrics.lock:
        return metrics.messages, metrics.errors, list(metrics.latencies)


def _callback_message_count(metrics: _SplitCallbackMetrics) -> int:
    with metrics.lock:
        return metrics.messages


def _all_push_messages_acknowledged(service: Any, config: BenchmarkConfig) -> bool:
    messages = [
        message
        for message in service.server.state.messages
        if message.topic == TOPIC and message.payload != {"warm": True}
    ]
    return len(messages) >= config.messages and all(
        message.acknowledged_for(CONSUMER_GROUP) for message in messages
    )


async def _warm_up(client: QueueClient) -> None:
    await client.send(TOPIC, {"warm": True})
    delivery: Delivery[Any]
    async for delivery in client.poll(TOPIC, CONSUMER_GROUP):
        async with delivery:
            pass


async def _large_stream(total_bytes: int, chunk_size: int) -> AsyncIterator[bytes]:
    remaining = total_bytes
    chunk = b"x" * min(chunk_size, total_bytes)
    while remaining > 0:
        size = min(len(chunk), remaining)
        yield chunk[:size]
        remaining -= size
        await checkpoint()


async def _consume_stream(payload: Any) -> int:
    if not hasattr(payload, "__aiter__"):
        raise TypeError(f"expected async stream payload, got {type(payload).__name__}")
    total = 0
    async for chunk in payload:
        total += len(chunk)
    return total


async def _message_bytes(payload: Any, *, scenario: ScenarioName) -> int:
    if scenario == "large":
        return await _consume_stream(payload)
    return _json_payload_size(payload)


def _small_payload(index: int, size: int) -> dict[str, Any]:
    return {"index": index, "body": "x" * size}


def _json_payload_size(payload: object) -> int:
    return len(json.dumps(payload).encode("utf-8"))


def _payload_size(config: BenchmarkConfig) -> int:
    if config.scenario == "large":
        return config.large_payload_bytes
    return _json_payload_size(_small_payload(0, config.small_payload_bytes))


def _validate_config(config: BenchmarkConfig) -> None:
    _positive_int(config.messages)
    _positive_int(config.concurrency)
    _poll_limit(config.poll_limit)
    _positive_int(config.small_payload_bytes)
    _positive_int(config.large_payload_bytes)
    _positive_int(config.chunk_size)
    if config.client == "js" and (config.architecture != "split" or config.delivery != "push"):
        raise ValueError("--client js requires --architecture split --delivery push")


def _config_env(config: BenchmarkConfig) -> str:
    return json.dumps(
        {
            "scenario": config.scenario,
            "delivery": config.delivery,
            "messages": config.messages,
            "concurrency": config.concurrency,
            "poll_limit": config.poll_limit,
            "small_payload_bytes": config.small_payload_bytes,
            "large_payload_bytes": config.large_payload_bytes,
            "chunk_size": config.chunk_size,
            "architecture": config.architecture,
            "client": config.client,
        },
        separators=(",", ":"),
    )


def _positive_int(value: str | int) -> int:
    try:
        parsed = value if isinstance(value, int) else int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _poll_limit(value: str | int) -> int:
    parsed = _positive_int(value)
    if parsed > 10:
        raise argparse.ArgumentTypeError("must be between 1 and 10")
    return parsed


def _receive_phase_name(delivery: DeliveryMode) -> str:
    if delivery == "push":
        return "push+ack"
    return "receive+ack"


def _percentile(sorted_samples: Sequence[float], percentile: float) -> float:
    if len(sorted_samples) == 1:
        return sorted_samples[0]
    position = (len(sorted_samples) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(sorted_samples) - 1)
    weight = position - lower
    return sorted_samples[lower] * (1 - weight) + sorted_samples[upper] * weight


def _rate(value: float, seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    return value / seconds


def _seconds(value: float) -> str:
    return f"{value:.3f}s"


def _rate_text(value: float, unit: str) -> str:
    return f"{value:,.2f} {unit}"


def _format_phase(name: str, phase: PhaseResult) -> str:
    return (
        f"{name}: {_seconds(phase.seconds)} "
        f"({_rate_text(phase.messages_per_second, 'msg/s')}, "
        f"{_rate_text(phase.mib_per_second, 'MiB/s')}, "
        f"p50 {phase.latency.median_seconds * 1000:.2f}ms, "
        f"p95 {phase.latency.p95_seconds * 1000:.2f}ms, "
        f"p99 {phase.latency.p99_seconds * 1000:.2f}ms)"
    )


if __name__ == "__main__":
    raise SystemExit(main())
