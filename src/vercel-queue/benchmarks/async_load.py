from __future__ import annotations

from typing import Any, Literal, cast

import argparse
import cProfile
import json
import pstats
import sys
import threading
import time
from collections.abc import AsyncIterable, AsyncIterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import anyio
from anyio.lowlevel import checkpoint

from vercel.queue import (
    ByteStreamTransport,
    Delivery,
    QueueClient,
    Topic,
    subscribe,
)
from vercel.queue.embedded import embedded_queue_service
from vercel.queue.testing import clear_subscriptions

ScenarioName = Literal["small", "large"]
DeliveryMode = Literal["pull", "push"]
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
class _CallbackMetrics:
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
    def __init__(self, *, metrics: _CallbackMetrics, **kwargs: Any) -> None:
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
    return parser.parse_args(argv)


async def run_benchmarks(
    configs: Sequence[BenchmarkConfig],
) -> list[BenchmarkResult]:
    return [await run_benchmark(config) for config in configs]


async def run_benchmark(
    config: BenchmarkConfig,
) -> BenchmarkResult:
    _validate_config(config)
    total_started = time.perf_counter()
    clear_subscriptions()
    metrics = _CallbackMetrics()
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
                )
            )
    return configs


def format_text(results: Sequence[BenchmarkResult]) -> str:
    return "\n\n".join(
        "\n".join([
            f"scenario: {result.scenario}",
            f"delivery: {result.config.delivery}",
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
    configs = configs_from_args(args)

    if args.profile is None:
        results = _run_configs_sync(configs, args.backend)
    else:
        profiler = cProfile.Profile()
        results = profiler.runcall(
            _run_configs_sync,
            configs,
            args.backend,
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


def _run_configs_sync(
    configs: Sequence[BenchmarkConfig],
    backend: str,
) -> list[BenchmarkResult]:
    return anyio.run(run_benchmarks, configs, backend=backend)


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
    metrics: _CallbackMetrics,
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


def _callback_metrics(metrics: _CallbackMetrics) -> tuple[int, int, list[float]]:
    with metrics.lock:
        return metrics.messages, metrics.errors, list(metrics.latencies)


def _callback_message_count(metrics: _CallbackMetrics) -> int:
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
