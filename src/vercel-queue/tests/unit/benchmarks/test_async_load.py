from __future__ import annotations

from contextlib import redirect_stderr
from io import StringIO

import pytest

from benchmarks.async_load import (
    BENCHMARK_ROOT,
    JS_CLIENT_ROOT,
    BenchmarkConfig,
    BenchmarkResult,
    LatencySummary,
    PhaseResult,
    configs_from_args,
    format_text,
    parse_args,
    run_benchmark,
)


def test_parse_args_accepts_scenarios() -> None:
    args = parse_args([
        "--scenario",
        "small",
        "--delivery",
        "push",
        "--messages",
        "12",
        "--concurrency",
        "3",
        "--poll-limit",
        "4",
        "--architecture",
        "split",
        "--client",
        "js",
    ])

    configs = configs_from_args(args)

    assert configs == [
        BenchmarkConfig(
            scenario="small",
            delivery="push",
            messages=12,
            concurrency=3,
            poll_limit=4,
            architecture="split",
            client="js",
        )
    ]


def test_parse_args_rejects_invalid_poll_limit() -> None:
    stderr = StringIO()
    with redirect_stderr(stderr), pytest.raises(SystemExit):
        parse_args(["--poll-limit", "11"])
    assert "--poll-limit" in stderr.getvalue()


def test_js_client_requires_split_push() -> None:
    with pytest.raises(ValueError, match="--client js"):
        BenchmarkConfig(scenario="small", messages=1, client="js")


def test_benchmark_state_uses_project_benchmarks_root() -> None:
    assert BENCHMARK_ROOT.name == ".benchmarks"
    assert JS_CLIENT_ROOT == BENCHMARK_ROOT / "js-client"


async def test_small_message_benchmark_completes(anyio_backend: str) -> None:
    result = await run_benchmark(
        BenchmarkConfig(
            scenario="small",
            messages=5,
            concurrency=2,
            poll_limit=2,
            small_payload_bytes=8,
        )
    )

    assert result.scenario == "small"
    assert result.send.messages == 5
    assert result.receive_ack.messages == 5
    assert result.send.bytes > 0
    assert result.receive_ack.bytes > 0
    assert result.errors == 0
    assert result.peak_in_flight <= 2
    assert "scenario: small" in format_text([result])


async def test_large_stream_benchmark_consumes_payloads(anyio_backend: str) -> None:
    result = await run_benchmark(
        BenchmarkConfig(
            scenario="large",
            messages=3,
            concurrency=2,
            poll_limit=2,
            large_payload_bytes=128,
            chunk_size=17,
        )
    )

    assert result.scenario == "large"
    assert result.send.messages == 3
    assert result.receive_ack.messages == 3
    assert result.send.bytes == 384
    assert result.receive_ack.bytes == 384
    assert result.errors == 0


async def test_small_push_benchmark_completes(anyio_backend: str) -> None:
    result = await run_benchmark(
        BenchmarkConfig(
            scenario="small",
            delivery="push",
            messages=5,
            concurrency=2,
            small_payload_bytes=8,
        )
    )

    assert result.config.delivery == "push"
    assert result.send.messages == 5
    assert result.receive_ack.messages == 5
    assert result.receive_ack.bytes > 0
    assert result.errors == 0
    assert "delivery: push" in format_text([result])


async def test_large_push_benchmark_consumes_payloads(anyio_backend: str) -> None:
    result = await run_benchmark(
        BenchmarkConfig(
            scenario="large",
            delivery="push",
            messages=3,
            concurrency=2,
            large_payload_bytes=128,
            chunk_size=17,
        )
    )

    assert result.config.delivery == "push"
    assert result.send.messages == 3
    assert result.receive_ack.messages == 3
    assert result.send.bytes == 384
    assert result.receive_ack.bytes == 384
    assert result.errors == 0


def test_json_summary_has_stable_metric_keys() -> None:
    config = BenchmarkConfig(scenario="large", messages=2)
    phase = PhaseResult(
        seconds=0.25,
        messages=2,
        bytes=1024 * 1024,
        latency=LatencySummary.from_samples([0.1, 0.2]),
    )
    result = BenchmarkResult(
        scenario="large",
        config=config,
        total_seconds=0.5,
        send=phase,
        receive_ack=phase,
        errors=0,
        peak_in_flight=1,
    )
    summary = result.to_json()

    assert set(summary) == {
        "config",
        "errors",
        "messages_per_second",
        "mib_per_second",
        "peak_in_flight",
        "receive_ack",
        "scenario",
        "send",
        "total_seconds",
    }
    assert summary["config"]["delivery"] == "pull"
    assert summary["config"]["architecture"] == "in-process"
    assert summary["config"]["client"] == "python"
    assert set(summary["send"]) == {
        "bytes",
        "latency",
        "messages",
        "messages_per_second",
        "mib_per_second",
        "seconds",
    }
    assert set(summary["send"]["latency"]) == {
        "max_seconds",
        "median_seconds",
        "min_seconds",
        "p95_seconds",
        "p99_seconds",
    }
    assert summary["mib_per_second"] == pytest.approx(2.0)
    assert summary["send"]["mib_per_second"] == pytest.approx(4.0)
