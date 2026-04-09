#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib.metadata import version as package_version
from pathlib import Path
from threading import Event, Thread
from typing import Any

import httpx
from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Status, StatusCode

from vercel.sandbox import AsyncSandbox, AsyncSnapshot, SandboxStatus, SnapshotSource

try:
    VERCEL_VERSION = package_version("vercel")
except Exception:
    VERCEL_VERSION = "development"

SPINNER_FRAMES = "|/-\\"


@dataclass(slots=True)
class AttemptResult:
    attempt_id: str
    started_at: str
    duration_ms: float
    success: bool
    has_defects: bool
    failed_stage: str | None
    initial_sandbox_id: str | None
    initial_sandbox_status: str | None
    snapshot_id: str | None
    snapshot_status: str | None
    restored_sandbox_id: str | None
    restored_sandbox_status: str | None
    restored_source_snapshot_id: str | None
    file_checks: dict[str, Any]
    defects: list[dict[str, Any]]
    trace_id: str
    cleanup: dict[str, Any]
    error: dict[str, Any] | None


class ProbeInterrupted(Exception):
    def __init__(self, result: AttemptResult, reason: str) -> None:
        super().__init__(reason)
        self.result = result
        self.reason = reason


class AttemptSpinner:
    def __init__(self, *, attempt_id: str) -> None:
        self._stop = Event()
        self._thread = Thread(target=self._run, daemon=True)

    def start(self) -> None:
        sys.stdout.write(" ")
        sys.stdout.flush()
        self._thread.start()

    def stop(self, marker: str) -> None:
        self._stop.set()
        self._thread.join()
        sys.stdout.write(marker)
        sys.stdout.flush()

    def _run(self) -> None:
        index = 0
        while not self._stop.wait(0.1):
            frame = SPINNER_FRAMES[index % len(SPINNER_FRAMES)]
            sys.stdout.write(f"\b{frame}")
            sys.stdout.flush()
            index += 1
        sys.stdout.write("\b")
        sys.stdout.flush()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Probe the async sandbox snapshot lifecycle: AsyncSandbox.create -> "
            "snapshot -> AsyncSandbox.create(source=SnapshotSource(...)) with "
            "HTTPX OpenTelemetry tracing."
        )
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=10,
        help="Number of lifecycle attempts to run.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=120_000,
        help="Sandbox timeout passed to both create calls.",
    )
    parser.add_argument(
        "--snapshot-expiration-ms",
        type=int,
        default=86_400_000,
        help="Snapshot expiration passed to sandbox.snapshot().",
    )
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop created sandboxes and delete created snapshots.",
    )
    parser.add_argument(
        "--cleanup-blocking",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Wait for stop completion when cleanup is enabled.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="JSON report path. Defaults to a timestamped file in the repo root.",
    )
    parser.add_argument(
        "--max-time",
        type=float,
        help="Maximum total runtime in seconds before stopping after the current attempt.",
    )
    return parser.parse_args()


def _serialize_exception(exc: BaseException) -> dict[str, Any]:
    error: dict[str, Any] = {
        "type": type(exc).__name__,
        "module": type(exc).__module__,
        "message": str(exc),
        "repr": repr(exc),
    }

    if isinstance(exc, httpx.HTTPError):
        request = getattr(exc, "request", None)
        if request is not None:
            error["request"] = {"method": request.method, "url": str(request.url)}

    response = getattr(exc, "response", None)
    if response is not None:
        error["response"] = {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "url": str(response.request.url) if response.request is not None else None,
        }

    cause = exc.__cause__
    if cause is not None:
        error["cause"] = {
            "type": type(cause).__name__,
            "module": type(cause).__module__,
            "message": str(cause),
            "repr": repr(cause),
        }

    return error


def _hex_trace_id(trace_id: int) -> str:
    return f"{trace_id:032x}"


def _attempt_marker(attempt: AttemptResult) -> str:
    if attempt.success:
        return "D" if attempt.has_defects else "."
    return "F" if attempt.has_defects else "f"


async def _record_cleanup_step(cleanup: dict[str, Any], key: str, func: Any) -> None:
    cleanup[key] = {"attempted": True, "succeeded": False, "error": None}
    try:
        result = func()
        if asyncio.iscoroutine(result):
            await result
        cleanup[key]["succeeded"] = True
    except Exception as exc:
        cleanup[key]["error"] = _serialize_exception(exc)


async def _run_attempt(
    *,
    attempt_number: int,
    tracer: trace.Tracer,
    args: argparse.Namespace,
) -> AttemptResult:
    attempt_id = f"attempt-{attempt_number:03d}"
    started_at = datetime.now(UTC).isoformat()
    spinner = AttemptSpinner(attempt_id=attempt_id)
    cleanup: dict[str, Any] = {
        "initial_sandbox_stop": {"attempted": False, "succeeded": False, "error": None},
        "restored_sandbox_stop": {"attempted": False, "succeeded": False, "error": None},
        "snapshot_delete": {"attempted": False, "succeeded": False, "error": None},
        "clients_closed": {
            "initial_sandbox": {"attempted": False, "succeeded": False, "error": None},
            "restored_sandbox": {"attempted": False, "succeeded": False, "error": None},
            "snapshot": {"attempted": False, "succeeded": False, "error": None},
        },
    }
    initial_file_path = "probe-initial.txt"
    restored_file_path = "probe-restored.txt"
    initial_file_content = f"{attempt_id}: initial sandbox file\n".encode()
    restored_file_content = f"{attempt_id}: restored sandbox file\n".encode()
    defects: list[dict[str, Any]] = []
    file_checks: dict[str, Any] = {
        "initial_before_snapshot": {
            "path": initial_file_path,
            "written": False,
            "read_back": None,
            "matches_expected": False,
            "error": None,
        },
        "restored_after_resume": {
            "path": restored_file_path,
            "written": False,
            "read_back": None,
            "matches_expected": False,
            "error": None,
        },
        "initial_after_resume": {
            "path": initial_file_path,
            "read_back": None,
            "matches_expected": False,
            "error": None,
        },
    }

    initial_sandbox: AsyncSandbox | None = None
    restored_sandbox: AsyncSandbox | None = None
    snapshot: AsyncSnapshot | None = None
    failed_stage: str | None = None

    with tracer.start_as_current_span(
        "sandbox.create.snapshot.restore.probe.attempt",
        attributes={"probe.attempt_id": attempt_id, "probe.attempt_number": attempt_number},
    ) as span:
        spinner.start()
        trace_id = _hex_trace_id(span.get_span_context().trace_id)
        started_monotonic = time.perf_counter()
        try:
            failed_stage = "initial_create"
            initial_sandbox = await AsyncSandbox.create(timeout=args.timeout_ms)
            await initial_sandbox.wait_for_status(SandboxStatus.RUNNING)
            span.set_attribute("sandbox.initial.id", initial_sandbox.sandbox_id)

            try:
                await initial_sandbox.write_files(
                    [{"path": initial_file_path, "content": initial_file_content}]
                )
                file_checks["initial_before_snapshot"]["written"] = True
            except Exception as exc:
                serialized = _serialize_exception(exc)
                file_checks["initial_before_snapshot"]["error"] = serialized
                defects.append({"stage": "initial_file_write", "error": serialized})
                span.record_exception(exc)

            try:
                initial_read = await initial_sandbox.read_file(initial_file_path)
                initial_text = None if initial_read is None else initial_read.decode("utf-8")
                file_checks["initial_before_snapshot"]["read_back"] = initial_text
                file_checks["initial_before_snapshot"]["matches_expected"] = (
                    initial_read == initial_file_content
                )
                if initial_read != initial_file_content:
                    defects.append(
                        {
                            "stage": "initial_file_read",
                            "error": {
                                "type": "ContentMismatch",
                                "message": "Initial file content did not match expected bytes",
                            },
                        }
                    )
            except Exception as exc:
                serialized = _serialize_exception(exc)
                file_checks["initial_before_snapshot"]["error"] = serialized
                defects.append({"stage": "initial_file_read", "error": serialized})
                span.record_exception(exc)
            span.set_attribute(
                "probe.initial_file.matches_expected",
                file_checks["initial_before_snapshot"]["matches_expected"],
            )

            failed_stage = "snapshot_create"
            snapshot = await initial_sandbox.snapshot(expiration=args.snapshot_expiration_ms)
            await initial_sandbox.wait_for_status(SandboxStatus.STOPPED)
            span.set_attribute("snapshot.id", snapshot.snapshot_id)

            failed_stage = "restore_create"
            restored_sandbox = await AsyncSandbox.create(
                timeout=args.timeout_ms,
                source=SnapshotSource(snapshot_id=snapshot.snapshot_id),
            )
            await restored_sandbox.wait_for_status(SandboxStatus.RUNNING)
            span.set_attribute("sandbox.restored.id", restored_sandbox.sandbox_id)

            try:
                await restored_sandbox.write_files(
                    [{"path": restored_file_path, "content": restored_file_content}]
                )
                file_checks["restored_after_resume"]["written"] = True
            except Exception as exc:
                serialized = _serialize_exception(exc)
                file_checks["restored_after_resume"]["error"] = serialized
                defects.append({"stage": "restored_file_write", "error": serialized})
                span.record_exception(exc)

            try:
                restored_read = await restored_sandbox.read_file(restored_file_path)
                restored_text = None if restored_read is None else restored_read.decode("utf-8")
                file_checks["restored_after_resume"]["read_back"] = restored_text
                file_checks["restored_after_resume"]["matches_expected"] = (
                    restored_read == restored_file_content
                )
                if restored_read != restored_file_content:
                    defects.append(
                        {
                            "stage": "restored_file_read",
                            "error": {
                                "type": "ContentMismatch",
                                "message": "Restored file content did not match expected bytes",
                            },
                        }
                    )
            except Exception as exc:
                serialized = _serialize_exception(exc)
                file_checks["restored_after_resume"]["error"] = serialized
                defects.append({"stage": "restored_file_read", "error": serialized})
                span.record_exception(exc)

            try:
                restored_initial_read = await restored_sandbox.read_file(initial_file_path)
                restored_initial_text = (
                    None if restored_initial_read is None else restored_initial_read.decode("utf-8")
                )
                file_checks["initial_after_resume"]["read_back"] = restored_initial_text
                file_checks["initial_after_resume"]["matches_expected"] = (
                    restored_initial_read == initial_file_content
                )
                if restored_initial_read != initial_file_content:
                    defects.append(
                        {
                            "stage": "restored_initial_file_read",
                            "error": {
                                "type": "ContentMismatch",
                                "message": "Initial file was not preserved in restored sandbox",
                            },
                        }
                    )
            except Exception as exc:
                serialized = _serialize_exception(exc)
                file_checks["initial_after_resume"]["error"] = serialized
                defects.append({"stage": "restored_initial_file_read", "error": serialized})
                span.record_exception(exc)
            span.set_attribute(
                "probe.restored_file.matches_expected",
                file_checks["restored_after_resume"]["matches_expected"],
            )
            span.set_attribute(
                "probe.initial_file_restored.matches_expected",
                file_checks["initial_after_resume"]["matches_expected"],
            )

            duration_ms = (time.perf_counter() - started_monotonic) * 1000
            result = AttemptResult(
                attempt_id=attempt_id,
                started_at=started_at,
                duration_ms=duration_ms,
                success=True,
                has_defects=bool(defects),
                failed_stage=None,
                initial_sandbox_id=initial_sandbox.sandbox_id,
                initial_sandbox_status=str(initial_sandbox.status),
                snapshot_id=snapshot.snapshot_id,
                snapshot_status=snapshot.status,
                restored_sandbox_id=restored_sandbox.sandbox_id,
                restored_sandbox_status=str(restored_sandbox.status),
                restored_source_snapshot_id=restored_sandbox.source_snapshot_id,
                file_checks=file_checks,
                defects=defects,
                trace_id=trace_id,
                cleanup=cleanup,
                error=None,
            )
            return result
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            duration_ms = (time.perf_counter() - started_monotonic) * 1000
            result = AttemptResult(
                attempt_id=attempt_id,
                started_at=started_at,
                duration_ms=duration_ms,
                success=False,
                has_defects=bool(defects),
                failed_stage=failed_stage,
                initial_sandbox_id=initial_sandbox.sandbox_id if initial_sandbox is not None else None,
                initial_sandbox_status=(
                    str(initial_sandbox.status) if initial_sandbox is not None else None
                ),
                snapshot_id=snapshot.snapshot_id if snapshot is not None else None,
                snapshot_status=snapshot.status if snapshot is not None else None,
                restored_sandbox_id=(
                    restored_sandbox.sandbox_id if restored_sandbox is not None else None
                ),
                restored_sandbox_status=(
                    str(restored_sandbox.status) if restored_sandbox is not None else None
                ),
                restored_source_snapshot_id=(
                    restored_sandbox.source_snapshot_id if restored_sandbox is not None else None
                ),
                file_checks=file_checks,
                defects=defects,
                trace_id=trace_id,
                cleanup=cleanup,
                error=_serialize_exception(exc),
            )
            return result
        except KeyboardInterrupt as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, "KeyboardInterrupt"))
            duration_ms = (time.perf_counter() - started_monotonic) * 1000
            result = AttemptResult(
                attempt_id=attempt_id,
                started_at=started_at,
                duration_ms=duration_ms,
                success=False,
                has_defects=bool(defects),
                failed_stage=failed_stage,
                initial_sandbox_id=initial_sandbox.sandbox_id if initial_sandbox is not None else None,
                initial_sandbox_status=(
                    str(initial_sandbox.status) if initial_sandbox is not None else None
                ),
                snapshot_id=snapshot.snapshot_id if snapshot is not None else None,
                snapshot_status=snapshot.status if snapshot is not None else None,
                restored_sandbox_id=(
                    restored_sandbox.sandbox_id if restored_sandbox is not None else None
                ),
                restored_sandbox_status=(
                    str(restored_sandbox.status) if restored_sandbox is not None else None
                ),
                restored_source_snapshot_id=(
                    restored_sandbox.source_snapshot_id if restored_sandbox is not None else None
                ),
                file_checks=file_checks,
                defects=defects,
                trace_id=trace_id,
                cleanup=cleanup,
                error=_serialize_exception(exc),
            )
            raise ProbeInterrupted(result, "KeyboardInterrupt") from None
        finally:
            result = locals().get("result")
            if args.cleanup:
                if restored_sandbox is not None:
                    await _record_cleanup_step(
                        cleanup,
                        "restored_sandbox_stop",
                        lambda: restored_sandbox.stop(blocking=args.cleanup_blocking),
                    )
                if snapshot is not None:
                    await _record_cleanup_step(cleanup, "snapshot_delete", snapshot.delete)
                if initial_sandbox is not None:
                    await _record_cleanup_step(
                        cleanup,
                        "initial_sandbox_stop",
                        lambda: initial_sandbox.stop(blocking=args.cleanup_blocking),
                    )

            if initial_sandbox is not None:
                await _record_cleanup_step(
                    cleanup["clients_closed"],
                    "initial_sandbox",
                    initial_sandbox.client.aclose,
                )
            if restored_sandbox is not None:
                await _record_cleanup_step(
                    cleanup["clients_closed"],
                    "restored_sandbox",
                    restored_sandbox.client.aclose,
                )
            if snapshot is not None:
                await _record_cleanup_step(
                    cleanup["clients_closed"],
                    "snapshot",
                    snapshot.client.aclose,
                )
            if result is None:
                duration_ms = (time.perf_counter() - started_monotonic) * 1000
                result = AttemptResult(
                    attempt_id=attempt_id,
                    started_at=started_at,
                    duration_ms=duration_ms,
                    success=False,
                    has_defects=bool(defects),
                    failed_stage=failed_stage,
                    initial_sandbox_id=(
                        initial_sandbox.sandbox_id if initial_sandbox is not None else None
                    ),
                    initial_sandbox_status=(
                        str(initial_sandbox.status) if initial_sandbox is not None else None
                    ),
                    snapshot_id=snapshot.snapshot_id if snapshot is not None else None,
                    snapshot_status=snapshot.status if snapshot is not None else None,
                    restored_sandbox_id=(
                        restored_sandbox.sandbox_id if restored_sandbox is not None else None
                    ),
                    restored_sandbox_status=(
                        str(restored_sandbox.status) if restored_sandbox is not None else None
                    ),
                    restored_source_snapshot_id=(
                        restored_sandbox.source_snapshot_id if restored_sandbox is not None else None
                    ),
                    file_checks=file_checks,
                    defects=defects,
                    trace_id=trace_id,
                    cleanup=cleanup,
                    error={
                        "type": "UnknownAttemptState",
                        "message": "Attempt completed without producing a result object",
                    },
                )
            spinner.stop(_attempt_marker(result))


def _simplify_span(span: Any) -> dict[str, Any]:
    parent_id = None if span.parent is None else f"{span.parent.span_id:016x}"
    status = getattr(span.status, "status_code", None)
    status_name = None if status is None else status.name
    return {
        "name": span.name,
        "trace_id": _hex_trace_id(span.context.trace_id),
        "span_id": f"{span.context.span_id:016x}",
        "parent_span_id": parent_id,
        "start_time_unix_nano": span.start_time,
        "end_time_unix_nano": span.end_time,
        "duration_ms": (span.end_time - span.start_time) / 1_000_000,
        "status_code": status_name,
        "status_description": getattr(span.status, "description", None),
        "attributes": dict(span.attributes),
        "events": [
            {
                "name": event.name,
                "timestamp_unix_nano": event.timestamp,
                "attributes": dict(event.attributes),
            }
            for event in span.events
        ],
    }


def _summarize_attempts(attempts: list[AttemptResult]) -> dict[str, Any]:
    stage_failures = Counter(
        attempt.failed_stage for attempt in attempts if attempt.failed_stage is not None
    )
    defect_stages = Counter(defect["stage"] for attempt in attempts for defect in attempt.defects)
    error_types = Counter(
        attempt.error["type"]
        for attempt in attempts
        if attempt.error is not None and "type" in attempt.error
    )
    defect_error_types = Counter(
        defect["error"]["type"]
        for attempt in attempts
        for defect in attempt.defects
        if "type" in defect["error"]
    )
    durations = [attempt.duration_ms for attempt in attempts]
    successes = sum(1 for attempt in attempts if attempt.success)
    attempts_with_defects = sum(1 for attempt in attempts if attempt.has_defects)
    return {
        "attempts": len(attempts),
        "successes": successes,
        "failures": len(attempts) - successes,
        "success_rate": successes / len(attempts) if attempts else 0.0,
        "attempts_with_defects": attempts_with_defects,
        "failed_stages": dict(stage_failures),
        "defect_stages": dict(defect_stages),
        "error_types": dict(error_types),
        "defect_error_types": dict(defect_error_types),
        "duration_ms": {
            "min": min(durations) if durations else 0.0,
            "max": max(durations) if durations else 0.0,
            "avg": sum(durations) / len(durations) if durations else 0.0,
        },
    }


def _summarize_spans(spans: list[dict[str, Any]]) -> dict[str, Any]:
    http_spans = []
    for span in spans:
        attributes = span["attributes"]
        method = attributes.get("http.method") or attributes.get("http.request.method")
        url = attributes.get("http.url") or attributes.get("url.full")
        status_code = attributes.get("http.status_code") or attributes.get("http.response.status_code")
        if method or url or status_code is not None:
            http_spans.append(span)

    by_endpoint: dict[str, dict[str, Any]] = {}
    status_counts: Counter[str] = Counter()
    for span in http_spans:
        attributes = span["attributes"]
        method = attributes.get("http.method") or attributes.get("http.request.method") or "UNKNOWN"
        target = (
            attributes.get("http.target")
            or attributes.get("url.path")
            or attributes.get("http.route")
            or attributes.get("url.full")
            or "unknown"
        )
        status_code = attributes.get("http.status_code") or attributes.get("http.response.status_code")
        key = f"{method} {target}"
        bucket = by_endpoint.setdefault(
            key,
            {"count": 0, "duration_ms_total": 0.0, "status_codes": Counter()},
        )
        bucket["count"] += 1
        bucket["duration_ms_total"] += span["duration_ms"]
        if status_code is not None:
            bucket["status_codes"][str(status_code)] += 1
            status_counts[str(status_code)] += 1

    for bucket in by_endpoint.values():
        bucket["avg_duration_ms"] = bucket["duration_ms_total"] / bucket["count"]
        bucket["status_codes"] = dict(bucket["status_codes"])

    return {
        "total_spans": len(spans),
        "http_span_count": len(http_spans),
        "status_codes": dict(status_counts),
        "http_by_endpoint": by_endpoint,
    }


def _default_output_path() -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return Path(f"sandbox-create-probe-report-{timestamp}.json")


def _print_summary(report: dict[str, Any]) -> None:
    summary = report["summary"]
    print(
        f"Completed {summary['attempts']} lifecycle attempts with {summary['failures']} failures "
        f"({summary['success_rate']:.1%} success rate)."
    )
    print(f"Attempts with non-fatal defects: {summary['attempts_with_defects']}")
    print(f"Failed stages: {summary['failed_stages']}")
    if summary["defect_stages"]:
        print(f"Defect stages: {summary['defect_stages']}")
    if summary["error_types"]:
        print(f"Error types: {summary['error_types']}")
    if summary["defect_error_types"]:
        print(f"Defect error types: {summary['defect_error_types']}")
    print(
        "Duration ms: "
        f"min={summary['duration_ms']['min']:.1f}, "
        f"avg={summary['duration_ms']['avg']:.1f}, "
        f"max={summary['duration_ms']['max']:.1f}"
    )

    span_summary = report["span_summary"]
    if span_summary["http_by_endpoint"]:
        print("HTTP span summary:")
        for endpoint, endpoint_summary in sorted(span_summary["http_by_endpoint"].items()):
            print(
                f"  {endpoint}: count={endpoint_summary['count']}, "
                f"avg={endpoint_summary['avg_duration_ms']:.1f} ms, "
                f"status_codes={endpoint_summary['status_codes']}"
            )


async def _main_async(args: argparse.Namespace) -> int:
    resource = Resource.create(
        {
            "service.name": "vercel-py-sandbox-create-probe",
            "service.version": VERCEL_VERSION,
        }
    )
    exporter = InMemorySpanExporter()
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    instrumentor = HTTPXClientInstrumentor()
    instrumentor.instrument(tracer_provider=provider)
    tracer = trace.get_tracer("vercel.sandbox.create.probe")

    interrupted = False
    interrupt_reason: str | None = None
    attempts: list[AttemptResult] = []
    run_started_monotonic = time.monotonic()
    try:
        print("Progress: ", end="", flush=True)
        for i in range(1, args.attempts + 1):
            if args.max_time is not None:
                elapsed = time.monotonic() - run_started_monotonic
                if elapsed >= args.max_time:
                    interrupted = True
                    interrupt_reason = f"max_time_exceeded ({args.max_time}s)"
                    break
            try:
                attempts.append(await _run_attempt(attempt_number=i, tracer=tracer, args=args))
            except ProbeInterrupted as exc:
                attempts.append(exc.result)
                interrupted = True
                interrupt_reason = exc.reason
                break
        print()
    finally:
        instrumentor.uninstrument()
        provider.force_flush()
        provider.shutdown()

    spans = [_simplify_span(span) for span in exporter.get_finished_spans()]
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "vercel_version": VERCEL_VERSION,
        },
        "config": {
            "attempts": args.attempts,
            "timeout_ms": args.timeout_ms,
            "snapshot_expiration_ms": args.snapshot_expiration_ms,
            "cleanup": args.cleanup,
            "cleanup_blocking": args.cleanup_blocking,
            "max_time": args.max_time,
        },
        "interrupted": interrupted,
        "interrupt_reason": interrupt_reason,
        "summary": _summarize_attempts(attempts),
        "attempts_data": [asdict(attempt) for attempt in attempts],
        "span_summary": _summarize_spans(spans),
        "spans": spans,
    }

    output_path = args.output or _default_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    _print_summary(report)
    if interrupted:
        print(f"Run interrupted: {interrupt_reason}")
    print(f"Report written to {output_path}")
    return 0


def main() -> int:
    args = _parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
