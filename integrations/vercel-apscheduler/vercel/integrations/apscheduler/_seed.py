from __future__ import annotations

import argparse
import importlib
import os
import sys
from collections.abc import Sequence
from datetime import datetime

from ._adapter import install_vercel_apscheduler_integration, seed_next_wakeup
from ._imports import BaseScheduler
from ._options import VercelAPSchedulerOptions

__all__ = ["main"]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m vercel.integrations.apscheduler",
        description="Seed the next Vercel Queue wakeup for an APScheduler object.",
    )
    parser.add_argument(
        "--entrypoint",
        required=True,
        help='scheduler entrypoint in "module:object" form',
    )
    parser.add_argument(
        "--now",
        help="optional timezone-aware ISO-8601 reference time for tests",
    )
    args = parser.parse_args(argv)

    # Defuse unguarded scheduler.start() calls while importing user code.
    os.environ.setdefault("VERCEL", "1")
    install_vercel_apscheduler_integration(options=VercelAPSchedulerOptions.from_env())

    try:
        scheduler = _load_scheduler(args.entrypoint)
        now = _parse_now(args.now)
        published = seed_next_wakeup(scheduler, now=now)
    except Exception as exc:  # noqa: BLE001 - CLI boundary converts failures to exit codes.
        sys.stderr.write(f"vercel.integrations.apscheduler: {exc}\n")
        return 1

    if published is None:
        sys.stdout.write("No APScheduler wakeup was scheduled.\n")
    else:
        sys.stdout.write(
            "Scheduled APScheduler wakeup "
            f"{published.logical_time.isoformat()} "
            f"with idempotency key {published.idempotency_key}.\n"
        )
    return 0


def _load_scheduler(entrypoint: str) -> BaseScheduler:
    module_name, separator, variable_name = entrypoint.partition(":")
    if not separator or not module_name or not variable_name:
        raise ValueError('entrypoint must use "module:object" form')

    module = importlib.import_module(module_name)
    scheduler = getattr(module, variable_name)
    if not isinstance(scheduler, BaseScheduler):
        raise TypeError(f"{entrypoint!r} is not an APScheduler BaseScheduler")
    return scheduler


def _parse_now(value: str | None) -> datetime | None:
    if value is None:
        return None
    now = datetime.fromisoformat(value)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("--now must be timezone-aware")
    return now


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
