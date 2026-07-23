"""Editable workspace namespace composition checks."""

from __future__ import annotations

import importlib
from pathlib import Path


def test_regular_root_discovers_all_workspace_namespace_portions() -> None:
    expected_roots = {
        "vercel.cache": "src/vercel-cache",
        "vercel.headers": "src/vercel-headers",
        "vercel.integrations.celery": "integrations/vercel-celery",
        "vercel.integrations.dramatiq": "integrations/vercel-dramatiq",
        "vercel.internal.core": "src/vercel-internal-core",
        "vercel.internal.telemetry": "src/vercel-internal-telemetry",
        "vercel.oidc": "src/vercel-oidc",
        "vercel.queue": "src/vercel-queue",
        "vercel.sandbox": "src/vercel-sandbox",
    }

    for module_name, source_root in expected_roots.items():
        module = importlib.import_module(module_name)
        module_file = Path(module.__file__ or "").resolve()
        assert Path(source_root).resolve() in module_file.parents
