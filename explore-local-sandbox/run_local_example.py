#!/usr/bin/env python3
"""Run real vercel-sandbox examples against a local Apple Silicon microVM."""

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

# Add explore-local-sandbox to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.host.daemon import LocalSandboxManager, create_app
import uvicorn

LOCAL_PORT = 5055

def start_local_daemon(manager: LocalSandboxManager) -> threading.Thread:
    app = create_app(manager)
    config = uvicorn.Config(app, host="127.0.0.1", port=LOCAL_PORT, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    time.sleep(1) # wait for server to start
    return t

def main() -> None:
    print("=" * 60)
    print("STARTING LOCAL APPLE SILICON MICROVM SANDBOX DEMO")
    print("=" * 60)

    assets_dir = Path(__file__).resolve().parent / "assets"
    manager = LocalSandboxManager(assets_dir=assets_dir)
    start_local_daemon(manager)
    print(f"Local Sandbox API server running at http://127.0.0.1:{LOCAL_PORT}")

    # Set environment variable so vercel.sandbox points to local daemon
    env = dict(os.environ)
    env["VERCEL_SANDBOX_API_BASE_URL"] = f"http://127.0.0.1:{LOCAL_PORT}"
    env["VERCEL_OIDC_TOKEN"] = "local_sandbox_token"
    env["VERCEL_PROJECT_ID"] = "prj_local"
    env["VERCEL_TEAM_ID"] = "team_local"

    example_path = (
        Path(__file__).resolve().parent.parent
        / "src"
        / "vercel-sandbox"
        / "examples"
        / "sandbox_02_sync_script_runner.py"
    )
    print(f"\nRunning real example against Local Sandbox:\n  {example_path}\n")

    try:
        proc = subprocess.run(
            ["uv", "run", "python3", str(example_path)],
            env=env,
            capture_output=True,
            text=True,
        )
        print("--- EXAMPLE STDOUT ---")
        print(proc.stdout)
        if proc.stderr:
            print("--- EXAMPLE STDERR ---")
            print(proc.stderr)
        print(f"Exit code: {proc.returncode}")
        if proc.returncode == 0:
            print("\nSUCCESS! Real vercel-sandbox example executed inside local Apple Silicon microVM.")
        else:
            print("\nExample failed. Review output above.")
    finally:
        print("\nCleaning up local microVMs...")
        manager.cleanup_all()

if __name__ == "__main__":
    main()
