#!/usr/bin/env python3
"""Host-side microVM lifecycle manager and Vercel Sandbox API adapter."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

ASSETS_DIR = Path(__file__).resolve().parent.parent.parent / "assets"

@dataclass
class LocalMicroVM:
    sandbox_id: str
    vfkit_proc: subprocess.Popen[bytes]
    socket_path: Path
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))

    def send_command(self, payload: dict) -> dict:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(10.0)
        s.connect(str(self.socket_path))
        try:
            s.sendall(json.dumps(payload).encode("utf-8") + b"\n")
            buffer = b""
            while b"\n" not in buffer:
                chunk = s.recv(65536)
                if not chunk:
                    break
                buffer += chunk
            line, _ = buffer.split(b"\n", 1)
            return json.loads(line.decode("utf-8"))
        finally:
            s.close()

    def destroy(self) -> None:
        if self.vfkit_proc.poll() is None:
            self.vfkit_proc.terminate()
            try:
                self.vfkit_proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.vfkit_proc.kill()
        if self.socket_path.exists():
            self.socket_path.unlink()

class LocalSandboxManager:
    def __init__(self, assets_dir: Path):
        self.assets_dir = assets_dir
        self.vms: dict[str, LocalMicroVM] = {}
        self.run_dir = Path("/tmp/vercel-local-sandbox")
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def launch_vm(self, name: str | None = None) -> LocalMicroVM:
        sandbox_id = f"sbx_local_{uuid.uuid4().hex[:8]}"
        sock_path = self.run_dir / f"{sandbox_id}.sock"
        log_path = self.run_dir / f"{sandbox_id}.log"
        if sock_path.exists():
            sock_path.unlink()

        kernel_path = self.assets_dir / "vmlinux"
        initramfs_path = self.assets_dir / "initramfs.cpio.gz"

        if not kernel_path.exists() or not initramfs_path.exists():
            raise RuntimeError(
                f"Missing microVM assets in {self.assets_dir}. Run scripts/build_assets.py first."
            )

        cmd = [
            "vfkit",
            "--kernel", str(kernel_path),
            "--initrd", str(initramfs_path),
            "--kernel-cmdline", "console=hvc0 init=/init panic=-1",
            "--cpus", "2",
            "--memory", "1024",
            "--device", "virtio-rng",
            "--device", f"virtio-vsock,port=10000,socketURL={sock_path},connect",
            "--device", f"virtio-serial,logFilePath={log_path}",
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for guest agent to become reachable over vsock unix socket
        vm = LocalMicroVM(sandbox_id=sandbox_id, vfkit_proc=proc, socket_path=sock_path)
        deadline = time.time() + 10.0
        connected = False
        while time.time() < deadline:
            if sock_path.exists():
                try:
                    res = vm.send_command({"op": "ping"})
                    if res.get("status") == "ok":
                        connected = True
                        break
                except Exception:
                    pass
            time.sleep(0.1)

        if not connected:
            vm.destroy()
            raise RuntimeError(f"MicroVM failed to initialize within timeout. Log: {log_path}")

        self.vms[sandbox_id] = vm
        return vm

    def get_vm(self, sandbox_id: str) -> LocalMicroVM | None:
        return self.vms.get(sandbox_id)

    def destroy_vm(self, sandbox_id: str) -> None:
        vm = self.vms.pop(sandbox_id, None)
        if vm:
            vm.destroy()

    def cleanup_all(self) -> None:
        for vm in list(self.vms.values()):
            vm.destroy()
        self.vms.clear()

def create_app(manager: LocalSandboxManager) -> Starlette:
    async def create_sandbox(request: Request) -> Response:
        body = await request.json() if request.headers.get("content-length") else {}
        name = body.get("name") or f"sbx_local_{uuid.uuid4().hex[:8]}"
        vm = manager.launch_vm(name=name)

        sandbox_payload = {
            "name": name,
            "status": "running",
            "currentSessionId": vm.sandbox_id,
            "persistent": False,
            "projectId": "local_project",
            "cwd": "/",
            "region": "local-mac",
            "memory": 1024,
            "vcpus": 2,
        }
        session_payload = {
            "id": vm.sandbox_id,
            "sandboxName": name,
            "projectId": "local_project",
            "status": "running",
            "cwd": "/",
            "region": "local-mac",
            "startedAt": vm.created_at,
        }
        resp_data = {
            "sandbox": sandbox_payload,
            "session": session_payload,
            "routes": [],
        }
        return JSONResponse(resp_data, status_code=200)

    async def get_sandbox(request: Request) -> Response:
        sandbox_id = request.path_params["sandbox_id"]
        vm = manager.get_vm(sandbox_id)
        if not vm:
            return JSONResponse({"error": {"code": "not_found", "message": "Sandbox not found"}}, status_code=404)
        return JSONResponse({
            "sandbox": {
                "name": vm.sandbox_id,
                "status": "running",
                "currentSessionId": vm.sandbox_id,
                "persistent": False,
                "projectId": "local_project",
            },
            "session": {
                "id": vm.sandbox_id,
                "status": "running",
                "startedAt": vm.created_at,
            },
            "routes": [],
        })

    async def stop_sandbox(request: Request) -> Response:
        sandbox_id = request.path_params["sandbox_id"]
        manager.destroy_vm(sandbox_id)
        return JSONResponse({"status": "stopped"})

    async def run_command(request: Request) -> Response:
        session_id = request.path_params["session_id"]
        vm = manager.get_vm(session_id)
        if not vm:
            return JSONResponse({"error": {"code": "not_found", "message": "Session not found"}}, status_code=404)

        body = await request.json()
        command = body.get("command")
        args = body.get("args") or []
        cwd = body.get("cwd") or "/"
        env = body.get("env") or {}
        full_cmd = [command] + args if args else command

        cmd_id = f"cmd_{uuid.uuid4().hex[:8]}"
        res = vm.send_command({
            "op": "exec",
            "cmd": full_cmd,
            "cwd": cwd,
            "env": env,
        })

        wait = request.query_params.get("wait") == "true"
        logs = request.query_params.get("logs") == "true"

        command_record = {
            "id": cmd_id,
            "name": command,
            "args": args,
            "cwd": cwd,
            "sessionId": session_id,
            "status": "completed",
            "exitCode": res.get("exit_code", 0),
            "startedAt": int(time.time() * 1000),
        }

        if wait and logs:
            # Streaming NDJSON response expected by SDK run_process:
            # First line: {"command": {...initial...}}
            # Next lines: {"stream": "stdout"|"stderr", "data": line}
            # Last line:  {"command": {...final...}}
            async def event_generator():
                # Initial command state
                initial_record = dict(command_record)
                initial_record["status"] = "running"
                initial_record["exitCode"] = None
                yield (json.dumps({"command": initial_record}) + "\n").encode("utf-8")

                # Stream stdout log lines
                if res.get("stdout"):
                    for line in res["stdout"].splitlines(keepends=True):
                        yield (json.dumps({
                            "stream": "stdout",
                            "data": line,
                        }) + "\n").encode("utf-8")

                # Stream stderr log lines
                if res.get("stderr"):
                    for line in res["stderr"].splitlines(keepends=True):
                        yield (json.dumps({
                            "stream": "stderr",
                            "data": line,
                        }) + "\n").encode("utf-8")

                # Final command state
                yield (json.dumps({"command": command_record}) + "\n").encode("utf-8")

            return StreamingResponse(event_generator(), media_type="application/x-ndjson")

        return JSONResponse(command_record)

    async def fs_mkdir(request: Request) -> Response:
        session_id = request.path_params["session_id"]
        vm = manager.get_vm(session_id)
        if not vm:
            return JSONResponse({"error": "session not found"}, status_code=404)
        body = await request.json()
        path = body.get("path")
        vm.send_command({"op": "mkdir", "path": path})
        return JSONResponse({"status": "ok"})

    async def fs_read(request: Request) -> Response:
        session_id = request.path_params["session_id"]
        vm = manager.get_vm(session_id)
        if not vm:
            return JSONResponse({"error": "session not found"}, status_code=404)
        body = await request.json()
        path = body.get("path")
        res = vm.send_command({"op": "read", "path": path})
        if res.get("status") != "ok":
            return JSONResponse({"error": "not found"}, status_code=404)
        content = res.get("content", "")
        return Response(content.encode("utf-8"), media_type="application/octet-stream")

    async def fs_write(request: Request) -> Response:
        # Handles write streaming / multipart or JSON
        session_id = request.path_params["session_id"]
        vm = manager.get_vm(session_id)
        if not vm:
            return JSONResponse({"error": "session not found"}, status_code=404)
        # Parse multipart form or body
        form = await request.form()
        for key, value in form.items():
            content = await value.read() if hasattr(value, "read") else str(value).encode("utf-8")
            vm.send_command({"op": "write", "path": key, "content": content.decode("utf-8", errors="replace")})
        return JSONResponse({"status": "ok"})

    async def stop_session(request: Request) -> Response:
        session_id = request.path_params["session_id"]
        vm = manager.get_vm(session_id)
        session_payload = {
            "id": session_id,
            "sandboxName": session_id,
            "projectId": "local_project",
            "status": "stopped",
            "startedAt": vm.created_at if vm else int(time.time() * 1000),
            "stoppedAt": int(time.time() * 1000),
        }
        sandbox_payload = {
            "name": session_id,
            "status": "stopped",
            "currentSessionId": session_id,
            "persistent": False,
            "projectId": "local_project",
        }
        manager.destroy_vm(session_id)
        return JSONResponse({"session": session_payload, "sandbox": sandbox_payload, "routes": []})

    routes = [
        Route("/v3/sandboxes", create_sandbox, methods=["POST"]),
        Route("/v2/sandboxes/{sandbox_id}", get_sandbox, methods=["GET"]),
        Route("/v2/sandboxes/{sandbox_id}/stop", stop_sandbox, methods=["POST"]),
        Route("/v2/sandboxes/sessions/{session_id}/stop", stop_session, methods=["POST"]),
        Route("/v2/sandboxes/sessions/{session_id}/cmd", run_command, methods=["POST"]),
        Route("/v2/sandboxes/sessions/{session_id}/fs/mkdir", fs_mkdir, methods=["POST"]),
        Route("/v2/sandboxes/sessions/{session_id}/fs/read", fs_read, methods=["POST"]),
        Route("/v2/sandboxes/sessions/{session_id}/fs/write", fs_write, methods=["POST"]),
    ]

    return Starlette(routes=routes)

def run_server(port: int = 5055) -> None:
    manager = LocalSandboxManager(assets_dir=ASSETS_DIR)
    app = create_app(manager)
    try:
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
    finally:
        manager.cleanup_all()

if __name__ == "__main__":
    run_server()
