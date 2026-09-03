#!/usr/bin/env python3
"""Guest agent that runs inside the Linux microVM listening on AF_VSOCK."""

import json
import os
import socket
import subprocess
import sys
import threading

AF_VSOCK = getattr(socket, "AF_VSOCK", 40)
VMADDR_CID_ANY = 0xFFFFFFFF
PORT = 10000

def handle_client(conn: socket.socket) -> None:
    try:
        buffer = b""
        while b"\n" not in buffer:
            chunk = conn.recv(65536)
            if not chunk:
                break
            buffer += chunk
        if not buffer:
            return
        
        line, _ = buffer.split(b"\n", 1)
        req = json.loads(line.decode("utf-8"))
        op = req.get("op")
        resp = {}

        if op == "ping":
            resp = {"status": "ok", "msg": "pong from microvm"}
        elif op == "exec":
            cmd = req.get("cmd")
            cwd = req.get("cwd", "/")
            env = dict(os.environ)
            if req.get("env"):
                env.update(req.get("env"))
            p = subprocess.run(
                cmd,
                shell=isinstance(cmd, str),
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
            )
            resp = {"exit_code": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
        elif op == "write":
            path = req.get("path")
            content = req.get("content", "")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            resp = {"status": "ok"}
        elif op == "read":
            path = req.get("path")
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    resp = {"status": "ok", "content": f.read()}
            else:
                resp = {"status": "not_found"}
        elif op == "mkdir":
            path = req.get("path")
            os.makedirs(path, exist_ok=True)
            resp = {"status": "ok"}
        else:
            resp = {"error": f"unknown op: {op}"}

        out_bytes = json.dumps(resp).encode("utf-8") + b"\n"
        conn.sendall(out_bytes)
    except Exception as e:
        err = json.dumps({"error": str(e)}).encode("utf-8") + b"\n"
        conn.sendall(err)
    finally:
        conn.close()

def main() -> None:
    sys.stderr.write(f"[guest-agent] Starting VSOCK server on port {PORT}\n")
    s = socket.socket(AF_VSOCK, socket.SOCK_STREAM)
    s.bind((VMADDR_CID_ANY, PORT))
    s.listen(32)
    sys.stderr.write(f"[guest-agent] Listening on vsock:{PORT}\n")

    while True:
        try:
            conn, _ = s.accept()
            t = threading.Thread(target=handle_client, args=(conn,), daemon=True)
            t.start()
        except Exception as e:
            sys.stderr.write(f"[guest-agent] Accept error: {e}\n")

if __name__ == "__main__":
    main()
