from __future__ import annotations

from http.server import BaseHTTPRequestHandler

from tasks import add

RESULT_TIMEOUT_SECONDS = 30


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def do_GET(self) -> None:
        result = add.chunks(zip(range(100), range(100), strict=False), 10).apply_async()
        waiting = "Waiting for results from workers..."
        print(waiting)

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(f"{waiting}\n".encode())
        self.wfile.flush()

        values = result.get(
            timeout=RESULT_TIMEOUT_SECONDS,
            disable_sync_subtasks=False,
        )
        print(f"chunk group {result.id} results: {values}")
        body = f"chunk group {result.id}\nresults: {values}\n".encode()

        self.wfile.write(body)
