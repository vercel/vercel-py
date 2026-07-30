from __future__ import annotations

from http.server import BaseHTTPRequestHandler
from itertools import starmap

from dramatiq.composition import group
from tasks import add

RESULT_TIMEOUT_MILLISECONDS = 30000


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def do_GET(self) -> None:
        messages = list(starmap(add.message, zip(range(100), range(100), strict=False)))
        result_group = group(messages).run()
        waiting = "Waiting for results from workers..."
        print(waiting)

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(f"{waiting}\n".encode())
        self.wfile.flush()

        values = list(result_group.get_results(block=True, timeout=RESULT_TIMEOUT_MILLISECONDS))
        print(f"chunk group results: {values}")
        self.wfile.write(f"chunk group\nresults: {values}\n".encode())
