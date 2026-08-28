Align `create_process` output handling with `subprocess.Popen`: inherit stdout and stderr by default, and support writable text streams alongside `PIPE`, `DEVNULL`, and `STDOUT`.

This is a breaking change because callers that relied on the previous implicit `PIPE` behavior must now pass `stdout=subprocess.PIPE` or `stderr=subprocess.PIPE` to capture output.
