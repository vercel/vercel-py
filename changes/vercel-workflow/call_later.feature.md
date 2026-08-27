Support `call_later`, `call_at`, and `now` in the event loop implementation.

This enables use of `asyncio.sleep()` as well as `asyncio.timeout` and
the `timeout` parameter of `asyncio.wait_for`.
