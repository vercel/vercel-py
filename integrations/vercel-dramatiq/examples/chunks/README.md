# Dramatiq chunks app

Deploy this directory as a Vercel app:

```bash
cd integrations/vercel-dramatiq/examples/chunks
vc link
vc deploy
```

Queue the chunks by requesting the sender endpoint on the deployed app:

```bash
vc curl /enqueue
```

Check the worker logs:

```bash
vc logs <deployment-url>
```

Expected output includes:

```text
queued chunk group
0 + 0 = 0
99 + 99 = 198
```

`pyproject.toml` declares `worker.py` as a queue subscriber:

```toml
[[tool.vercel.subscribers]]
entrypoint = "worker:broker"
```

The Vercel build introspects the Dramatiq broker's declared queues, including
their delay queues, and compiles the subscriber into a queue-triggered
function; no `vercel.json` trigger configuration is needed. The FastAPI
sender in `main.py` uses a Dramatiq message group:

```python
group(starmap(add.message, zip(range(100), range(100), strict=False))).run()
```

Tasks publish through the Vercel Queue broker and store results with the
Vercel Runtime Cache result backend.
