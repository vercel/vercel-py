# Django tasks chunks app

Deploy this directory as a Vercel app:

```bash
cd integrations/vercel-django-tasks/examples/chunks
vc link
vc deploy
```

Queue the chunks by requesting the Django view on the deployed app:

```bash
vc curl /send_chunks/
```

Expected output includes:

```text
queued 100 add tasks
results: [0, 2, 4, 6
```

`pyproject.toml` declares the Django WSGI application as both the web
entrypoint and a queue subscriber:

```toml
[tool.vercel]
entrypoint = "chunks_project.wsgi:application"

[[tool.vercel.subscribers]]
entrypoint = "chunks_project.wsgi"
```

The Vercel build imports the Django application, introspects the generated
task subscriber registered during startup, and compiles it into a
queue-triggered function. The runtime activates the Django Tasks integration
for both the web and subscriber functions. No manual integration setup, queue
view, or `vercel.json` trigger configuration is needed.
