# vercel-django-tasks

Django task backend backed by Vercel Queue Service. The installer uses
`VercelQueuesBackend` as Django's default task backend when no `TASKS`
backends are configured and registers generated queue subscribers.

Register push subscribers during application startup:

```python
from vercel.integrations.django import install_vercel_django_task_integration

install_vercel_django_task_integration()
```

No `TASKS` setting is required. Configure one explicitly to customize the
backend or to use a different backend:

```python
TASKS = {
    "default": {
        "BACKEND": "vercel.integrations.django.VercelQueuesBackend",
        "QUEUES": ["default"],
        "OPTIONS": {
            "result_namespace": "django-task-results",
            "result_ttl_seconds": 86400,
        },
    },
}
```

Declare the module that loads your Django application in `pyproject.toml` so
Vercel generates a queue subscriber function:

```toml
[[tool.vercel.subscribers]]
entrypoint = "my_project.wsgi"
```

No manual queue endpoint is required.

Task result state is stored in Vercel Runtime Cache. Results are cache-backed
and expire according to `result_ttl_seconds`; they are not durable storage.

This package depends on Django, `vercel-queue`, and `vercel-cache`.
